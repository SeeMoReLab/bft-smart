package bftsmart.demo.smallbank2pc;

import bftsmart.injection.InjectionClient;
import bftsmart.tom.ServiceProxy;
import org.apache.commons.cli.*;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DisabledListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SmallBank Client for BFT-SMaRt with 2PC support for cross-shard transactions.
 *
 * This client can operate in two modes:
 * 1. Single-shard mode: All transactions go to one shard (legacy behavior)
 * 2. Multi-shard mode: Transactions are routed based on account partitioning,
 *    and cross-shard transactions use 2PC protocol
 */
public class SmallBankClient2PC {
    private static final Logger LOG = LoggerFactory.getLogger(SmallBankClient2PC.class);
    private static final String SINGLE_LINE = "======================================================================";

    // Client configuration
    private final int clientId;
    private final WorkloadConfig config;
    private final Random random;

    // Shard configuration
    private final int numShards;
    private final Map<Integer, ServiceProxy> shardProxies;
    private final Map<Integer, String> shardConfigPaths;

    // 2PC coordinator (client-side)
    private final ClientTwoPhaseCoordinator coordinator;

    // Statistics
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger errorCount = new AtomicInteger(0);
    private final AtomicInteger crossShardCount = new AtomicInteger(0);
    private final AtomicLong totalLatency = new AtomicLong(0);
    private final List<Long> latencies = new CopyOnWriteArrayList<>();

    // Transaction ID generator
    private final AtomicLong txIdCounter = new AtomicLong(0);

    public static void main(String[] args) {
        try {
            CommandLineParser parser = new DefaultParser();
            Options options = buildOptions();
            CommandLine argsLine = parser.parse(options, args);

            if (argsLine.hasOption("h") || !argsLine.hasOption("c")) {
                printUsage(options);
                return;
            }

            String configFile = argsLine.getOptionValue("c");
            int clientId = Integer.parseInt(argsLine.getOptionValue("id", "0"));
            int numShards = Integer.parseInt(argsLine.getOptionValue("shards", "1"));
            String shardConfigBase = argsLine.getOptionValue("shard-config", "");

            System.out.println(SINGLE_LINE);
            System.out.println("SmallBank BFT-SMaRt Client (2PC Enabled)");
            System.out.println("Configuration: " + configFile);
            System.out.println("Client ID: " + clientId);
            System.out.println("Number of shards: " + numShards);
            if (!shardConfigBase.isEmpty()) {
                System.out.println("Shard config base: " + shardConfigBase);
            }
            System.out.println(SINGLE_LINE);

            WorkloadConfig config = loadConfiguration(configFile);

            // Build shard config paths
            Map<Integer, String> shardConfigs = new HashMap<>();
            for (int i = 0; i < numShards; i++) {
                if (shardConfigBase.isEmpty()) {
                    shardConfigs.put(i, "shard" + i + "/replica0/config");
                } else {
                    shardConfigs.put(i, shardConfigBase + "/shard" + i + "/replica0/config");
                }
            }

            SmallBankClient2PC client = new SmallBankClient2PC(clientId, config, numShards, shardConfigs);

            if (argsLine.hasOption("create")) {
                System.out.println("Creating accounts across " + numShards + " shards...");
                client.createAccounts();
            }

            if (argsLine.hasOption("inject")) {
                try {
                    ExecutorService injectionExecutor = Executors.newFixedThreadPool(1);
                    InjectionClient injectionClient = new InjectionClient(
                            "config/injection.json", config.terminals);
                    injectionExecutor.submit(injectionClient::start);
                } catch (Exception ex) {
                    System.out.println("Could not load injection config " + ex.getMessage());
                }
            }

            if (argsLine.hasOption("execute")) {
                System.out.println("Executing workload...");
                for (int i = 0; i < config.phases.length; i++) {
                    client.executeWorkload(i);
                }
            }

            client.close();
            System.out.println(SINGLE_LINE);
            System.out.println("Client finished");

        } catch (Exception e) {
            LOG.error("Error in client execution", e);
            System.exit(1);
        }
    }

    /**
     * Constructor for multi-shard client.
     */
    public SmallBankClient2PC(int clientId, WorkloadConfig config, int numShards,
                              Map<Integer, String> shardConfigPaths) {
        this.clientId = clientId;
        this.config = config;
        this.random = new Random(config.randomSeed + clientId);
        this.numShards = numShards;
        this.shardConfigPaths = new HashMap<>(shardConfigPaths);
        this.shardProxies = new ConcurrentHashMap<>();

        // Initialize proxies for each shard
        initializeShardProxies();

        // Create coordinator
        this.coordinator = new ClientTwoPhaseCoordinator();

        System.out.printf("Client %d initialized with %d shards%n", clientId, numShards);
    }

    /**
     * Constructor for single-shard mode.
     */
    public SmallBankClient2PC(int clientId, WorkloadConfig config) {
        this(clientId, config, 1, Collections.singletonMap(0, "config"));
    }

    /**
     * Initialize ServiceProxy connections to all shards.
     */
    private void initializeShardProxies() {
        for (int shardId = 0; shardId < numShards; shardId++) {
            String configPath = shardConfigPaths.getOrDefault(shardId, "config");
            int proxyId = clientId * 1000 + shardId; // Unique ID per shard

            LOG.info("Creating proxy for shard {} with clientId {} and config {}",
                     shardId, proxyId, configPath);

            try {
                ServiceProxy proxy = new ServiceProxy(proxyId, configPath);
                shardProxies.put(shardId, proxy);
            } catch (Exception e) {
                LOG.error("Failed to create proxy for shard {}", shardId, e);
                throw new RuntimeException("Failed to initialize shard " + shardId, e);
            }
        }
    }

    // Partitioning Logic

    private int getShardForAccount(long accountId) {
        return (int) (accountId % numShards);
    }

    private boolean isCrossShardTransaction(SmallBankMessage2PC.TransactionType type,
                                            long custId1, long custId2) {
        if (numShards == 1) return false;

        switch (type) {
            case SEND_PAYMENT:
            case AMALGAMATE:
                return getShardForAccount(custId1) != getShardForAccount(custId2);
            default:
                return false;
        }
    }

    private void createAccounts() {
        System.out.printf("Creating %d accounts across %d shards...%n", config.numAccounts, numShards);
        long startTime = System.currentTimeMillis();

        for (long custId = 0; custId < config.numAccounts; custId++) {
            String custName = String.format("Customer%010d", custId);
            double savingsBalance = 10000.0;
            double checkingBalance = 10000.0;

            SmallBankMessage2PC msg = SmallBankMessage2PC.newCreateAccountRequest(
                    custId, custName, savingsBalance, checkingBalance
            );

            // Route to correct shard
            int targetShard = getShardForAccount(custId);
            ServiceProxy proxy = shardProxies.get(targetShard);

            try {
                byte[] reply = proxy.invokeOrdered(msg.getBytes());
                SmallBankMessage2PC response = SmallBankMessage2PC.getObject(reply);

                if (response == null || response.getResult() != 0) {
                    String error = response != null ? response.getErrorMsg() : "null response";
                    System.out.println("Failed to create account " + custId + " on shard " +
                                      targetShard + ": " + error);
                }
            } catch (Exception e) {
                System.out.println("Error creating account " + custId + ": " + e);
            }

            if ((custId + 1) % 1000 == 0) {
                System.out.printf("Created %d accounts%n", custId + 1);
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        System.out.printf("Finished creating %d accounts in %d ms%n", config.numAccounts, duration);
    }

    private void executeWorkload(int phaseNum) {
        // Reset statistics for this phase
        successCount.set(0);
        errorCount.set(0);
        crossShardCount.set(0);
        totalLatency.set(0);
        latencies.clear();

        ExecutorService executor = Executors.newFixedThreadPool(config.terminals + 1);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(config.terminals);

        int terminals = (config.phases[phaseNum].terminals == -1) ?
                        config.terminals : config.phases[phaseNum].terminals;

        System.out.printf("Starting %d terminals for %d seconds%n", terminals, config.phases[phaseNum].duration);
        System.out.printf("Target rate: %.2f TPS per terminal%n", config.phases[phaseNum].rate);
        System.out.println("Transaction weights: " + Arrays.toString(config.phases[phaseNum].weights));
        System.out.println("Number of shards: " + numShards);

        for (int i = 0; i < terminals; i++) {
            final int terminalId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    runTerminal(terminalId, phaseNum);
                } catch (Exception e) {
                    System.out.println("Error in terminal " + terminalId + ": " + e);
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        System.out.println("All terminals ready. Starting workload...");
        long workloadStart = System.nanoTime();
        startLatch.countDown();

        try {
            completionLatch.await(config.phases[phaseNum].duration + 10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for completion", e);
        }

        executor.shutdownNow();
        long workloadEnd = System.nanoTime();

        printResults(phaseNum, workloadStart, workloadEnd);
    }

    private void runTerminal(int terminalId, int phaseNum) {
        Random terminalRandom = new Random(config.randomSeed + terminalId);
        long startTime = System.nanoTime();
        long endTime = startTime + TimeUnit.SECONDS.toNanos(config.phases[phaseNum].duration);

        long intervalNs = (long) (1_000_000_000.0 / config.phases[phaseNum].rate);
        long nextTransactionTime = startTime;

        int txCount = 0;

        while (System.nanoTime() < endTime) {
            long now = System.nanoTime();
            long waitTime = nextTransactionTime - now;
            if (waitTime > 0) {
                try {
                    Thread.sleep(waitTime / 1_000_000, (int) (waitTime % 1_000_000));
                } catch (InterruptedException e) {
                    break;
                }
            }

            SmallBankMessage2PC.TransactionType txType = selectTransactionType(
                    terminalRandom, config.phases[phaseNum].weights);

            long txStart = System.nanoTime();
            boolean success = executeTransaction(txType, terminalRandom);
            long txEnd = System.nanoTime();

            if (success) {
                successCount.incrementAndGet();
                long latency = txEnd - txStart;
                totalLatency.addAndGet(latency);
                latencies.add(latency / 1_000_000);
            } else {
                errorCount.incrementAndGet();
            }

            txCount++;
            nextTransactionTime += intervalNs;

            if (txCount % 100 == 0 && terminalId == 0) {
                LOG.debug("Terminal {} executed {} transactions", terminalId, txCount);
            }
        }

        System.out.printf("Terminal %d completed %d transactions%n", terminalId, txCount);
    }

    private SmallBankMessage2PC.TransactionType selectTransactionType(Random rnd, int[] weights) {
        int totalWeight = 0;
        for (int weight : weights) {
            totalWeight += weight;
        }

        int randomValue = rnd.nextInt(totalWeight);
        int cumulativeWeight = 0;

        for (int i = 0; i < weights.length; i++) {
            cumulativeWeight += weights[i];
            if (randomValue < cumulativeWeight) {
                return SmallBankMessage2PC.TransactionType.values()[i];
            }
        }

        return SmallBankMessage2PC.TransactionType.WRITE_CHECK;
    }

    private boolean executeTransaction(SmallBankMessage2PC.TransactionType type, Random rnd) {
        try {
            long custId1 = rnd.nextInt(config.numAccounts);
            long custId2 = rnd.nextInt(config.numAccounts);
            while (custId2 == custId1) {
                custId2 = rnd.nextInt(config.numAccounts);
            }
            double amount = 1.0 + rnd.nextDouble() * 99.0;

            // Check if cross-shard
            if (isCrossShardTransaction(type, custId1, custId2)) {
                crossShardCount.incrementAndGet();
                return executeCrossShardTransaction(type, custId1, custId2, amount);
            } else {
                return executeSingleShardTransaction(type, custId1, custId2, amount);
            }

        } catch (Exception e) {
            LOG.error("Error executing transaction {}", type, e);
            return false;
        }
    }

    /**
     * Execute a single-shard transaction.
     */
    private boolean executeSingleShardTransaction(SmallBankMessage2PC.TransactionType type,
                                                   long custId1, long custId2, double amount) {
        SmallBankMessage2PC msg = buildTransactionMessage(type, custId1, custId2, amount);
        if (msg == null) return false;

        int targetShard = getShardForAccount(custId1);
        ServiceProxy proxy = shardProxies.get(targetShard);

        if (type == SmallBankMessage2PC.TransactionType.BALANCE) {
            byte[] reply = proxy.invokeUnordered(msg.getBytes());
            SmallBankMessage2PC response = SmallBankMessage2PC.getObject(reply);
            return response != null && response.getResult() == 0;
        }

        byte[] reply = proxy.invokeOrdered(msg.getBytes());
        SmallBankMessage2PC response = SmallBankMessage2PC.getObject(reply);
        return response != null && response.getResult() == 0;
    }

    /**
     * Execute a cross-shard transaction using 2PC.
     */
    private boolean executeCrossShardTransaction(SmallBankMessage2PC.TransactionType type,
                                                  long custId1, long custId2, double amount) {
        LOG.debug("Executing cross-shard {} between accounts {} (shard {}) and {} (shard {})",
                  type, custId1, getShardForAccount(custId1), custId2, getShardForAccount(custId2));

        return coordinator.execute2PC(type, custId1, custId2, amount);
    }

    /**
     * Build a transaction message.
     */
    private SmallBankMessage2PC buildTransactionMessage(SmallBankMessage2PC.TransactionType type,
                                                         long custId1, long custId2, double amount) {
        switch (type) {
            case DEPOSIT_CHECKING:
                return SmallBankMessage2PC.newDepositCheckingRequest(custId1, amount);
            case TRANSACT_SAVINGS:
                return SmallBankMessage2PC.newTransactSavingsRequest(custId1, amount);
            case WRITE_CHECK:
                return SmallBankMessage2PC.newWriteCheckRequest(custId1, amount);
            case SEND_PAYMENT:
                return SmallBankMessage2PC.newSendPaymentRequest(custId1, custId2, amount);
            case AMALGAMATE:
                return SmallBankMessage2PC.newAmalgamateRequest(custId1, custId2);
            case BALANCE:
                return SmallBankMessage2PC.newBalanceRequest(custId1);
            default:
                return null;
        }
    }

    // Client-Side 2PC Coordinator

    /**
     * Client-side 2PC coordinator for cross-shard transactions.
     */
    private class ClientTwoPhaseCoordinator {
        private final ExecutorService executor = Executors.newCachedThreadPool();

        /**
         * Execute a cross-shard transaction using 2PC.
         */
        public boolean execute2PC(SmallBankMessage2PC.TransactionType type,
                                  long custId1, long custId2, double amount) {
            String txId = generateTransactionId();

            int shard1 = getShardForAccount(custId1);
            int shard2 = getShardForAccount(custId2);

            LOG.debug("2PC transaction {} starting: type={}, shards=[{}, {}]", txId, type, shard1, shard2);

            // Build PREPARE messages for each shard
            Map<Integer, SmallBankMessage2PC> prepareMessages = buildPrepareMessages(
                    txId, type, custId1, custId2, amount, shard1, shard2);

            // Phase 1: Send PREPARE to all shards in parallel
            Map<Integer, SmallBankMessage2PC> prepareResponses = executePreparePhase(txId, prepareMessages);

            // Check votes
            boolean allPrepared = checkAllPrepared(prepareResponses);

            // Phase 2: COMMIT or ABORT
            if (allPrepared) {
                LOG.debug("2PC transaction {} - all prepared, committing", txId);
                executeCommitPhase(txId, prepareMessages.keySet());
                return true;
            } else {
                LOG.debug("2PC transaction {} - prepare failed, aborting", txId);
                executeAbortPhase(txId, prepareMessages.keySet());
                return false;
            }
        }

        private String generateTransactionId() {
            return String.format("ctx-%d-%d-%d", clientId, System.currentTimeMillis(),
                                txIdCounter.incrementAndGet());
        }

        private Map<Integer, SmallBankMessage2PC> buildPrepareMessages(
                String txId, SmallBankMessage2PC.TransactionType type,
                long custId1, long custId2, double amount, int shard1, int shard2) {

            Map<Integer, SmallBankMessage2PC> messages = new HashMap<>();

            switch (type) {
                case SEND_PAYMENT:
                    // Shard with source account: WRITE_CHECK (debit)
                    messages.put(shard1, SmallBankMessage2PC.newPrepareRequest(
                            txId, -1, shard1,
                            SmallBankMessage2PC.TransactionType.WRITE_CHECK,
                            custId1, amount));

                    // Shard with dest account: DEPOSIT_CHECKING (credit)
                    messages.put(shard2, SmallBankMessage2PC.newPrepareRequest(
                            txId, -1, shard2,
                            SmallBankMessage2PC.TransactionType.DEPOSIT_CHECKING,
                            custId2, amount));
                    break;

                case AMALGAMATE:
                    // This is more complex - simplified here
                    messages.put(shard1, SmallBankMessage2PC.newPrepareRequest(
                            txId, -1, shard1,
                            SmallBankMessage2PC.TransactionType.DEPOSIT_CHECKING,
                            custId1, 0));
                    messages.put(shard2, SmallBankMessage2PC.newPrepareRequest(
                            txId, -1, shard2,
                            SmallBankMessage2PC.TransactionType.WRITE_CHECK,
                            custId2, 0));
                    break;

                default:
                    LOG.warn("Unsupported cross-shard transaction type: {}", type);
            }

            return messages;
        }

        private Map<Integer, SmallBankMessage2PC> executePreparePhase(
                String txId, Map<Integer, SmallBankMessage2PC> prepareMessages) {

            Map<Integer, Future<SmallBankMessage2PC>> futures = new HashMap<>();

            for (Map.Entry<Integer, SmallBankMessage2PC> entry : prepareMessages.entrySet()) {
                int shardId = entry.getKey();
                SmallBankMessage2PC prepareMsg = entry.getValue();
                ServiceProxy proxy = shardProxies.get(shardId);

                futures.put(shardId, executor.submit(() -> {
                    byte[] response = proxy.invokeOrdered(prepareMsg.getBytes());
                    if (response == null) {
                        return SmallBankMessage2PC.newPrepareFail(txId, "No response");
                    }
                    return SmallBankMessage2PC.getObject(response);
                }));
            }

            Map<Integer, SmallBankMessage2PC> responses = new HashMap<>();
            for (Map.Entry<Integer, Future<SmallBankMessage2PC>> entry : futures.entrySet()) {
                try {
                    SmallBankMessage2PC response = entry.getValue().get(30, TimeUnit.SECONDS);
                    responses.put(entry.getKey(), response);
                } catch (Exception e) {
                    LOG.error("Error in PREPARE for shard {}", entry.getKey(), e);
                    responses.put(entry.getKey(), SmallBankMessage2PC.newPrepareFail(txId, e.getMessage()));
                }
            }

            return responses;
        }

        private boolean checkAllPrepared(Map<Integer, SmallBankMessage2PC> responses) {
            for (SmallBankMessage2PC response : responses.values()) {
                if (response == null || !response.isPrepareOk()) {
                    return false;
                }
            }
            return true;
        }

        private void executeCommitPhase(String txId, Set<Integer> shards) {
            executePhase2(txId, shards, true);
        }

        private void executeAbortPhase(String txId, Set<Integer> shards) {
            executePhase2(txId, shards, false);
        }

        private void executePhase2(String txId, Set<Integer> shards, boolean commit) {
            SmallBankMessage2PC phase2Msg = commit
                    ? SmallBankMessage2PC.newCommit(txId, -1)
                    : SmallBankMessage2PC.newAbort(txId, -1);

            List<Future<?>> futures = new ArrayList<>();
            for (int shardId : shards) {
                ServiceProxy proxy = shardProxies.get(shardId);
                futures.add(executor.submit(() -> {
                    try {
                        proxy.invokeOrdered(phase2Msg.getBytes());
                    } catch (Exception e) {
                        LOG.error("Error in phase 2 for shard {}", shardId, e);
                    }
                }));
            }

            // Wait for all to complete
            for (Future<?> future : futures) {
                try {
                    future.get(30, TimeUnit.SECONDS);
                } catch (Exception e) {
                    LOG.warn("Timeout waiting for phase 2 completion");
                }
            }
        }

        public void shutdown() {
            executor.shutdown();
        }
    }

    // Results

    private void printResults(int phaseNum, long startNs, long endNs) {
        double durationSeconds = (endNs - startNs) / 1_000_000_000.0;
        int totalTxns = successCount.get() + errorCount.get();
        double throughput = totalTxns / durationSeconds;
        double avgLatency = successCount.get() > 0 ?
                           totalLatency.get() / 1_000_000.0 / successCount.get() : 0;

        Collections.sort(latencies);
        long p50 = getPercentile(latencies, 0.50);
        long p95 = getPercentile(latencies, 0.95);
        long p99 = getPercentile(latencies, 0.99);

        System.out.println(SINGLE_LINE);
        System.out.printf("Phase %d Results%n", phaseNum);
        System.out.println(SINGLE_LINE);
        System.out.printf("Duration: %.2f seconds%n", durationSeconds);
        System.out.printf("Total Transactions: %d%n", totalTxns);
        System.out.printf("Successful: %d%n", successCount.get());
        System.out.printf("Errors: %d%n", errorCount.get());
        System.out.printf("Cross-shard transactions: %d%n", crossShardCount.get());
        System.out.printf("Throughput: %.2f TPS%n", throughput);
        System.out.printf("Average Latency: %.2f ms%n", avgLatency);
        System.out.printf("P50 Latency: %d ms%n", p50);
        System.out.printf("P95 Latency: %d ms%n", p95);
        System.out.printf("P99 Latency: %d ms%n", p99);
        System.out.println(SINGLE_LINE);
    }

    private long getPercentile(List<Long> sortedValues, double percentile) {
        if (sortedValues.isEmpty()) return 0;
        int index = (int) Math.ceil(percentile * sortedValues.size()) - 1;
        index = Math.max(0, Math.min(index, sortedValues.size() - 1));
        return sortedValues.get(index);
    }

    private void close() {
        coordinator.shutdown();
        for (ServiceProxy proxy : shardProxies.values()) {
            proxy.close();
        }
    }

    // Configuration

    private static WorkloadConfig loadConfiguration(String configFile) throws ConfigurationException {
        XMLConfiguration xml = buildConfiguration(configFile);
        WorkloadConfig config = new WorkloadConfig();

        config.numAccounts = xml.getInt("numAccounts", 100000);
        config.terminals = xml.getInt("terminals", 1);
        config.randomSeed = xml.getInt("randomSeed", 17);

        int size = xml.configurationsAt("/works/work").size();
        config.phases = new Phase[size];

        for (int i = 1; i < size + 1; i++) {
            final HierarchicalConfiguration<ImmutableNode> work =
                    xml.configurationAt("works/work[" + i + "]");
            Phase phase = new Phase();
            phase.terminals = work.getInt("terminals", -1);
            phase.duration = work.getInt("time");
            phase.rate = work.getDouble("rate");

            String weightsStr = work.getString("weights", "15,15,15,25,15,15");
            String[] weightParts = weightsStr.split(",");
            phase.weights = new int[weightParts.length];
            int weightsSum = 0;
            for (int j = 0; j < weightParts.length; j++) {
                phase.weights[j] = Integer.parseInt(weightParts[j].trim());
                weightsSum += phase.weights[j];
            }

            if (weightsSum != 100) {
                throw new ConfigurationException("Bad weights for phase " + i + " with sum: " + weightsSum);
            }

            config.phases[i - 1] = phase;
        }

        System.out.println("Configuration loaded:");
        System.out.printf("Accounts: %d%n", config.numAccounts);
        System.out.printf("Terminals: %d%n", config.terminals);
        for (int i = 0; i < config.phases.length; i++) {
            System.out.printf("Phase %d: duration=%ds, rate=%.2f TPS, weights=%s%n",
                    i + 1, config.phases[i].duration, config.phases[i].rate,
                    Arrays.toString(config.phases[i].weights));
        }

        return config;
    }

    private static XMLConfiguration buildConfiguration(String filename) throws ConfigurationException {
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<XMLConfiguration> builder =
                new FileBasedConfigurationBuilder<>(XMLConfiguration.class)
                        .configure(params.xml()
                                .setFileName(filename)
                                .setListDelimiterHandler(new DisabledListDelimiterHandler())
                                .setExpressionEngine(new XPathExpressionEngine()));
        return builder.getConfiguration();
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption("c", "config", true, "[required] Configuration file");
        options.addOption("id", "clientId", true, "Client ID for BFT-SMaRt proxy");
        options.addOption("s", "shards", true, "Number of shards (default: 1)");
        options.addOption(null, "shard-config", true, "Base path for shard configs");
        options.addOption(null, "create", false, "Create initial accounts");
        options.addOption(null, "execute", false, "Execute benchmark workload");
        options.addOption(null, "inject", false, "Enable injection");
        options.addOption("h", "help", false, "Print this help");
        return options;
    }

    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("SmallBankClient2PC", options);
        System.out.println("\nExamples:");
        System.out.println("  Single shard:");
        System.out.println("    java ... SmallBankClient2PC -c config/smallbank.xml --create --execute");
        System.out.println("\n  Multi-shard (2 shards):");
        System.out.println("    java ... SmallBankClient2PC -c config/smallbank.xml -s 2 --create --execute");
    }

    private static class WorkloadConfig {
        int numAccounts;
        int terminals;
        int randomSeed;
        Phase[] phases;
    }

    private static class Phase {
        int terminals;
        int duration;
        double rate;
        int[] weights;
    }
}
