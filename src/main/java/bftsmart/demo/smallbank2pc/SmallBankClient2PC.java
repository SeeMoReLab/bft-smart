package bftsmart.demo.smallbank2pc;

import bftsmart.demo.util.Histogram;
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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * SmallBank Client for BFT-SMaRt with 2PC support for cross-shard transactions.
 *
 * This client can operate in two modes:
 * 1. Single-shard mode: All transactions go to one shard (legacy behavior)
 * 2. Multi-shard mode: Transactions are routed based on account partitioning,
 *    and cross-shard transactions use 2PC protocol
 */
public class SmallBankClient2PC {
    private final Logger measurementLogger = LoggerFactory.getLogger("measurement");
    private static final Logger LOG = LoggerFactory.getLogger(SmallBankClient2PC.class);
    private static final String SINGLE_LINE = "======================================================================";
    private static final int MAX_LATENCY_MS = 10_000;
    private static final String DEFAULT_CONFIG_HOME = "config";
    private static final String WORKLOAD_FILE_NAME = "smallbank.xml";
    private static final int ACCOUNT_CREATION_TERMINALS = 100;

    // Client configuration
    private final int clientId;
    private final WorkloadConfig config;

    // Shard configuration
    private final int numShards;
    private final Map<Integer, ServiceProxy> shardProxies;
    private final Map<Integer, String> shardConfigPaths;

    // Statistics
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger abortCount = new AtomicInteger(0);
    private final AtomicInteger errorCount = new AtomicInteger(0);
    private final AtomicInteger crossShardCount = new AtomicInteger(0);
    private final AtomicLong totalLatency = new AtomicLong(0);
    private final Histogram<Long> latencyHistogram = new Histogram<>();

    private enum TxOutcome {
        SUCCESS,
        ABORTED,
        ERROR
    }

    public static void main(String[] args) {
        try {
            CommandLineParser parser = new DefaultParser();
            Options options = buildOptions();
            CommandLine argsLine = parser.parse(options, args);

            if (argsLine.hasOption("h")) {
                printUsage(options);
                return;
            }

            List<String> positionalArgs = argsLine.getArgList();
            if (!positionalArgs.isEmpty()) {
                printUsage(options);
                return;
            }

            String configHome = argsLine.getOptionValue("config-dir", DEFAULT_CONFIG_HOME);
            String configFile = resolveWorkloadConfigFile(configHome);
            int clientId = Integer.parseInt(argsLine.getOptionValue("id", "0"));
            int numShards = Integer.parseInt(argsLine.getOptionValue("shards", "1"));
            String shardConfigBase = argsLine.getOptionValue("shard-config", "");
            Long startUnixMs = parseStartUnixMs(argsLine.getOptionValue("start-unix-ms"));

            System.out.println(SINGLE_LINE);
            System.out.println("SmallBank BFT-SMaRt Client (2PC Enabled)");
            System.out.println("Configuration home: " + defaultConfigHome(configHome));
            System.out.println("Workload file: " + configFile);
            System.out.println("Client ID: " + clientId);
            System.out.println("Number of shards: " + numShards);
            if (!shardConfigBase.isEmpty()) {
                System.out.println("Shard config base: " + shardConfigBase);
            }
            if (startUnixMs != null) {
                System.out.println("Benchmark start unix ms: " + startUnixMs + " (" + formatEpochMs(startUnixMs) + ")");
            } else {
                System.out.println("Benchmark start unix ms: not set (execute starts immediately)");
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

            if (argsLine.hasOption("execute")) {
                System.out.println("Executing workload...");
                waitUntilStartUnixMs(startUnixMs);
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
        this.numShards = numShards;
        this.shardConfigPaths = new HashMap<>(shardConfigPaths);
        this.shardProxies = new ConcurrentHashMap<>();

        // Initialize proxies for each shard
        initializeShardProxies();

        System.out.printf("Client %d initialized with %d shards%n", clientId, numShards);
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
        ExecutorService executor = Executors.newFixedThreadPool(ACCOUNT_CREATION_TERMINALS);
        AtomicLong processed = new AtomicLong(0);
        AtomicInteger creationErrors = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();

        for (int terminalId = 0; terminalId < ACCOUNT_CREATION_TERMINALS; terminalId++) {
            final int workerId = terminalId;
            futures.add(executor.submit(() -> {
                for (long custId = workerId; custId < config.numAccounts; custId += ACCOUNT_CREATION_TERMINALS) {
                    String custName = String.format("Customer%010d", custId);
                    double savingsBalance = 10000.0;
                    double checkingBalance = 10000.0;

                    SmallBankMessage2PC msg = SmallBankMessage2PC.newCreateAccountRequest(
                            custId, custName, savingsBalance, checkingBalance
                    );

                    int targetShard = getShardForAccount(custId);
                    ServiceProxy proxy = shardProxies.get(targetShard);

                    try {
                        byte[] reply = proxy.invokeOrdered(msg.getBytes());
                        SmallBankMessage2PC response = SmallBankMessage2PC.getObject(reply);
                        if (response == null || response.getResult() != 0) {
                            creationErrors.incrementAndGet();
                            String error = response != null ? response.getErrorMsg() : "null response";
                            System.out.println("Failed to create account " + custId + " on shard " +
                                    targetShard + ": " + error);
                        }
                    } catch (Exception e) {
                        creationErrors.incrementAndGet();
                        System.out.println("Error creating account " + custId + ": " + e);
                    }

                    long current = processed.incrementAndGet();
                    if (current % 1000 == 0) {
                        System.out.printf("Created %d accounts%n", current);
                    }
                }
            }));
        }

        executor.shutdown();
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException e) {
                creationErrors.incrementAndGet();
                System.out.println("Account creation task failed: " + e.getCause());
            }
        }
        if (!executor.isTerminated()) {
            executor.shutdownNow();
        }

        long duration = System.currentTimeMillis() - startTime;
        long finishUnixMs = startTime + duration;
        System.out.printf("Finished creating %d accounts in %d ms (errors=%d)%n",
                processed.get(), duration, creationErrors.get());
        System.out.printf("Account creation finished at unix ms: %d (%s)%n",
                finishUnixMs, formatEpochMs(finishUnixMs));
    }

    private void executeWorkload(int phaseNum) {
        // Reset statistics for this phase
        successCount.set(0);
        abortCount.set(0);
        errorCount.set(0);
        crossShardCount.set(0);
        totalLatency.set(0);
        latencyHistogram.clear();

        int terminals = (config.phases[phaseNum].terminals == -1) ?
                        config.terminals : config.phases[phaseNum].terminals;

        ExecutorService executor = Executors.newFixedThreadPool(terminals);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(terminals);

        System.out.printf("Starting %d terminals for %d seconds%n", terminals, config.phases[phaseNum].duration);
        if (config.phases[phaseNum].rate == 0.0) {
            System.out.println("Target rate: SATURATE (rate=0)");
        } else {
            System.out.printf("Target rate: %.2f TPS per terminal%n", config.phases[phaseNum].rate);
        }
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
        MetricsSnapshot phaseBaseline = captureSnapshot();
        ScheduledExecutorService monitorExec = startMonitor(phaseNum, phaseBaseline);
        long workloadStart = System.nanoTime();
        startLatch.countDown();

        try {
            boolean completed = completionLatch.await(config.phases[phaseNum].duration + 10L, TimeUnit.SECONDS);
            if (!completed) {
                LOG.warn("Workload phase {} did not complete before timeout", phaseNum + 1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while waiting for completion", e);
        }

        executor.shutdownNow();
        if (monitorExec != null) {
            monitorExec.shutdownNow();
        }
        long workloadEnd = System.nanoTime();
        MetricsSnapshot phaseAfter = captureSnapshot();
        MetricsSnapshot phaseDelta = computeDelta(phaseBaseline, phaseAfter);
        printResults("Phase " + (phaseNum + 1), (workloadEnd - workloadStart) / 1_000_000_000.0, phaseDelta);
    }

    private void runTerminal(int terminalId, int phaseNum) {
        Random terminalRandom = new Random(config.randomSeed + terminalId);
        long startTime = System.nanoTime();
        long endTime = startTime + TimeUnit.SECONDS.toNanos(config.phases[phaseNum].duration);

        boolean saturate = config.phases[phaseNum].rate == 0.0;
        long intervalNs = saturate ? 0L : Math.max(1L, (long) (1_000_000_000.0 / config.phases[phaseNum].rate));
        long nextTransactionTime = startTime;

        int txCount = 0;

        while (System.nanoTime() < endTime) {
            long now = System.nanoTime();
            if (!saturate) {
                long waitTime = nextTransactionTime - now;
                if (waitTime > 0) {
                    try {
                        Thread.sleep(waitTime / 1_000_000, (int) (waitTime % 1_000_000));
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }

            SmallBankMessage2PC.TransactionType txType = selectTransactionType(
                    terminalRandom, config.phases[phaseNum].weights);

            long txStart = System.nanoTime();
            TxOutcome outcome = executeTransaction(txType, terminalRandom);
            long txEnd = System.nanoTime();

            if (outcome == TxOutcome.SUCCESS) {
                successCount.incrementAndGet();
                long latency = txEnd - txStart;
                totalLatency.addAndGet(latency);
                recordLatency(latency);
            } else if (outcome == TxOutcome.ABORTED) {
                abortCount.incrementAndGet();
            } else {
                errorCount.incrementAndGet();
            }

            txCount++;
            if (!saturate) {
                nextTransactionTime += intervalNs;
                while (nextTransactionTime < System.nanoTime()) {
                    nextTransactionTime += intervalNs;
                }
            }

            if (txCount % 100 == 0 && terminalId == 0) {
                LOG.debug("Terminal {} executed {} transactions", terminalId, txCount);
            }
        }

        LOG.debug("Terminal {} completed {} transactions in phase {}", terminalId, txCount, phaseNum + 1);
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

    private TxOutcome executeTransaction(SmallBankMessage2PC.TransactionType type, Random rnd) {
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
                return executeSingleShardTransaction(type, custId1, custId2, amount)
                        ? TxOutcome.SUCCESS : TxOutcome.ERROR;
            }

        } catch (Exception e) {
            LOG.error("Error executing transaction {}", type, e);
            return TxOutcome.ERROR;
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
            return isSuccessOrExpectedBusinessOutcome(response);
        }

        byte[] reply = proxy.invokeOrdered(msg.getBytes());
        SmallBankMessage2PC response = SmallBankMessage2PC.getObject(reply);
        return isSuccessOrExpectedBusinessOutcome(response);
    }

    /**
     * Execute a cross-shard transaction using leader-based 2PC.
     * Sends CROSS_SHARD_REQUEST to the coordinator shard (shard containing sender account).
     * The leader of that shard will coordinate the 2PC.
     */
    private TxOutcome executeCrossShardTransaction(SmallBankMessage2PC.TransactionType type,
                                                   long custId1, long custId2, double amount) {
        // Send to coordinator shard (shard of sender/source account)
        int coordinatorShard = getShardForAccount(custId1);

        LOG.debug("Executing cross-shard {} between accounts {} (shard {}) and {} (shard {}), coordinator={}",
                  type, custId1, coordinatorShard, custId2, getShardForAccount(custId2), coordinatorShard);

        SmallBankMessage2PC request = SmallBankMessage2PC.newCrossShardRequest(
                type, custId1, custId2, amount);

        ServiceProxy proxy = shardProxies.get(coordinatorShard);
        byte[] reply = proxy.invokeCrossShardRequest(request.getBytes());

        SmallBankMessage2PC response = SmallBankMessage2PC.getObject(reply);
        if (response == null) {
            return TxOutcome.ERROR;
        }
        if (response.getResult() == 0) {
            return TxOutcome.SUCCESS;
        }
        if (isInsufficientFundsResponse(response)) {
            return TxOutcome.SUCCESS;
        }
        if (is2pcAbortResponse(response)) {
            return TxOutcome.ABORTED;
        }
        return TxOutcome.ERROR;
    }

    private boolean isSuccessOrExpectedBusinessOutcome(SmallBankMessage2PC response) {
        if (response == null) {
            return false;
        }
        return response.getResult() == 0 || isInsufficientFundsResponse(response);
    }

    private boolean isInsufficientFundsResponse(SmallBankMessage2PC response) {
        if (response == null) {
            return false;
        }
        String errorMsg = response.getErrorMsg();
        if (errorMsg == null) {
            return false;
        }
        return errorMsg.toLowerCase(Locale.ROOT).contains("insufficient funds");
    }

    private boolean is2pcAbortResponse(SmallBankMessage2PC response) {
        if (response == null) {
            return false;
        }
        String errorMsg = response.getErrorMsg();
        if (errorMsg == null) {
            return false;
        }
        String normalized = errorMsg.toLowerCase(Locale.ROOT);
        return normalized.contains("2pc prepare failed")
                || normalized.contains("2pc prepare timeout")
                || normalized.contains("prepare failed")
                || normalized.contains("prepare timeout")
                || normalized.contains("abort");
    }

    /**
     * Build a transaction message.
     */
    private SmallBankMessage2PC buildTransactionMessage(SmallBankMessage2PC.TransactionType type,
                                                         long custId1, long custId2, double amount) {
        // String txId = generateTransactionId();
        switch (type) {
            case DEPOSIT_CHECKING:
                return SmallBankMessage2PC.newDepositCheckingRequest(custId1, amount, null);
            case TRANSACT_SAVINGS:
                return SmallBankMessage2PC.newTransactSavingsRequest(custId1, amount);
            case WRITE_CHECK:
                return SmallBankMessage2PC.newWriteCheckRequest(custId1, amount, null);
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

    // Results

    private void printResults(String label, double durationSeconds, MetricsSnapshot delta) {
        int totalTxns = (int) (delta.success + delta.aborted + delta.errors);
        double throughput = durationSeconds == 0 ? 0 : totalTxns / durationSeconds;
        double avgLatency = delta.success == 0 ? 0 : delta.totalLatencyNs / 1_000_000.0 / delta.success;

        long p50 = getPercentileFromHistogram(delta.latencyHistogram, delta.success, 0.50);
        long p95 = getPercentileFromHistogram(delta.latencyHistogram, delta.success, 0.95);
        long p99 = getPercentileFromHistogram(delta.latencyHistogram, delta.success, 0.99);

        if (label.startsWith("Monitor")) {
            measurementLogger.info(
                    "{} duration={}s trxs={} succ={} aborted={} err={} cross_shard={} tps={} avg_ms={} p50={} p95={} p99={}",
                    label, durationSeconds, totalTxns, delta.success, delta.aborted, delta.errors, delta.crossShard, throughput,
                    avgLatency, p50, p95, p99);
            return;
        }

        measurementLogger.info(SINGLE_LINE);
        measurementLogger.info("{} results:", label);
        measurementLogger.info(
                "duration: {} seconds, total trxs: {}, successful: {}, aborted: {}, errors: {}, cross-shard: {}",
                durationSeconds, totalTxns, delta.success, delta.aborted, delta.errors, delta.crossShard);
        measurementLogger.info(
                "throughput: {} TPS, avg_latency: {} ms, p50: {} ms, p95: {} ms, p99: {} ms",
                throughput, avgLatency, p50, p95, p99);
        measurementLogger.info(SINGLE_LINE);
    }

    private long getPercentileFromHistogram(Histogram<Long> histogram, long successes, double percentile) {
        if (successes == 0) {
            return 0;
        }

        long target = (long) Math.ceil(percentile * successes);
        long cumulative = 0;
        synchronized (histogram) {
            for (Long bucket : histogram.values()) {
                int count = histogram.get(bucket, 0);
                cumulative += count;
                if (cumulative >= target) {
                    return bucket;
                }
            }
        }
        return MAX_LATENCY_MS;
    }

    private void recordLatency(long latencyNs) {
        long latencyMs = TimeUnit.NANOSECONDS.toMillis(latencyNs);
        long bucket = Math.min(latencyMs, MAX_LATENCY_MS);
        latencyHistogram.put(bucket);
    }

    private MetricsSnapshot captureSnapshot() {
        return new MetricsSnapshot(
                successCount.get(),
                abortCount.get(),
                errorCount.get(),
                crossShardCount.get(),
                totalLatency.get(),
                copyHistogram(latencyHistogram));
    }

    private MetricsSnapshot computeDelta(MetricsSnapshot before, MetricsSnapshot after) {
        long successDelta = after.success - before.success;
        long abortDelta = after.aborted - before.aborted;
        long errorDelta = after.errors - before.errors;
        long crossShardDelta = after.crossShard - before.crossShard;
        long latencyDelta = after.totalLatencyNs - before.totalLatencyNs;
        Histogram<Long> deltaHistogram = diffHistogram(before.latencyHistogram, after.latencyHistogram);
        return new MetricsSnapshot(successDelta, abortDelta, errorDelta, crossShardDelta, latencyDelta, deltaHistogram);
    }

    private Histogram<Long> copyHistogram(Histogram<Long> source) {
        Histogram<Long> copy = new Histogram<>();
        synchronized (source) {
            for (Long bucket : source.values()) {
                int count = source.get(bucket, 0);
                if (count > 0) {
                    copy.put(bucket, count);
                }
            }
        }
        return copy;
    }

    private Histogram<Long> diffHistogram(Histogram<Long> before, Histogram<Long> after) {
        Histogram<Long> delta = new Histogram<>();
        synchronized (after) {
            for (Long bucket : after.values()) {
                int afterCount = after.get(bucket, 0);
                int beforeCount = before.get(bucket, 0);
                int diff = afterCount - beforeCount;
                if (diff > 0) {
                    delta.put(bucket, diff);
                }
            }
        }
        return delta;
    }

    private ScheduledExecutorService startMonitor(int phaseNum, MetricsSnapshot initialBaseline) {
        if (config.monitorIntervalSec <= 0) {
            return null;
        }
        ScheduledExecutorService monitorExec = Executors.newSingleThreadScheduledExecutor();
        AtomicReference<MetricsSnapshot> baselineRef = new AtomicReference<>(initialBaseline);
        AtomicLong lastSampleNs = new AtomicLong(System.nanoTime());
        monitorExec.scheduleAtFixedRate(() -> {
            try {
                MetricsSnapshot current = captureSnapshot();
                MetricsSnapshot delta = computeDelta(baselineRef.get(), current);
                long now = System.nanoTime();
                double durationSeconds = (now - lastSampleNs.get()) / 1_000_000_000.0;
                printResults("Monitor phase=" + (phaseNum + 1), durationSeconds, delta);
                baselineRef.set(current);
                lastSampleNs.set(now);
            } catch (Exception e) {
                LOG.warn("Error while sampling monitor metrics for phase {}", phaseNum + 1, e);
            }
        }, config.monitorIntervalSec, config.monitorIntervalSec, TimeUnit.SECONDS);
        return monitorExec;
    }

    private void close() {
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
        config.monitorIntervalSec = xml.getInt("monitorInterval", 0);

        int size = xml.configurationsAt("/works/work").size();
        config.phases = new Phase[size];

        for (int i = 1; i < size + 1; i++) {
            final HierarchicalConfiguration<ImmutableNode> work =
                    xml.configurationAt("works/work[" + i + "]");
            Phase phase = new Phase();
            phase.terminals = work.getInt("terminals", -1);
            phase.duration = work.getInt("time");
            phase.rate = work.getDouble("rate");
            if (phase.rate < 0.0) {
                throw new ConfigurationException("Bad rate for phase " + i + ": rate must be >= 0");
            }

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
        System.out.printf("Monitor interval: %d seconds%n", config.monitorIntervalSec);
        for (int i = 0; i < config.phases.length; i++) {
            if (config.phases[i].rate == 0.0) {
                System.out.printf("Phase %d: duration=%ds, rate=SATURATE (rate=0), weights=%s%n",
                        i + 1, config.phases[i].duration,
                        Arrays.toString(config.phases[i].weights));
            } else {
                System.out.printf("Phase %d: duration=%ds, rate=%.2f TPS, weights=%s%n",
                        i + 1, config.phases[i].duration, config.phases[i].rate,
                        Arrays.toString(config.phases[i].weights));
            }
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

    private static String defaultConfigHome(String configHome) {
        return (configHome == null || configHome.isBlank()) ? DEFAULT_CONFIG_HOME : configHome;
    }

    private static String resolveWorkloadConfigFile(String configHome) {
        Path configDir = Paths.get(defaultConfigHome(configHome));
        Path workloadFile = configDir.resolve(WORKLOAD_FILE_NAME);
        return workloadFile.toString();
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption("id", "clientId", true, "Client ID for BFT-SMaRt proxy");
        options.addOption(null, "config-dir", true, "Configuration directory (default: config)");
        options.addOption(null, "start-unix-ms", true, "Absolute Unix epoch time in ms for benchmark start");
        options.addOption("s", "shards", true, "Number of shards (default: 1)");
        options.addOption(null, "shard-config", true, "Base path for shard configs");
        options.addOption(null, "create", false, "Create initial accounts");
        options.addOption(null, "execute", false, "Execute benchmark workload");
        options.addOption("h", "help", false, "Print this help");
        return options;
    }

    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("SmallBankClient2PC", options);
        System.out.println("\nExamples:");
        System.out.println("  Single shard:");
        System.out.println("    java ... SmallBankClient2PC --config-dir config --create --execute");
        System.out.println("\n  Multi-shard (2 shards):");
        System.out.println("    java ... SmallBankClient2PC --config-dir config -s 2 --create --execute");
        System.out.println("    java ... SmallBankClient2PC --config-dir config -s 2 --execute --start-unix-ms 1735689600000");
    }

    private static Long parseStartUnixMs(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        long parsed;
        try {
            parsed = Long.parseLong(value.trim());
        } catch (NumberFormatException numberFormatException) {
            throw new IllegalArgumentException("Invalid --start-unix-ms value: " + value);
        }
        if (parsed < 0) {
            throw new IllegalArgumentException("--start-unix-ms must be >= 0");
        }
        return parsed;
    }

    private static String formatEpochMs(long epochMs) {
        return Instant.ofEpochMilli(epochMs).toString();
    }

    private static void waitUntilStartUnixMs(Long startUnixMs) {
        if (startUnixMs == null) {
            return;
        }
        while (true) {
            long remainingMs = startUnixMs - System.currentTimeMillis();
            if (remainingMs <= 0) {
                return;
            }
            long sleepMs = Math.min(remainingMs, 200L);
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static class WorkloadConfig {
        int numAccounts;
        int terminals;
        int randomSeed;
        int monitorIntervalSec;
        Phase[] phases;
    }

    private static class MetricsSnapshot {
        final long success;
        final long aborted;
        final long errors;
        final long crossShard;
        final long totalLatencyNs;
        final Histogram<Long> latencyHistogram;

        MetricsSnapshot() {
            this(0, 0, 0, 0, 0, new Histogram<>());
        }

        MetricsSnapshot(long success, long aborted, long errors, long crossShard, long totalLatencyNs,
                        Histogram<Long> latencyHistogram) {
            this.success = success;
            this.aborted = aborted;
            this.errors = errors;
            this.crossShard = crossShard;
            this.totalLatencyNs = totalLatencyNs;
            this.latencyHistogram = latencyHistogram;
        }
    }

    private static class Phase {
        int terminals;
        int duration;
        double rate;
        int[] weights;
    }
}
