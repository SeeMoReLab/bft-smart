package bftsmart.demo.smallbank;

import bftsmart.injection.InjectionClient;
import bftsmart.tom.ServiceProxy;
import bftsmart.demo.util.Histogram;
import bftsmart.demo.util.RandomDistribution;
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
 * SmallBank Client for BFT-SMaRt
 * Reads configuration from XML and executes workload with rate limiting
 */
public class SmallBankClient {
    private final Logger measurementLogger = LoggerFactory.getLogger("measurement");
    private final Logger logger = LoggerFactory.getLogger(SmallBankClient.class);

    private static final String SINGLE_LINE = "======================================================================";
    private static final int MAX_LATENCY_MS = 10_000; // bucket upper bound; overflow latencies are clamped

    private final ServiceProxy proxy;
    private final WorkloadConfig config;
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger errorCount = new AtomicInteger(0);
    private final AtomicLong totalLatency = new AtomicLong(0);
    private final Histogram<Long> latencyHistogram = new Histogram<>();

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

            System.out.println(SINGLE_LINE);
            System.out.println("SmallBank BFT-SMaRt Client");
            System.out.println("Configuration: " + configFile);
            System.out.println("Client ID: " + clientId);
            System.out.println(SINGLE_LINE);

            WorkloadConfig config = loadConfiguration(configFile);
            SmallBankClient client = new SmallBankClient(clientId, config);

            if (argsLine.hasOption("create")) {
                System.out.println("Creating accounts...");
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
            System.out.println("Error in client execution" + e.getMessage());
            System.exit(1);
        }
    }

    public SmallBankClient(int clientId, WorkloadConfig config) {
        this.config = config;
        this.proxy = new ServiceProxy(clientId);
        System.out.printf("Client %d initialized%n", clientId);
    }

    private void createAccounts() {
        logger.info("Creating {} accounts...", config.numAccounts);
        long startTime = System.currentTimeMillis();

        for (long custId = 0; custId < config.numAccounts; custId++) {
            String custName = String.format("Customer%010d", custId);
            double savingsBalance = 10000.0;
            double checkingBalance = 10000.0;

            SmallBankMessage msg = SmallBankMessage.newCreateAccountRequest(
                    custId, custName, savingsBalance, checkingBalance);

            try {
                byte[] reply = proxy.invokeOrdered(msg.getBytes());
                SmallBankMessage response = SmallBankMessage.getObject(reply);

                if (response.getResult() != 0) {
                    logger.error("Failed to create account {}: {}", custId, response.getErrorMsg());
                }
            } catch (Exception e) {
                logger.error("Error creating account {}", custId, e);
            }

            if ((custId + 1) % 1000 == 0) {
                logger.info("Created {} accounts", custId + 1);
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        logger.info("Finished creating {} accounts in {} ms", config.numAccounts, duration);
    }

    private void executeWorkload(int phaseNum) {
        resetMetrics();
        ExecutorService executor = Executors.newFixedThreadPool(config.terminals + 1);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(config.terminals);

        // If number of terminals in phase is -1, default to parent level
        int terminals = (config.phases[phaseNum].terminals == -1) ? config.terminals
                : config.phases[phaseNum].terminals;

        logger.info("Starting {} terminals for {} seconds", terminals, config.phases[phaseNum].duration);
        logger.info("Target rate: {} TPS per terminal", config.phases[phaseNum].rate);
        logger.info("Transaction weights: {}", Arrays.toString(config.phases[phaseNum].weights));

        // Create worker threads
        for (int i = 0; i < config.terminals; i++) {
            final int terminalId = i;
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    runTerminal(terminalId, phaseNum);
                } catch (Exception e) {
                    logger.error("Error in terminal {}", terminalId, e);
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        // Start all terminals simultaneously
        // System.out.println("All terminals ready. Starting workload...");
        long workloadStart = System.nanoTime();
        startLatch.countDown();

        // Wait for duration
        try {
            completionLatch.await(config.phases[phaseNum].duration + 10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for completion", e);
        }

        executor.shutdownNow();
        long workloadEnd = System.nanoTime();

        // Print results
        printResults(phaseNum, workloadStart, workloadEnd);
    }

    private void runTerminal(int terminalId, int phaseNum) {
        long seed = computeTerminalSeed(terminalId, phaseNum);
        Random terminalRandom = new Random(seed);
        RandomDistribution.Flat accountRng = new RandomDistribution.Flat(new Random(seed ^ 0x9E3779B97F4A7C15L),
                0, config.numAccounts);
        long startTime = System.nanoTime();
        long endTime = startTime + TimeUnit.SECONDS.toNanos(config.phases[phaseNum].duration);

        // Calculate interval between transactions for rate limiting
        long intervalNs = (long) (1_000_000_000.0 / config.phases[phaseNum].rate);
        long nextTransactionTime = startTime;

        int txCount = 0;

        while (System.nanoTime() < endTime) {
            // Rate limiting: wait until next transaction time
            long now = System.nanoTime();
            long waitTime = nextTransactionTime - now;
            if (waitTime > 0) {
                try {
                    Thread.sleep(waitTime / 1_000_000, (int) (waitTime % 1_000_000));
                } catch (InterruptedException e) {
                    break;
                }
            }

            // Execute transaction
            SmallBankMessage.TransactionType txType = selectTransactionType(terminalRandom,
                    config.phases[phaseNum].weights);
            long txStart = System.nanoTime();
            boolean success = executeTransaction(txType, terminalRandom, accountRng);
            long txEnd = System.nanoTime();

            // Record results
            if (success) {
                successCount.incrementAndGet();
                long latency = txEnd - txStart;
                totalLatency.addAndGet(latency);
                recordLatency(latency);
            } else {
                errorCount.incrementAndGet();
            }

            txCount++;
            nextTransactionTime += intervalNs;

            // Log progress periodically
            if (txCount % 100 == 0 && terminalId == 0) {
                logger.debug("Terminal {} executed {} transactions", terminalId, txCount);
            }
        }

        logger.info("Terminal {} completed {} transactions", terminalId, txCount);
    }

    private SmallBankMessage.TransactionType selectTransactionType(Random rnd, int[] weights) {
        int totalWeight = 0;
        for (int weight : weights) {
            totalWeight += weight;
        }

        int randomValue = rnd.nextInt(totalWeight);
        int cumulativeWeight = 0;

        for (int i = 0; i < weights.length; i++) {
            cumulativeWeight += weights[i];
            if (randomValue < cumulativeWeight) {
                return SmallBankMessage.TransactionType.values()[i];
            }
        }

        return SmallBankMessage.TransactionType.WRITE_CHECK; // Default
    }

    private boolean executeTransaction(SmallBankMessage.TransactionType type, Random rnd,
            RandomDistribution.Flat accountRng) {
        try {
            SmallBankMessage msg = null;
            long custId1 = accountRng.nextLong();
            long custId2 = accountRng.nextLong();
            while (custId2 == custId1) {
                custId2 = accountRng.nextLong();
            }
            double amount = 1.0 + rnd.nextDouble() * 99.0; // 1-100
            logger.debug("Executing Type {}", type.toString());
            switch (type) {
                case DEPOSIT_CHECKING:
                    msg = SmallBankMessage.newDepositCheckingRequest(custId1, amount);
                    break;
                case TRANSACT_SAVINGS:
                    msg = SmallBankMessage.newTransactSavingsRequest(custId1, amount);
                    break;
                case WRITE_CHECK:
                    msg = SmallBankMessage.newWriteCheckRequest(custId1, amount);
                    logger.debug("Executing WriteCheck: {}", msg);
                    break;
                case SEND_PAYMENT:
                    msg = SmallBankMessage.newSendPaymentRequest(custId1, custId2, amount);
                    break;
                case AMALGAMATE:
                    msg = SmallBankMessage.newAmalgamateRequest(custId1, custId2);
                    break;
                case BALANCE:
                    // Read operation - can use unordered
                    msg = SmallBankMessage.newBalanceRequest(custId1);
                    byte[] reply = proxy.invokeUnordered(msg.getBytes());
                    SmallBankMessage response = SmallBankMessage.getObject(reply);
                    logger.debug("Savings balance: {}", response.getSavingsBalance());
                    return response.getResult() == 0;
            }

            byte[] reply = proxy.invokeOrdered(msg.getBytes());
            SmallBankMessage response = SmallBankMessage.getObject(reply);

            return response.getResult() == 0;

        } catch (Exception e) {
            logger.error("Error executing transaction {}", type, e);
            return false;
        }
    }

    private void printResults(int phaseNum, long startNs, long endNs) {
        double durationSeconds = (endNs - startNs) / 1_000_000_000.0;
        int totalTxns = successCount.get() + errorCount.get();
        double throughput = totalTxns / durationSeconds;
        int successes = successCount.get();
        double avgLatency = successes == 0 ? 0 : totalLatency.get() / 1_000_000.0 / successes;

        long p50 = getPercentileFromHistogram(0.50);
        long p95 = getPercentileFromHistogram(0.95);
        long p99 = getPercentileFromHistogram(0.99);

        measurementLogger.info(SINGLE_LINE);
        measurementLogger.info("Phase {} results:", phaseNum);
        measurementLogger.info("duration: {} seconds, total trxs: {}, successful: {}, errors: {}", durationSeconds,
                totalTxns, successCount.get(), errorCount.get());
        measurementLogger.info("throughput: {} TPS, avg_latency: {} ms, p50: {} ms, p95: {} ms, p99: {} ms",
                throughput, avgLatency, p50, p95, p99);
        measurementLogger.info(SINGLE_LINE);
    }

    private long getPercentileFromHistogram(double percentile) {
        int successes = successCount.get();
        if (successes == 0) {
            return 0;
        }

        long target = (long) Math.ceil(percentile * successes);
        long cumulative = 0;
        for (Long bucket : latencyHistogram.values()) {
            Integer count = latencyHistogram.get(bucket);
            if (count == null) {
                continue;
            }
            cumulative += count;
            if (cumulative >= target) {
                return bucket;
            }
        }
        return MAX_LATENCY_MS;
    }

    private void recordLatency(long latencyNs) {
        long latencyMs = TimeUnit.NANOSECONDS.toMillis(latencyNs);
        long bucket = Math.min(latencyMs, MAX_LATENCY_MS);
        latencyHistogram.put(bucket);
    }

    private void resetMetrics() {
        successCount.set(0);
        errorCount.set(0);
        totalLatency.set(0);
        latencyHistogram.clear();
    }

    private long computeTerminalSeed(int terminalId, int phaseNum) {
        return ((long) config.randomSeed * 31 + terminalId * 17L) ^ (phaseNum * 1_003L);
    }

    private void close() {
        proxy.close();
    }

    private static WorkloadConfig loadConfiguration(String configFile) throws ConfigurationException {
        XMLConfiguration xml = buildConfiguration(configFile);
        WorkloadConfig config = new WorkloadConfig();

        config.numAccounts = xml.getInt("numAccounts", 100000);
        config.terminals = xml.getInt("terminals", 1);
        config.randomSeed = xml.getInt("randomSeed", 17);
        int size = xml.configurationsAt("/works/work").size();
        config.phases = new Phase[size];
        for (int i = 1; i < size + 1; i++) {
            final HierarchicalConfiguration<ImmutableNode> work = xml.configurationAt("works/work[" + i + "]");
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
                throw new ConfigurationException("Bad weights for phase " + i + "with sum: " + weightsSum);
            }

            config.phases[i - 1] = phase;
        }

        System.out.println("Configuration loaded:");
        System.out.printf("Accounts: %d%n", config.numAccounts);
        System.out.printf("Terminals: %d%n", config.terminals);
        for (int i = 0; i < config.phases.length; i++) {
            System.out.printf("Phase %d:%n", i + 1);
            System.out.printf("Duration: %d seconds%n", config.phases[i].duration);
            System.out.printf("Rate: %.2f TPS/terminal%n", config.phases[i].rate);
            System.out.println("Weights: " + Arrays.toString(config.phases[i].weights));
        }

        return config;
    }

    private static XMLConfiguration buildConfiguration(String filename) throws ConfigurationException {
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<XMLConfiguration> builder = new FileBasedConfigurationBuilder<>(
                XMLConfiguration.class)
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
        options.addOption(null, "create", false, "Create initial accounts");
        options.addOption(null, "execute", false, "Execute benchmark workload");
        options.addOption(null, "inject", false, "Enable injection");
        options.addOption("h", "help", false, "Print this help");
        return options;
    }

    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("SmallBankClient", options);
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
