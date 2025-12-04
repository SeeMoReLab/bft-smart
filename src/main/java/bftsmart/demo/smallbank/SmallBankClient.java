package bftsmart.demo.smallbank;

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
 * SmallBank Client for BFT-SMaRt
 * Reads configuration from XML and executes workload with rate limiting
 */
public class SmallBankClient {
    private static final Logger LOG = LoggerFactory.getLogger(SmallBankClient.class);
    private static final String SINGLE_LINE = "======================================================================";

    private final ServiceProxy proxy;
    private final WorkloadConfig config;
    private final Random random;
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger errorCount = new AtomicInteger(0);
    private final AtomicLong totalLatency = new AtomicLong(0);
    private final List<Long> latencies = new CopyOnWriteArrayList<>();

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
            LOG.error("Error in client execution", e);
            System.exit(1);
        }
    }

    public SmallBankClient(int clientId, WorkloadConfig config) {
        this.config = config;
        this.random = new Random(config.randomSeed + clientId);
        this.proxy = new ServiceProxy(clientId);
        System.out.printf("Client %d initialized%n", clientId);
    }

    private void createAccounts() {
        System.out.printf("Creating %d accounts...%n", config.numAccounts);
        long startTime = System.currentTimeMillis();

        for (long custId = 0; custId < config.numAccounts; custId++) {
            String custName = String.format("Customer%010d", custId);
            double savingsBalance = 10000.0;
            double checkingBalance = 10000.0;

            SmallBankMessage msg = SmallBankMessage.newCreateAccountRequest(
                    custId, custName, savingsBalance, checkingBalance
            );

            try {
                byte[] reply = proxy.invokeOrdered(msg.getBytes());
                SmallBankMessage response = SmallBankMessage.getObject(reply);

                if (response.getResult() != 0) {
                    System.out.println("Failed to create account " +  custId + ": " + response.getErrorMsg());
                }
            } catch (Exception e) {
                System.out.println("Error creating account " +  custId + ": " + e);
            }

            if ((custId + 1) % 1000 == 0) {
                System.out.printf("Created %d accounts%n", custId + 1);
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        System.out.printf("Finished creating %d accounts in %d ms%n", config.numAccounts, duration);
    }

    private void executeWorkload(int phaseNum) {
        ExecutorService executor = Executors.newFixedThreadPool(config.terminals + 1);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(config.terminals);

        // If number of terminals in phase is -1, default to parent level
        int terminals = (config.phases[phaseNum].terminals == -1)? config.terminals: config.phases[phaseNum].terminals;

        System.out.printf("Starting %d terminals for %d seconds%n", terminals, config.phases[phaseNum].duration);
        System.out.printf("Target rate: %.2f TPS per terminal%n", config.phases[phaseNum].rate);
        System.out.println("Transaction weights: " + Arrays.toString(config.phases[phaseNum].weights));

        // Create worker threads
        for (int i = 0; i < config.terminals; i++) {
            final int terminalId = i;
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    runTerminal(terminalId, phaseNum);
                } catch (Exception e) {
                    System.out.println("Error in terminal" + terminalId + ": " + e);
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        // Start all terminals simultaneously
        System.out.println("All terminals ready. Starting workload...");
        long workloadStart = System.nanoTime();
        startLatch.countDown();

        // Wait for duration
        try {
            completionLatch.await(config.phases[phaseNum].duration + 10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for completion", e);
        }

        executor.shutdownNow();
        long workloadEnd = System.nanoTime();

        // Print results
        printResults(phaseNum, workloadStart, workloadEnd);
    }

    private void runTerminal(int terminalId, int phaseNum) {
        Random terminalRandom = new Random(config.randomSeed + terminalId);
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
            SmallBankMessage.TransactionType txType = selectTransactionType(terminalRandom, config.phases[phaseNum].weights);
            long txStart = System.nanoTime();
            boolean success = executeTransaction(txType, terminalRandom);
            long txEnd = System.nanoTime();

            // Record results
            if (success) {
                successCount.incrementAndGet();
                long latency = txEnd - txStart;
                totalLatency.addAndGet(latency);
                latencies.add(latency / 1_000_000); // Convert to ms
            } else {
                errorCount.incrementAndGet();
            }

            txCount++;
            nextTransactionTime += intervalNs;

            // Log progress periodically
            if (txCount % 100 == 0 && terminalId == 0) {
                LOG.debug("Terminal {} executed {} transactions", terminalId, txCount);
            }
        }

        System.out.printf("Terminal %d completed %d transactions", terminalId, txCount);
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

    private boolean executeTransaction(SmallBankMessage.TransactionType type, Random rnd) {
        try {
            SmallBankMessage msg = null;
            long custId1 = rnd.nextInt(config.numAccounts);
            long custId2 = rnd.nextInt(config.numAccounts);
            while (custId2 == custId1) {
                custId2 = rnd.nextInt(config.numAccounts);
            }
            double amount = 1.0 + rnd.nextDouble() * 99.0; // 1-100
            System.out.println("Executing Type " + type.toString());
            switch (type) {
                case DEPOSIT_CHECKING:
                    msg = SmallBankMessage.newDepositCheckingRequest(custId1, amount);
                    break;
                case TRANSACT_SAVINGS:
                    msg = SmallBankMessage.newTransactSavingsRequest(custId1, amount);
                    break;
                case WRITE_CHECK:
                    msg = SmallBankMessage.newWriteCheckRequest(custId1, amount);
                    System.out.println("Executing WriteCheck: " + msg);
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
                    System.out.println("Savings balance: " + response.getSavingsBalance());
                    return response.getResult() == 0;
            }

            byte[] reply = proxy.invokeOrdered(msg.getBytes());
            SmallBankMessage response = SmallBankMessage.getObject(reply);

            return response.getResult() == 0;

        } catch (Exception e) {
            LOG.error("Error executing transaction {}", type, e);
            return false;
        }
    }

    private void printResults(int phaseNum, long startNs, long endNs) {
        double durationSeconds = (endNs - startNs) / 1_000_000_000.0;
        int totalTxns = successCount.get() + errorCount.get();
        double throughput = totalTxns / durationSeconds;
        double avgLatency = totalLatency.get() / 1_000_000.0 / successCount.get();

        // Calculate percentiles
        Collections.sort(latencies);
        long p50 = getPercentile(latencies, 0.50);
        long p95 = getPercentile(latencies, 0.95);
        long p99 = getPercentile(latencies, 0.99);

        System.out.println(SINGLE_LINE);
        System.out.printf("Phase %d results", phaseNum);
        System.out.println(SINGLE_LINE);
        System.out.println("Workload Results:");
        System.out.printf("Duration: %.2f seconds%n", durationSeconds);
        System.out.printf("Total Transactions: %d%n", totalTxns);
        System.out.printf("Successful: %d%n", successCount.get());
        System.out.printf("Errors: %d%n", errorCount.get());
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
                throw new ConfigurationException("Bad weights for phase " + i + "with sum: " + weightsSum);
            }

            config.phases[i - 1] = phase;
        }

        System.out.println("Configuration loaded:");
        System.out.printf("Accounts: %d%n", config.numAccounts);
        System.out.printf("Terminals: %d%n", config.terminals);
        for (int i = 0; i < config.phases.length; i++) {
            System.out.printf("Phase %d:%n", i+1);
            System.out.printf("Duration: %d seconds%n", config.phases[i].duration);
            System.out.printf("Rate: %.2f TPS/terminal%n", config.phases[i].rate);
            System.out.println("Weights: " + Arrays.toString(config.phases[i].weights));
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
//        int duration;
//        double rate;
//        int[] weights;
        Phase[] phases;
    }

    private static class Phase {
        int terminals;
        int duration;
        double rate;
        int[] weights;
    }
}