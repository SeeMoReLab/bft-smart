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
import java.util.concurrent.atomic.AtomicReference;

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

                client.executeWorkload();

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

    private void executeWorkload() {
        PhaseWindow[] windows = buildPhaseSchedule();
        int maxTerminals = Arrays.stream(windows).mapToInt(w -> w.terminals).max().orElse(config.terminals);
        ExecutorService executor = Executors.newFixedThreadPool(maxTerminals);
        CountDownLatch startLatch = new CountDownLatch(1);
        Phaser phaseBarrier = new Phaser(maxTerminals);

        MetricsSnapshot phaseBaseline = captureSnapshot();
        MetricsSnapshot monitorBaseline = phaseBaseline;
        ScheduledExecutorService monitorExec = startMonitor(monitorBaseline);

        for (int i = 0; i < maxTerminals; i++) {
            final int terminalId = i;
            executor.submit(() -> runWorker(terminalId, windows, startLatch, phaseBarrier));
        }

        long workloadStart = System.nanoTime();
        startLatch.countDown();

        for (int i = 0; i < windows.length; i++) {
            waitForPhaseCompletion(phaseBarrier, i);
            MetricsSnapshot after = captureSnapshot();
            MetricsSnapshot delta = computeDelta(phaseBaseline, after);
            double durationSeconds = windows[i].durationSeconds();
            printResults("Phase " + (i + 1), durationSeconds, delta);
            phaseBaseline = after;
        }

        executor.shutdownNow();
        if (monitorExec != null) {
            monitorExec.shutdownNow();
        }

        long workloadEnd = System.nanoTime();
        MetricsSnapshot totalSnapshot = captureSnapshot();
        MetricsSnapshot overallDelta = computeDelta(new MetricsSnapshot(), totalSnapshot);
        printResults("Total", (workloadEnd - workloadStart) / 1_000_000_000.0, overallDelta);
    }

    private void runWorker(int terminalId, PhaseWindow[] windows, CountDownLatch startLatch, Phaser phaseBarrier) {
        try {
            startLatch.await();
            for (int phaseIdx = 0; phaseIdx < windows.length; phaseIdx++) {
                PhaseWindow window = windows[phaseIdx];
                boolean active = terminalId < window.terminals;
                // Wait for phase start
                while (System.nanoTime() < window.startNs) {
                    Thread.sleep(1);
                }
                if (active) {
                    runPhase(terminalId, phaseIdx, window);
                } else {
                    // Inactive terminals stay idle for this phase
                    while (System.nanoTime() < window.endNs) {
                        long remainingNs = window.endNs - System.nanoTime();
                        if (remainingNs <= 0) {
                            break;
                        }
                        long sleepMs = Math.min(TimeUnit.NANOSECONDS.toMillis(remainingNs), 100);
                        Thread.sleep(Math.max(sleepMs, 1));
                    }
                }
                phaseBarrier.arriveAndAwaitAdvance();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Terminal {} aborted", terminalId, e);
        }
    }

    private void runPhase(int terminalId, int phaseNum, PhaseWindow window) {
        long seed = computeTerminalSeed(terminalId, phaseNum);
        Random terminalRandom = new Random(seed);
        RandomDistribution.Flat accountRng = new RandomDistribution.Flat(new Random(seed ^ 0x9E3779B97F4A7C15L),
                0, config.numAccounts);

        long intervalNs = (long) (1_000_000_000.0 / window.rate);
        long nextTransactionTime = window.startNs;
        int txCount = 0;

        while (true) {
            long now = System.nanoTime();
            if (now >= window.endNs) {
                break;
            }

            long waitTime = nextTransactionTime - now;
            if (waitTime > 0) {
                try {
                    Thread.sleep(waitTime / 1_000_000, (int) (waitTime % 1_000_000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            SmallBankMessage.TransactionType txType = selectTransactionType(terminalRandom,
                    config.phases[phaseNum].weights);
            long txStart = System.nanoTime();
            boolean success = executeTransaction(txType, terminalRandom, accountRng);
            long txEnd = System.nanoTime();

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
            // catch-up if we fall behind to avoid burst
            while (nextTransactionTime < System.nanoTime()) {
                nextTransactionTime += intervalNs;
            }

            if (txCount % 1000 == 0 && terminalId == 0) {
                logger.debug("Terminal {} executed {} transactions in phase {}", terminalId, txCount, phaseNum + 1);
            }
        }
        logger.info("Terminal {} completed {} transactions in phase {}", terminalId, txCount, phaseNum + 1);
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

    private void printResults(String label, double durationSeconds, MetricsSnapshot delta) {
        int totalTxns = (int) (delta.success + delta.errors);
        double throughput = durationSeconds == 0 ? 0 : totalTxns / durationSeconds;
        double avgLatency = delta.success == 0 ? 0 : delta.totalLatencyNs / 1_000_000.0 / delta.success;

        long p50 = getPercentileFromHistogram(delta.latencyHistogram, delta.success, 0.50);
        long p95 = getPercentileFromHistogram(delta.latencyHistogram, delta.success, 0.95);
        long p99 = getPercentileFromHistogram(delta.latencyHistogram, delta.success, 0.99);

        if (label.startsWith("Monitor")) {
            measurementLogger.info(
                    "{} duration={}s trxs={} succ={} err={} tps={} avg_ms={} p50={} p95={} p99={}",
                    label, durationSeconds, totalTxns, delta.success, delta.errors, throughput, avgLatency, p50, p95,
                    p99);
            return;
        }

        measurementLogger.info(SINGLE_LINE);
        measurementLogger.info("{} results:", label);
        measurementLogger.info("duration: {} seconds, total trxs: {}, successful: {}, errors: {}", durationSeconds,
                totalTxns, delta.success, delta.errors);
        measurementLogger.info("throughput: {} TPS, avg_latency: {} ms, p50: {} ms, p95: {} ms, p99: {} ms",
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

    private PhaseWindow[] buildPhaseSchedule() {
        PhaseWindow[] windows = new PhaseWindow[config.phases.length];
        long startNs = System.nanoTime();
        for (int i = 0; i < config.phases.length; i++) {
            Phase phase = config.phases[i];
            long durationNs = TimeUnit.SECONDS.toNanos(phase.duration);
            long endNs = startNs + durationNs;
            int terminals = (phase.terminals == -1) ? config.terminals : phase.terminals;
            windows[i] = new PhaseWindow(startNs, endNs, phase.rate, terminals);
            startNs = endNs;
        }
        return windows;
    }

    private void waitForPhaseCompletion(Phaser barrier, int phaseIndex) {
        try {
            barrier.awaitAdvanceInterruptibly(phaseIndex);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private MetricsSnapshot captureSnapshot() {
        return new MetricsSnapshot(successCount.get(), errorCount.get(), totalLatency.get(),
                copyHistogram(latencyHistogram));
    }

    private MetricsSnapshot computeDelta(MetricsSnapshot before, MetricsSnapshot after) {
        long successDelta = after.success - before.success;
        long errorDelta = after.errors - before.errors;
        long latencyDelta = after.totalLatencyNs - before.totalLatencyNs;
        Histogram<Long> deltaHistogram = diffHistogram(before.latencyHistogram, after.latencyHistogram);
        return new MetricsSnapshot(successDelta, errorDelta, latencyDelta, deltaHistogram);
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

    private ScheduledExecutorService startMonitor(MetricsSnapshot initialBaseline) {
        if (config.monitorIntervalSec <= 0) {
            return null;
        }
        ScheduledExecutorService monitorExec = Executors.newSingleThreadScheduledExecutor();
        AtomicReference<MetricsSnapshot> baselineRef = new AtomicReference<>(initialBaseline);
        AtomicLong lastSampleNs = new AtomicLong(System.nanoTime());
        monitorExec.scheduleAtFixedRate(() -> {
            MetricsSnapshot current = captureSnapshot();
            MetricsSnapshot delta = computeDelta(baselineRef.get(), current);
            long now = System.nanoTime();
            double durationSeconds = (now - lastSampleNs.get()) / 1_000_000_000.0;
            printResults("Monitor", durationSeconds, delta);
            baselineRef.set(current);
            lastSampleNs.set(now);
        }, config.monitorIntervalSec, config.monitorIntervalSec, TimeUnit.SECONDS);
        return monitorExec;
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
        config.monitorIntervalSec = xml.getInt("monitorInterval", 0);
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
        System.out.printf("Monitor interval: %d seconds%n", config.monitorIntervalSec);
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
        int monitorIntervalSec;
        Phase[] phases;
    }

    private static class Phase {
        int terminals;
        int duration;
        double rate;
        int[] weights;
    }

    private static class PhaseWindow {
        final long startNs;
        final long endNs;
        final double rate;
        final int terminals;

        PhaseWindow(long startNs, long endNs, double rate, int terminals) {
            this.startNs = startNs;
            this.endNs = endNs;
            this.rate = rate;
            this.terminals = terminals;
        }

        double durationSeconds() {
            return (endNs - startNs) / 1_000_000_000.0;
        }
    }

    private static class MetricsSnapshot {
        final long success;
        final long errors;
        final long totalLatencyNs;
        final Histogram<Long> latencyHistogram;

        MetricsSnapshot() {
            this(0, 0, 0, new Histogram<>());
        }

        MetricsSnapshot(long success, long errors, long totalLatencyNs, Histogram<Long> latencyHistogram) {
            this.success = success;
            this.errors = errors;
            this.totalLatencyNs = totalLatencyNs;
            this.latencyHistogram = latencyHistogram;
        }
    }
}
