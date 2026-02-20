package bftsmart.demo.util;

import bftsmart.rlrpc.PbftReport;
import bftsmart.tom.MessageContext;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.util.Storage;

/**
 * Aggregates per-consensus features for one learning window.
 */
public class TimeoutLearningWindowMetrics {
    private static final double NS_TO_MS = 1_000_000.0;
    private static final double NS_TO_SECONDS = 1_000_000_000.0;

    private final Storage consensusLatencyNs;
    private final Storage batchSize;
    private final Storage proposeDelayNs;
    private final Storage writeDelayNs;
    private final Storage acceptDelayNs;
    private final Storage postDecisionDelayNs;

    private long totalTransactions;
    private int totalConsensus;
    private int timeoutViolationCount;
    private int leaderChangeCount;
    private int regencyChangeCount;
    private Integer previousLeader;
    private Integer previousRegency;
    private long firstDecisionTimeNs = -1L;
    private long lastDecisionTimeNs = -1L;

    public TimeoutLearningWindowMetrics(int capacity) {
        this.consensusLatencyNs = new Storage(capacity);
        this.batchSize = new Storage(capacity);
        this.proposeDelayNs = new Storage(capacity);
        this.writeDelayNs = new Storage(capacity);
        this.acceptDelayNs = new Storage(capacity);
        this.postDecisionDelayNs = new Storage(capacity);
    }

    public void reset() {
        consensusLatencyNs.reset();
        batchSize.reset();
        proposeDelayNs.reset();
        writeDelayNs.reset();
        acceptDelayNs.reset();
        postDecisionDelayNs.reset();
        totalTransactions = 0L;
        totalConsensus = 0;
        timeoutViolationCount = 0;
        leaderChangeCount = 0;
        regencyChangeCount = 0;
        firstDecisionTimeNs = -1L;
        lastDecisionTimeNs = -1L;
    }

    public void recordConsensus(MessageContext context, int consensusBatchSize, int timeoutMs) {
        if (context == null) {
            return;
        }

        totalConsensus++;

        int safeBatchSize = Math.max(consensusBatchSize, 0);
        totalTransactions += safeBatchSize;
        batchSize.store(safeBatchSize);

        updateLeaderRegency(context);

        TOMMessage firstInBatch = context.getFirstInBatch();
        if (firstInBatch == null) {
            return;
        }

        long consensusLatency = durationNs(firstInBatch.consensusStartTime, firstInBatch.decisionTime);
        if (consensusLatency >= 0L) {
            consensusLatencyNs.store(consensusLatency);
            if (timeoutMs > 0 && toMs(consensusLatency) > timeoutMs) {
                timeoutViolationCount++;
            }
        }

        if (firstInBatch.decisionTime > 0L) {
            if (firstDecisionTimeNs < 0L) {
                firstDecisionTimeNs = firstInBatch.decisionTime;
            }
            lastDecisionTimeNs = firstInBatch.decisionTime;
        }

        storeIfValid(proposeDelayNs, durationNs(firstInBatch.consensusStartTime, firstInBatch.writeSentTime));
        storeIfValid(writeDelayNs, durationNs(firstInBatch.writeSentTime, firstInBatch.acceptSentTime));
        storeIfValid(acceptDelayNs, durationNs(firstInBatch.acceptSentTime, firstInBatch.decisionTime));

        long postDecision = durationNs(firstInBatch.decisionTime, firstInBatch.executedTime);
        if (postDecision < 0L) {
            postDecision = durationNs(firstInBatch.decisionTime, firstInBatch.deliveryTime);
        }
        storeIfValid(postDecisionDelayNs, postDecision);
    }

    public PbftReport buildReport() {
        int latencySamples = consensusLatencyNs.getCount();
        if (latencySamples == 0) {
            return null;
        }

        PbftReport.Builder builder = PbftReport.newBuilder()
                .setTotalTransactions(saturatingInt(totalTransactions))
                .setTotalConsensusInstances(totalConsensus)
                .setAvgConsensusLatencyMs(toMs(consensusLatencyNs.getAverage(false)))
                .setP95ConsensusLatencyMs(toMs(consensusLatencyNs.getPercentile(0.95)))
                .setP99ConsensusLatencyMs(toMs(consensusLatencyNs.getPercentile(0.99)))
                .setThroughputTps(computeThroughputTps())
                .setTimeoutViolationRate(totalConsensus > 0 ? (float) timeoutViolationCount / totalConsensus : 0f)
                .setAvgBatchSize(totalConsensus > 0 ? (float) totalTransactions / totalConsensus : 0f)
                .setP95BatchSize(batchSize.getCount() > 0 ? batchSize.getPercentile(0.95) : 0f)
                .setLeaderChangeCount(leaderChangeCount)
                .setRegencyChangeCount(regencyChangeCount)
                .setPhaseProposeAvgDelayMs(averageMs(proposeDelayNs))
                .setPhaseProposeP95DelayMs(percentileMs(proposeDelayNs, 0.95))
                .setPhaseWriteAvgDelayMs(averageMs(writeDelayNs))
                .setPhaseWriteP95DelayMs(percentileMs(writeDelayNs, 0.95))
                .setPhaseAcceptAvgDelayMs(averageMs(acceptDelayNs))
                .setPhaseAcceptP95DelayMs(percentileMs(acceptDelayNs, 0.95))
                .setPhasePostDecisionAvgDelayMs(averageMs(postDecisionDelayNs))
                .setPhasePostDecisionP95DelayMs(percentileMs(postDecisionDelayNs, 0.95));

        return builder.build();
    }

    private void updateLeaderRegency(MessageContext context) {
        int leader = context.getLeader();
        if (leader >= 0) {
            if (previousLeader != null && previousLeader != leader) {
                leaderChangeCount++;
            }
            previousLeader = leader;
        }

        int regency = context.getRegency();
        if (regency >= 0) {
            if (previousRegency != null && previousRegency != regency) {
                regencyChangeCount++;
            }
            previousRegency = regency;
        }
    }

    private static void storeIfValid(Storage storage, long valueNs) {
        if (valueNs >= 0L) {
            storage.store(valueNs);
        }
    }

    private static long durationNs(long startNs, long endNs) {
        if (startNs <= 0L || endNs <= 0L || endNs < startNs) {
            return -1L;
        }
        return endNs - startNs;
    }

    private float computeThroughputTps() {
        long durationNs = lastDecisionTimeNs - firstDecisionTimeNs;
        if (durationNs <= 0L) {
            return 0f;
        }
        return (float) (totalTransactions / (durationNs / NS_TO_SECONDS));
    }

    private static int saturatingInt(long value) {
        if (value <= 0L) {
            return 0;
        }
        if (value >= Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) value;
    }

    private static float averageMs(Storage storage) {
        if (storage.getCount() == 0) {
            return 0f;
        }
        return toMs(storage.getAverage(false));
    }

    private static float percentileMs(Storage storage, double percentile) {
        if (storage.getCount() == 0) {
            return 0f;
        }
        return toMs(storage.getPercentile(percentile));
    }

    private static float toMs(double nanoseconds) {
        return (float) (nanoseconds / NS_TO_MS);
    }
}
