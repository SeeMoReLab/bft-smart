package bftsmart.tom.core;

import bftsmart.demo.smallbank2pc.SmallBankMessage2PC;
import bftsmart.rlrpc.PbftReport;
import bftsmart.rlrpc.TwoPcNeighborReport;
import bftsmart.rlrpc.TwoPcOverPbftReport;
import bftsmart.tom.ServiceProxy;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.util.Storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Handles cross-shard communication for multi-shard BFT-SMaRt deployments.
 * Manages ServiceProxy connections to other shards.
 */
public class ShardHandler {
    private static final Logger logger = LoggerFactory.getLogger(ShardHandler.class);
    private static final double NS_TO_MS = 1_000_000.0;

    // Base client ID for cross-shard proxies
    private static final int BASE_CROSS_SHARD_CLIENT_ID = 8000;

    // This replica's identity
    private final int shardId;
    private final int replicaId;
    // private final String configHome;

    // Shard configuration
    private final int totalShards;
    private final Map<Integer, String> shardConfigPaths;

    private final Map<Integer, ServiceProxy> shardProxies;
    private final ReentrantReadWriteLock proxyLock;

    // Connection states
    private final Map<Integer, ConnectionState> connectionStates;

    // Executor for parallel cross-shard requests
    private final ExecutorService executor;

    private int timeoutMs = 5000; // Timeout for cross-shard requests
    private final Map<Integer, Integer> timeoutMsByShard;

    private enum PrepareFailureReason {
        LOCK_BUSY,
        VALIDATION_FAIL,
        TIMEOUT,
        OTHER
    }

    private final Object learningMetricsLock = new Object();
    private final Storage endToEndLatencyNs = new Storage(4096);
    private final Storage prepareLatencyNs = new Storage(4096);
    private final Storage commitLatencyNs = new Storage(4096);
    private final Storage lockWaitNs = new Storage(4096);
    private final Storage lockHoldNs = new Storage(4096);
    private long crossShardTransactions = 0;
    private long prepareOkCount = 0;
    private long prepareFailCount = 0;
    private long commitOkCount = 0;
    private long abortCount = 0;
    private long inDoubtCount = 0;
    private long prepareTimeoutCount = 0;
    private long commitTimeoutUnknownOutcomeCount = 0;
    private long abortBestEffortTimeoutCount = 0;
    private long lockContentionCount = 0;
    private long reasonPrepareLockBusyCount = 0;
    private long reasonPrepareValidationFailCount = 0;
    private long reasonPrepareTimeoutCount = 0;
    private long reasonPrepareOtherCount = 0;
    private long reasonCommitTimeoutUnknownOutcomeCount = 0;
    private long reasonAbortBestEffortTimeoutCount = 0;
    private long reasonOtherCount = 0;
    private final Map<Integer, NeighborLearningMetrics> neighborMetricsByShard;

    private static final class NeighborLearningMetrics {
        final Storage endToEndLatencyNs = new Storage(4096);
        final Storage prepareLatencyNs = new Storage(4096);
        final Storage commitLatencyNs = new Storage(4096);
        final Storage lockWaitNs = new Storage(4096);
        final Storage lockHoldNs = new Storage(4096);
        long crossShardTransactions = 0;
        long prepareOkCount = 0;
        long prepareFailCount = 0;
        long commitOkCount = 0;
        long abortCount = 0;
        long inDoubtCount = 0;
        long prepareTimeoutCount = 0;
        long commitTimeoutUnknownOutcomeCount = 0;
        long abortBestEffortTimeoutCount = 0;
        long lockContentionCount = 0;
        long reasonPrepareLockBusyCount = 0;
        long reasonPrepareValidationFailCount = 0;
        long reasonPrepareTimeoutCount = 0;
        long reasonPrepareOtherCount = 0;
        long reasonCommitTimeoutUnknownOutcomeCount = 0;
        long reasonAbortBestEffortTimeoutCount = 0;
        long reasonOtherCount = 0;

        void reset() {
            endToEndLatencyNs.reset();
            prepareLatencyNs.reset();
            commitLatencyNs.reset();
            lockWaitNs.reset();
            lockHoldNs.reset();
            crossShardTransactions = 0;
            prepareOkCount = 0;
            prepareFailCount = 0;
            commitOkCount = 0;
            abortCount = 0;
            inDoubtCount = 0;
            prepareTimeoutCount = 0;
            commitTimeoutUnknownOutcomeCount = 0;
            abortBestEffortTimeoutCount = 0;
            lockContentionCount = 0;
            reasonPrepareLockBusyCount = 0;
            reasonPrepareValidationFailCount = 0;
            reasonPrepareTimeoutCount = 0;
            reasonPrepareOtherCount = 0;
            reasonCommitTimeoutUnknownOutcomeCount = 0;
            reasonAbortBestEffortTimeoutCount = 0;
            reasonOtherCount = 0;
        }
    }

    public enum ConnectionState {
        NOT_CONNECTED,
        CONNECTING,
        CONNECTED,
        FAILED
    }

    /**
     * Simple constructor.
     *
     * @param shardId This replica's shard ID
     */
    public ShardHandler(int shardId) {
        this(shardId, 0, "config");
    }

    /**
     * Primary constructor: loads shard configuration from shards.config.
     *
     * @param shardId    This replica's shard ID
     * @param replicaId  This replica's ID within the shard
     * @param configHome Path to this replica's config directory
     */
    public ShardHandler(int shardId, int replicaId, String configHome) {
        this.shardId = shardId;
        this.replicaId = replicaId;
        // this.configHome = configHome;
        this.shardProxies = new ConcurrentHashMap<>();
        this.connectionStates = new ConcurrentHashMap<>();
        this.timeoutMsByShard = new ConcurrentHashMap<>();
        this.neighborMetricsByShard = new ConcurrentHashMap<>();
        this.proxyLock = new ReentrantReadWriteLock();
        this.executor = Executors.newCachedThreadPool();

        // Load shard configuration
        ShardConfig config = loadShardConfig(configHome);
        this.totalShards = config.totalShards;
        this.shardConfigPaths = config.shardConfigPaths;
        this.timeoutMs = config.timeoutMs;
        // this.maxRetries = config.maxRetries;
        // this.retryDelayMs = config.retryDelayMs;

        // Initialize connection states for other shards
        for (int i = 0; i < totalShards; i++) {
            if (i != shardId) {
                connectionStates.put(i, ConnectionState.NOT_CONNECTED);
                timeoutMsByShard.put(i, this.timeoutMs);
                neighborMetricsByShard.put(i, new NeighborLearningMetrics());
            }
        }

        logger.info("ShardHandler initialized for shard {} replica {}, total shards: {}",
                shardId, replicaId, totalShards);
    }

    /**
     * Alternate constructor.
     *
     * @param shardId          This replica's shard ID
     * @param replicaId        This replica's ID within the shard
     * @param totalShards      Total number of shards
     * @param shardConfigPaths Map of shardId to config directory path
     */
    public ShardHandler(int shardId, int replicaId, int totalShards,
                        Map<Integer, String> shardConfigPaths) {
        this.shardId = shardId;
        this.replicaId = replicaId;
        // this.configHome = "";
        this.totalShards = totalShards;
        this.shardConfigPaths = new ConcurrentHashMap<>(shardConfigPaths);
        this.shardProxies = new ConcurrentHashMap<>();
        this.connectionStates = new ConcurrentHashMap<>();
        this.timeoutMsByShard = new ConcurrentHashMap<>();
        this.neighborMetricsByShard = new ConcurrentHashMap<>();
        this.proxyLock = new ReentrantReadWriteLock();
        this.executor = Executors.newCachedThreadPool();

        for (int i = 0; i < totalShards; i++) {
            if (i != shardId) {
                connectionStates.put(i, ConnectionState.NOT_CONNECTED);
                timeoutMsByShard.put(i, this.timeoutMs);
                neighborMetricsByShard.put(i, new NeighborLearningMetrics());
            }
        }

        logger.info("ShardHandler initialized programmatically for shard {} replica {}, total shards: {}",
                shardId, replicaId, totalShards);
    }

    private int getShardForAccount(long accountId) {
        return (int) (accountId % totalShards);
    }

    private int[] participantShardPair(int firstShardId, int secondShardId) {
        if (firstShardId == secondShardId) {
            return new int[]{firstShardId};
        }
        return new int[]{firstShardId, secondShardId};
    }

    private int timeoutMsForShard(int targetShardId) {
        int configured = timeoutMsByShard.getOrDefault(targetShardId, timeoutMs);
        return Math.max(1, configured);
    }


    /**
     * Send an ordered request to a specific shard and wait for response.
     * Thread-safe: multiple threads can call this concurrently.
     *
     * @param targetShardId The shard to send the request to
     * @param request       The serialized request payload
     * @return The response from the target shard, or null on timeout/failure
     * @throws IllegalArgumentException if targetShardId is invalid
     */
    public byte[] invokeOrderedOnShard(int targetShardId, byte[] request) {
        validateTargetShard(targetShardId);
        ServiceProxy proxy = getOrCreateProxy(targetShardId);

        if (proxy == null) {
            logger.error("Failed to get proxy for shard {}", targetShardId);
            return null;
        }

        try {
            return proxy.invokeOrdered(request);
        } catch (Exception e) {
            logger.error("Error invoking ordered request on shard {}", targetShardId, e);
            handleProxyFailure(targetShardId, e);
            return null;
        }
    }

    public void handleIncomingCrossShardRequest(TOMMessage msg, TOMLayer tomLayer) {
        // Run asynchronously to avoid blocking TOMLayer's message processing thread
        executor.submit(() -> {
            try {
                doHandleCrossShardRequest(msg, tomLayer);
            } catch (Exception e) {
                logger.error("Error handling cross-shard request from client {}", msg.getSender(), e);
                sendReplyToClient(msg, tomLayer,
                        SmallBankMessage2PC.newErrorMessage("Internal error: " + e.getMessage()));
            }
        });
    }

    private void doHandleCrossShardRequest(TOMMessage msg, TOMLayer tomLayer) {
        logger.debug("Handling incoming cross-shard request from client {} with sequence number {} for session {}",
                msg.getSender(), msg.getSequence(), msg.getSession());
        SmallBankMessage2PC request = SmallBankMessage2PC.getObject(msg.getContent());
        if (request == null) {
            logger.error("Failed to deserialize incoming cross-shard request");
            sendReplyToClient(msg, tomLayer,
                    SmallBankMessage2PC.newErrorMessage("Failed to deserialize request"));
            return;
        }

        switch (request.getTxType()) {
            case SEND_PAYMENT:
                handleCrossShardSendPayment(request, msg, tomLayer);
                break;
            case AMALGAMATE:
                handleCrossShardAmalgamate(request, msg, tomLayer);
                break;
            default:
                logger.error("Unsupported cross-shard transaction type: {}", request.getTxType());
                sendReplyToClient(msg, tomLayer,
                        SmallBankMessage2PC.newErrorMessage("Unsupported cross-shard type: " + request.getTxType()));
                break;
        }
    }

    private void handleCrossShardSendPayment(SmallBankMessage2PC request, TOMMessage msg, TOMLayer tomLayer) {
        String txId = String.format("2pc-%d-%d-%d", shardId, System.currentTimeMillis(), System.nanoTime());
        long txStartNs = System.nanoTime();

        int senderShard = getShardForAccount(request.getCustomerId());
        int receiverShard = getShardForAccount(request.getDestCustomerId());
        int[] participantShards = participantShardPair(senderShard, receiverShard);
        recordCrossShardTransaction(participantShards);

        logger.debug("Starting 2PC for SEND_PAYMENT txId={}, sender={} (shard {}), receiver={} (shard {})",
                txId, request.getCustomerId(), senderShard, request.getDestCustomerId(), receiverShard);

        ServiceProxy senderProxy = getOrCreateProxy(senderShard);
        ServiceProxy receiverProxy = getOrCreateProxy(receiverShard);
        if (senderProxy == null || receiverProxy == null) {
            logger.error("Failed to get proxies for 2PC: senderProxy={}, receiverProxy={}",
                    senderProxy != null ? "OK" : "null", receiverProxy != null ? "OK" : "null");
            recordAbortWithoutPrepare(txStartNs, participantShards);
            sendReplyToClient(msg, tomLayer,
                    SmallBankMessage2PC.newErrorMessage("Failed to connect to participant shards"));
            return;
        }

        SmallBankMessage2PC senderPrepare = SmallBankMessage2PC.newPrepareRequest(
                txId, shardId, senderShard,
                SmallBankMessage2PC.TransactionType.WRITE_CHECK,
                request.getCustomerId(), request.getAmount());
        SmallBankMessage2PC receiverPrepare = SmallBankMessage2PC.newPrepareRequest(
                txId, shardId, receiverShard,
                SmallBankMessage2PC.TransactionType.DEPOSIT_CHECKING,
                request.getDestCustomerId(), request.getAmount());

        long prepareStartNs = System.nanoTime();
        Future<byte[]> senderFuture = executor.submit(() -> senderProxy.invokeOrdered(senderPrepare.getBytes()));
        Future<byte[]> receiverFuture = executor.submit(() -> receiverProxy.invokeOrdered(receiverPrepare.getBytes()));

        byte[] senderReply;
        byte[] receiverReply;
        try {
            senderReply = senderFuture.get(timeoutMsForShard(senderShard), TimeUnit.MILLISECONDS);
            receiverReply = receiverFuture.get(timeoutMsForShard(receiverShard), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Timeout/error during PREPARE phase for txId={}", txId, e);
            recordPrepareFailure(
                    PrepareFailureReason.TIMEOUT,
                    System.nanoTime() - prepareStartNs,
                    txStartNs,
                    participantShards
            );
            abortBestEffort(txId, senderProxy, senderShard, receiverProxy, receiverShard, participantShards);
            sendReplyToClient(msg, tomLayer,
                    SmallBankMessage2PC.newErrorMessage("2PC prepare timeout"));
            return;
        }

        SmallBankMessage2PC senderResp = SmallBankMessage2PC.getObject(senderReply);
        SmallBankMessage2PC receiverResp = SmallBankMessage2PC.getObject(receiverReply);
        boolean senderOk = senderResp != null && senderResp.getResult() == 0;
        boolean receiverOk = receiverResp != null && receiverResp.getResult() == 0;
        long prepareLatencyNs = System.nanoTime() - prepareStartNs;

        if (!senderOk || !receiverOk) {
            logger.debug("PREPARE failed for txId={}: sender={}, receiver={}", txId,
                    senderOk ? "OK" : (senderResp != null ? senderResp.getErrorMsg() : "null"),
                    receiverOk ? "OK" : (receiverResp != null ? receiverResp.getErrorMsg() : "null"));
            recordPrepareFailure(
                    classifyPrepareFailure(senderResp, receiverResp),
                    prepareLatencyNs,
                    txStartNs,
                    participantShards
            );
            abortBestEffort(txId, senderProxy, senderShard, receiverProxy, receiverShard, participantShards);
            sendReplyToClient(msg, tomLayer,
                    SmallBankMessage2PC.newErrorMessage("2PC prepare failed"));
            return;
        }
        recordPrepareSuccess(prepareLatencyNs, participantShards);

        logger.debug("All PREPAREs successful for txId={}, sending COMMITs", txId);
        SmallBankMessage2PC senderCommit = SmallBankMessage2PC.newCommitWithDetails(
                txId, shardId,
                SmallBankMessage2PC.TransactionType.WRITE_CHECK,
                request.getCustomerId(), request.getAmount());
        SmallBankMessage2PC receiverCommit = SmallBankMessage2PC.newCommitWithDetails(
                txId, shardId,
                SmallBankMessage2PC.TransactionType.DEPOSIT_CHECKING,
                request.getDestCustomerId(), request.getAmount());

        long commitStartNs = System.nanoTime();
        Future<byte[]> senderCommitFuture = executor.submit(() -> senderProxy.invokeOrdered(senderCommit.getBytes()));
        Future<byte[]> receiverCommitFuture = executor.submit(() -> receiverProxy.invokeOrdered(receiverCommit.getBytes()));

        try {
            senderCommitFuture.get(timeoutMsForShard(senderShard), TimeUnit.MILLISECONDS);
            receiverCommitFuture.get(timeoutMsForShard(receiverShard), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Timeout/error during COMMIT phase for txId={}", txId, e);
            recordCommitTimeoutUnknownOutcome(System.nanoTime() - commitStartNs, txStartNs, participantShards);
            sendReplyToClient(msg, tomLayer,
                    SmallBankMessage2PC.newErrorMessage("2PC commit timeout"));
            return;
        }

        logger.debug("2PC COMMIT successful for txId={}", txId);
        recordCommitSuccess(System.nanoTime() - commitStartNs, txStartNs, participantShards);
        sendReplyToClient(msg, tomLayer, SmallBankMessage2PC.newResponse(0));
    }

    private void handleCrossShardAmalgamate(SmallBankMessage2PC request, TOMMessage msg, TOMLayer tomLayer) {
        String txId = String.format("2pc-%d-%d-%d", shardId, System.currentTimeMillis(), System.nanoTime());
        long txStartNs = System.nanoTime();

        int destShard = getShardForAccount(request.getCustomerId());
        int sourceShard = getShardForAccount(request.getDestCustomerId());
        int[] participantShards = participantShardPair(destShard, sourceShard);
        recordCrossShardTransaction(participantShards);

        logger.debug("Starting 2PC for AMALGAMATE txId={}, custId1={} (shard {}), custId2={} (shard {})",
                txId, request.getCustomerId(), destShard, request.getDestCustomerId(), sourceShard);

        ServiceProxy destProxy = getOrCreateProxy(destShard);
        ServiceProxy sourceProxy = getOrCreateProxy(sourceShard);
        if (destProxy == null || sourceProxy == null) {
            logger.error("Failed to get proxies for 2PC: destProxy={}, sourceProxy={}",
                    destProxy != null ? "OK" : "null", sourceProxy != null ? "OK" : "null");
            recordAbortWithoutPrepare(txStartNs, participantShards);
            sendReplyToClient(msg, tomLayer,
                    SmallBankMessage2PC.newErrorMessage("Failed to connect to participant shards"));
            return;
        }

        SmallBankMessage2PC destPrepare = SmallBankMessage2PC.newPrepareRequest(
                txId, shardId, destShard,
                SmallBankMessage2PC.TransactionType.TRANSACT_SAVINGS,
                request.getCustomerId(), request.getAmount());
        SmallBankMessage2PC sourcePrepare = SmallBankMessage2PC.newPrepareRequest(
                txId, shardId, sourceShard,
                SmallBankMessage2PC.TransactionType.WRITE_CHECK,
                request.getDestCustomerId(), request.getAmount());

        long prepareStartNs = System.nanoTime();
        Future<byte[]> destFuture = executor.submit(() -> destProxy.invokeOrdered(destPrepare.getBytes()));
        Future<byte[]> sourceFuture = executor.submit(() -> sourceProxy.invokeOrdered(sourcePrepare.getBytes()));

        byte[] destReply;
        byte[] sourceReply;
        try {
            destReply = destFuture.get(timeoutMsForShard(destShard), TimeUnit.MILLISECONDS);
            sourceReply = sourceFuture.get(timeoutMsForShard(sourceShard), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Timeout/error during PREPARE phase for txId={}", txId, e);
            recordPrepareFailure(
                    PrepareFailureReason.TIMEOUT,
                    System.nanoTime() - prepareStartNs,
                    txStartNs,
                    participantShards
            );
            abortBestEffort(txId, destProxy, destShard, sourceProxy, sourceShard, participantShards);
            sendReplyToClient(msg, tomLayer,
                    SmallBankMessage2PC.newErrorMessage("2PC prepare timeout"));
            return;
        }

        SmallBankMessage2PC destResp = SmallBankMessage2PC.getObject(destReply);
        SmallBankMessage2PC sourceResp = SmallBankMessage2PC.getObject(sourceReply);
        boolean destOk = destResp != null && destResp.getResult() == 0;
        boolean sourceOk = sourceResp != null && sourceResp.getResult() == 0;
        long prepareLatencyNs = System.nanoTime() - prepareStartNs;

        if (!destOk || !sourceOk) {
            logger.debug("PREPARE failed for txId={}: dest={}, source={}", txId,
                    destOk ? "OK" : (destResp != null ? destResp.getErrorMsg() : "null"),
                    sourceOk ? "OK" : (sourceResp != null ? sourceResp.getErrorMsg() : "null"));
            recordPrepareFailure(
                    classifyPrepareFailure(destResp, sourceResp),
                    prepareLatencyNs,
                    txStartNs,
                    participantShards
            );
            abortBestEffort(txId, destProxy, destShard, sourceProxy, sourceShard, participantShards);
            sendReplyToClient(msg, tomLayer,
                    SmallBankMessage2PC.newErrorMessage("2PC prepare failed"));
            return;
        }
        recordPrepareSuccess(prepareLatencyNs, participantShards);

        logger.debug("All PREPAREs successful for txId={}, sending COMMITs", txId);
        SmallBankMessage2PC destCommit = SmallBankMessage2PC.newCommitWithDetails(
                txId, shardId,
                SmallBankMessage2PC.TransactionType.TRANSACT_SAVINGS,
                request.getCustomerId(), request.getAmount());
        SmallBankMessage2PC sourceCommit = SmallBankMessage2PC.newCommitWithDetails(
                txId, shardId,
                SmallBankMessage2PC.TransactionType.WRITE_CHECK,
                request.getDestCustomerId(), request.getAmount());

        long commitStartNs = System.nanoTime();
        Future<byte[]> destCommitFuture = executor.submit(() -> destProxy.invokeOrdered(destCommit.getBytes()));
        Future<byte[]> sourceCommitFuture = executor.submit(() -> sourceProxy.invokeOrdered(sourceCommit.getBytes()));

        try {
            destCommitFuture.get(timeoutMsForShard(destShard), TimeUnit.MILLISECONDS);
            sourceCommitFuture.get(timeoutMsForShard(sourceShard), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Timeout/error during COMMIT phase for txId={}", txId, e);
            recordCommitTimeoutUnknownOutcome(System.nanoTime() - commitStartNs, txStartNs, participantShards);
            sendReplyToClient(msg, tomLayer,
                    SmallBankMessage2PC.newErrorMessage("2PC commit timeout"));
            return;
        }

        logger.debug("2PC COMMIT successful for txId={}", txId);
        recordCommitSuccess(System.nanoTime() - commitStartNs, txStartNs, participantShards);
        sendReplyToClient(msg, tomLayer, SmallBankMessage2PC.newResponse(0));
    }

    private void abortBestEffort(
            String txId,
            ServiceProxy senderProxy,
            int senderShardId,
            ServiceProxy receiverProxy,
            int receiverShardId,
            int[] participantShardIds
    ) {
        SmallBankMessage2PC abortMsg = SmallBankMessage2PC.newAbort(txId, shardId);
        Future<?> senderAbort = executor.submit(() -> {
            try {
                senderProxy.invokeOrdered(abortMsg.getBytes());
            } catch (Exception e) {
                logger.warn("Error sending ABORT to sender shard for txId={}: {}", txId, e.getMessage());
            }
        });
        Future<?> receiverAbort = executor.submit(() -> {
            try {
                receiverProxy.invokeOrdered(abortMsg.getBytes());
            } catch (Exception e) {
                logger.warn("Error sending ABORT to receiver shard for txId={}: {}", txId, e.getMessage());
            }
        });
        try {
            senderAbort.get(timeoutMsForShard(senderShardId), TimeUnit.MILLISECONDS);
            receiverAbort.get(timeoutMsForShard(receiverShardId), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.warn("Timeout waiting for ABORT completion for txId={}: {}", txId, e.getMessage());
            recordAbortBestEffortTimeout(participantShardIds);
        }
    }

    private void sendReplyToClient(TOMMessage originalMsg, TOMLayer tomLayer, SmallBankMessage2PC response) {
        TOMMessage replyMsg = new TOMMessage(
                tomLayer.controller.getStaticConf().getProcessId(),
                originalMsg.getSession(),
                originalMsg.getSequence(),
                originalMsg.getOperationId(),
                response.getBytes(),
                tomLayer.controller.getCurrentViewId(),
                originalMsg.getReqType()
        );
        tomLayer.getCommunication().send(new int[]{originalMsg.getSender()}, replyMsg);
        logger.debug("Sent 2PC reply to client {}: result={}", originalMsg.getSender(), response.getResult());
    }

    public void recordParticipantLockWaitNanos(long durationNs) {
        if (durationNs <= 0) {
            return;
        }
        synchronized (learningMetricsLock) {
            lockWaitNs.store(durationNs);
        }
    }

    public void recordParticipantLockHoldMillis(long durationMs) {
        if (durationMs <= 0) {
            return;
        }
        synchronized (learningMetricsLock) {
            lockHoldNs.store(durationMs * 1_000_000L);
        }
    }

    public void recordParticipantLockContention() {
        synchronized (learningMetricsLock) {
            lockContentionCount = safeAdd(lockContentionCount, 1);
        }
    }

    public TwoPcOverPbftReport buildTwoPcOverPbftReport(PbftReport pbftReport, int singleShardTransactions) {
        if (pbftReport == null) {
            return null;
        }

        synchronized (learningMetricsLock) {
            long safeSingleShard = Math.max(0, singleShardTransactions);
            long safeCrossShard = Math.max(0, crossShardTransactions);
            long totalTransactions = safeAdd(safeSingleShard, safeCrossShard);
            TwoPcOverPbftReport.Builder reportBuilder = TwoPcOverPbftReport.newBuilder()
                    .setPbft(pbftReport)
                    .setTotalTransactions(saturatingInt(totalTransactions))
                    .setSingleShardTransactions(saturatingInt(safeSingleShard))
                    .setCrossShardTransactions(saturatingInt(safeCrossShard));

            List<Integer> participantShardIds = new ArrayList<>(neighborMetricsByShard.keySet());
            Collections.sort(participantShardIds);
            for (int participantShardId : participantShardIds) {
                NeighborLearningMetrics metrics = neighborMetricsByShard.get(participantShardId);
                if (metrics == null) {
                    continue;
                }
                double denominator = metrics.crossShardTransactions > 0 ? metrics.crossShardTransactions : 1.0;
                reportBuilder.addNeighborReports(
                        TwoPcNeighborReport.newBuilder()
                                .setParticipantShardId(participantShardId)
                                .setCrossShardTransactions(saturatingInt(metrics.crossShardTransactions))
                                .setPrepareOkCount(saturatingInt(metrics.prepareOkCount))
                                .setPrepareFailCount(saturatingInt(metrics.prepareFailCount))
                                .setCommitOkCount(saturatingInt(metrics.commitOkCount))
                                .setAbortCount(saturatingInt(metrics.abortCount))
                                .setInDoubtCount(saturatingInt(metrics.inDoubtCount))
                                .setPrepareTimeoutCount(saturatingInt(metrics.prepareTimeoutCount))
                                .setCommitTimeoutUnknownOutcomeCount(
                                        saturatingInt(metrics.commitTimeoutUnknownOutcomeCount))
                                .setAbortBestEffortTimeoutCount(saturatingInt(metrics.abortBestEffortTimeoutCount))
                                .setLockContentionCount(saturatingInt(metrics.lockContentionCount))
                                .setPrepareTimeoutRate((float) (metrics.prepareTimeoutCount / denominator))
                                .setAbortRate((float) (metrics.abortCount / denominator))
                                .setCommitRate((float) (metrics.commitOkCount / denominator))
                                .setLockContentionRate((float) (metrics.lockContentionCount / denominator))
                                .setAvgEndToEndLatencyMs(averageMs(metrics.endToEndLatencyNs))
                                .setP95EndToEndLatencyMs(percentileMs(metrics.endToEndLatencyNs, 0.95))
                                .setP99EndToEndLatencyMs(percentileMs(metrics.endToEndLatencyNs, 0.99))
                                .setAvgPrepareLatencyMs(averageMs(metrics.prepareLatencyNs))
                                .setP95PrepareLatencyMs(percentileMs(metrics.prepareLatencyNs, 0.95))
                                .setAvgCommitLatencyMs(averageMs(metrics.commitLatencyNs))
                                .setP95CommitLatencyMs(percentileMs(metrics.commitLatencyNs, 0.95))
                                .setAvgLockWaitMs(averageMs(metrics.lockWaitNs))
                                .setP95LockWaitMs(percentileMs(metrics.lockWaitNs, 0.95))
                                .setAvgLockHoldMs(averageMs(metrics.lockHoldNs))
                                .setP95LockHoldMs(percentileMs(metrics.lockHoldNs, 0.95))
                                .setReasonPrepareLockBusyCount(saturatingInt(metrics.reasonPrepareLockBusyCount))
                                .setReasonPrepareValidationFailCount(
                                        saturatingInt(metrics.reasonPrepareValidationFailCount))
                                .setReasonPrepareTimeoutCount(saturatingInt(metrics.reasonPrepareTimeoutCount))
                                .setReasonPrepareOtherCount(saturatingInt(metrics.reasonPrepareOtherCount))
                                .setReasonCommitTimeoutUnknownOutcomeCount(
                                        saturatingInt(metrics.reasonCommitTimeoutUnknownOutcomeCount))
                                .setReasonAbortBestEffortTimeoutCount(
                                        saturatingInt(metrics.reasonAbortBestEffortTimeoutCount))
                                .setReasonOtherCount(saturatingInt(metrics.reasonOtherCount))
                                .build()
                );
            }
            return reportBuilder.build();
        }
    }

    public void resetLearningWindowMetrics() {
        synchronized (learningMetricsLock) {
            endToEndLatencyNs.reset();
            prepareLatencyNs.reset();
            commitLatencyNs.reset();
            lockWaitNs.reset();
            lockHoldNs.reset();
            crossShardTransactions = 0;
            prepareOkCount = 0;
            prepareFailCount = 0;
            commitOkCount = 0;
            abortCount = 0;
            inDoubtCount = 0;
            prepareTimeoutCount = 0;
            commitTimeoutUnknownOutcomeCount = 0;
            abortBestEffortTimeoutCount = 0;
            lockContentionCount = 0;
            reasonPrepareLockBusyCount = 0;
            reasonPrepareValidationFailCount = 0;
            reasonPrepareTimeoutCount = 0;
            reasonPrepareOtherCount = 0;
            reasonCommitTimeoutUnknownOutcomeCount = 0;
            reasonAbortBestEffortTimeoutCount = 0;
            reasonOtherCount = 0;
            for (NeighborLearningMetrics metrics : neighborMetricsByShard.values()) {
                if (metrics != null) {
                    metrics.reset();
                }
            }
        }
    }

    private void recordCrossShardTransaction(int[] participantShardIds) {
        synchronized (learningMetricsLock) {
            crossShardTransactions = safeAdd(crossShardTransactions, 1);
            for (int participantShardId : participantShardIds) {
                NeighborLearningMetrics metrics = neighborMetricsByShard.get(participantShardId);
                if (metrics != null) {
                    metrics.crossShardTransactions = safeAdd(metrics.crossShardTransactions, 1);
                }
            }
        }
    }

    private void recordPrepareSuccess(long prepareDurationNs, int[] participantShardIds) {
        synchronized (learningMetricsLock) {
            prepareOkCount = safeAdd(prepareOkCount, 1);
            storeIfPositive(prepareLatencyNs, prepareDurationNs);
            for (int participantShardId : participantShardIds) {
                NeighborLearningMetrics metrics = neighborMetricsByShard.get(participantShardId);
                if (metrics != null) {
                    metrics.prepareOkCount = safeAdd(metrics.prepareOkCount, 1);
                    storeIfPositive(metrics.prepareLatencyNs, prepareDurationNs);
                }
            }
        }
    }

    private void recordPrepareFailure(
            PrepareFailureReason reason,
            long prepareDurationNs,
            long txStartNs,
            int[] participantShardIds
    ) {
        synchronized (learningMetricsLock) {
            prepareFailCount = safeAdd(prepareFailCount, 1);
            abortCount = safeAdd(abortCount, 1);
            storeIfPositive(prepareLatencyNs, prepareDurationNs);
            storeIfPositive(endToEndLatencyNs, System.nanoTime() - txStartNs);
            switch (reason) {
                case LOCK_BUSY:
                    reasonPrepareLockBusyCount = safeAdd(reasonPrepareLockBusyCount, 1);
                    break;
                case VALIDATION_FAIL:
                    reasonPrepareValidationFailCount = safeAdd(reasonPrepareValidationFailCount, 1);
                    break;
                case TIMEOUT:
                    prepareTimeoutCount = safeAdd(prepareTimeoutCount, 1);
                    reasonPrepareTimeoutCount = safeAdd(reasonPrepareTimeoutCount, 1);
                    break;
                default:
                    reasonPrepareOtherCount = safeAdd(reasonPrepareOtherCount, 1);
                    break;
            }
            for (int participantShardId : participantShardIds) {
                NeighborLearningMetrics metrics = neighborMetricsByShard.get(participantShardId);
                if (metrics == null) {
                    continue;
                }
                metrics.prepareFailCount = safeAdd(metrics.prepareFailCount, 1);
                metrics.abortCount = safeAdd(metrics.abortCount, 1);
                storeIfPositive(metrics.prepareLatencyNs, prepareDurationNs);
                storeIfPositive(metrics.endToEndLatencyNs, System.nanoTime() - txStartNs);
                switch (reason) {
                    case LOCK_BUSY:
                        metrics.reasonPrepareLockBusyCount = safeAdd(metrics.reasonPrepareLockBusyCount, 1);
                        break;
                    case VALIDATION_FAIL:
                        metrics.reasonPrepareValidationFailCount =
                                safeAdd(metrics.reasonPrepareValidationFailCount, 1);
                        break;
                    case TIMEOUT:
                        metrics.prepareTimeoutCount = safeAdd(metrics.prepareTimeoutCount, 1);
                        metrics.reasonPrepareTimeoutCount = safeAdd(metrics.reasonPrepareTimeoutCount, 1);
                        break;
                    default:
                        metrics.reasonPrepareOtherCount = safeAdd(metrics.reasonPrepareOtherCount, 1);
                        break;
                }
            }
        }
    }

    private void recordAbortWithoutPrepare(long txStartNs, int[] participantShardIds) {
        synchronized (learningMetricsLock) {
            abortCount = safeAdd(abortCount, 1);
            reasonOtherCount = safeAdd(reasonOtherCount, 1);
            storeIfPositive(endToEndLatencyNs, System.nanoTime() - txStartNs);
            for (int participantShardId : participantShardIds) {
                NeighborLearningMetrics metrics = neighborMetricsByShard.get(participantShardId);
                if (metrics == null) {
                    continue;
                }
                metrics.abortCount = safeAdd(metrics.abortCount, 1);
                metrics.reasonOtherCount = safeAdd(metrics.reasonOtherCount, 1);
                storeIfPositive(metrics.endToEndLatencyNs, System.nanoTime() - txStartNs);
            }
        }
    }

    private void recordCommitSuccess(long commitDurationNs, long txStartNs, int[] participantShardIds) {
        synchronized (learningMetricsLock) {
            commitOkCount = safeAdd(commitOkCount, 1);
            storeIfPositive(commitLatencyNs, commitDurationNs);
            storeIfPositive(endToEndLatencyNs, System.nanoTime() - txStartNs);
            for (int participantShardId : participantShardIds) {
                NeighborLearningMetrics metrics = neighborMetricsByShard.get(participantShardId);
                if (metrics == null) {
                    continue;
                }
                metrics.commitOkCount = safeAdd(metrics.commitOkCount, 1);
                storeIfPositive(metrics.commitLatencyNs, commitDurationNs);
                storeIfPositive(metrics.endToEndLatencyNs, System.nanoTime() - txStartNs);
            }
        }
    }

    private void recordCommitTimeoutUnknownOutcome(
            long commitDurationNs,
            long txStartNs,
            int[] participantShardIds
    ) {
        synchronized (learningMetricsLock) {
            inDoubtCount = safeAdd(inDoubtCount, 1);
            commitTimeoutUnknownOutcomeCount = safeAdd(commitTimeoutUnknownOutcomeCount, 1);
            reasonCommitTimeoutUnknownOutcomeCount = safeAdd(reasonCommitTimeoutUnknownOutcomeCount, 1);
            storeIfPositive(commitLatencyNs, commitDurationNs);
            storeIfPositive(endToEndLatencyNs, System.nanoTime() - txStartNs);
            for (int participantShardId : participantShardIds) {
                NeighborLearningMetrics metrics = neighborMetricsByShard.get(participantShardId);
                if (metrics == null) {
                    continue;
                }
                metrics.inDoubtCount = safeAdd(metrics.inDoubtCount, 1);
                metrics.commitTimeoutUnknownOutcomeCount =
                        safeAdd(metrics.commitTimeoutUnknownOutcomeCount, 1);
                metrics.reasonCommitTimeoutUnknownOutcomeCount =
                        safeAdd(metrics.reasonCommitTimeoutUnknownOutcomeCount, 1);
                storeIfPositive(metrics.commitLatencyNs, commitDurationNs);
                storeIfPositive(metrics.endToEndLatencyNs, System.nanoTime() - txStartNs);
            }
        }
    }

    private void recordAbortBestEffortTimeout(int[] participantShardIds) {
        synchronized (learningMetricsLock) {
            abortBestEffortTimeoutCount = safeAdd(abortBestEffortTimeoutCount, 1);
            reasonAbortBestEffortTimeoutCount = safeAdd(reasonAbortBestEffortTimeoutCount, 1);
            for (int participantShardId : participantShardIds) {
                NeighborLearningMetrics metrics = neighborMetricsByShard.get(participantShardId);
                if (metrics == null) {
                    continue;
                }
                metrics.abortBestEffortTimeoutCount = safeAdd(metrics.abortBestEffortTimeoutCount, 1);
                metrics.reasonAbortBestEffortTimeoutCount =
                        safeAdd(metrics.reasonAbortBestEffortTimeoutCount, 1);
            }
        }
    }

    private static PrepareFailureReason classifyPrepareFailure(SmallBankMessage2PC first, SmallBankMessage2PC second) {
        String firstError = first != null ? first.getErrorMsg() : null;
        String secondError = second != null ? second.getErrorMsg() : null;
        if (containsIgnoreCase(firstError, "lock") || containsIgnoreCase(secondError, "lock")
                || containsIgnoreCase(firstError, "busy") || containsIgnoreCase(secondError, "busy")) {
            return PrepareFailureReason.LOCK_BUSY;
        }
        if (containsIgnoreCase(firstError, "insufficient")
                || containsIgnoreCase(secondError, "insufficient")
                || containsIgnoreCase(firstError, "account")
                || containsIgnoreCase(secondError, "account")) {
            return PrepareFailureReason.VALIDATION_FAIL;
        }
        if (firstError != null || secondError != null) {
            return PrepareFailureReason.OTHER;
        }
        return PrepareFailureReason.OTHER;
    }

    private static boolean containsIgnoreCase(String text, String token) {
        return text != null && text.toLowerCase(Locale.ROOT).contains(token);
    }

    private static long safeAdd(long left, long right) {
        if (Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
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

    private static void storeIfPositive(Storage storage, long valueNs) {
        if (valueNs > 0L) {
            storage.store(valueNs);
        }
    }

    private static float averageMs(Storage storage) {
        if (storage.getCount() == 0) {
            return 0f;
        }
        return (float) (storage.getAverage(false) / NS_TO_MS);
    }

    private static float percentileMs(Storage storage, double percentile) {
        if (storage.getCount() == 0) {
            return 0f;
        }
        return (float) (storage.getPercentile(percentile) / NS_TO_MS);
    }

    // Proxy Management

    /**
     * Get or create a ServiceProxy for the target shard.
     */
    private ServiceProxy getOrCreateProxy(int targetShardId) {
        // check if proxy exists
        ServiceProxy proxy = shardProxies.get(targetShardId);
        if (proxy != null) {
            return proxy;
        }

        // create proxy with proper synchronization
        proxyLock.writeLock().lock();
        try {
            // Double-check after acquiring lock
            proxy = shardProxies.get(targetShardId);
            if (proxy != null) {
                return proxy;
            }

            return createProxyForShard(targetShardId);
        } finally {
            proxyLock.writeLock().unlock();
        }
    }

    /**
     * Create a new ServiceProxy for the target shard.
     * Must be called while holding write lock.
     */
    private ServiceProxy createProxyForShard(int targetShardId) {
        connectionStates.put(targetShardId, ConnectionState.CONNECTING);

        String targetConfigHome = shardConfigPaths.get(targetShardId);
        if (targetConfigHome == null) {
            logger.error("No config path found for shard {}", targetShardId);
            connectionStates.put(targetShardId, ConnectionState.FAILED);
            return null;
        }

        int clientId = generateCrossShardClientId(targetShardId);

        logger.info("Creating ServiceProxy for shard {} with clientId {} and configHome {}",
                targetShardId, clientId, targetConfigHome);

        try {
            ServiceProxy proxy = new ServiceProxy(clientId, targetConfigHome);
            shardProxies.put(targetShardId, proxy);
            connectionStates.put(targetShardId, ConnectionState.CONNECTED);
            logger.info("Successfully created proxy for shard {}", targetShardId);
            return proxy;
        } catch (Exception e) {
            logger.error("Failed to create proxy for shard {}", targetShardId, e);
            connectionStates.put(targetShardId, ConnectionState.FAILED);
            return null;
        }
    }

    /**
     * Generate a unique client ID for cross-shard communication.
     */
    private int generateCrossShardClientId(int targetShardId) {
        return BASE_CROSS_SHARD_CLIENT_ID +
                (shardId * 1000) +
                (replicaId * 100) +
                targetShardId;
    }


    private void handleProxyFailure(int targetShardId, Exception e) {
        proxyLock.writeLock().lock();
        try {
            // Close existing proxy if any
            ServiceProxy oldProxy = shardProxies.remove(targetShardId);
            if (oldProxy != null) {
                try {
                    oldProxy.close();
                } catch (Exception closeEx) {
                    logger.warn("Error closing failed proxy for shard {}", targetShardId, closeEx);
                }
            }

            connectionStates.put(targetShardId, ConnectionState.FAILED);
        } finally {
            proxyLock.writeLock().unlock();
        }
    }

    public boolean reconnectToShard(int targetShardId) {
        ConnectionState state = connectionStates.get(targetShardId);
        if (state == ConnectionState.CONNECTED) {
            return true;
        }

        proxyLock.writeLock().lock();
        try {
            // Remove old proxy if exists
            ServiceProxy oldProxy = shardProxies.remove(targetShardId);
            if (oldProxy != null) {
                try {
                    oldProxy.close();
                } catch (Exception e) {
                    logger.warn("Error closing old proxy for shard {}", targetShardId);
                }
            }

            // Try to create new proxy
            ServiceProxy proxy = createProxyForShard(targetShardId);
            return proxy != null;
        } finally {
            proxyLock.writeLock().unlock();
        }
    }

    // Config
    private ShardConfig loadShardConfig(String configHome) {
        ShardConfig config = new ShardConfig();
        String sep = System.getProperty("file.separator");
        // String path = configHome + sep + "shards.config";
        String path = "config/shards.config";

        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            Properties props = new Properties();
            props.load(reader);

            config.totalShards = Integer.parseInt(
                    props.getProperty("system.shards.count", "1"));

            String basePath = props.getProperty("system.shards.basePath", "");
            config.timeoutMs = Integer.parseInt(
                    props.getProperty("system.shards.timeoutMs", "5000"));
            config.shardConfigPaths = new HashMap<>();
            for (int i = 0; i < config.totalShards; i++) {
                String shardConfigHome = props.getProperty("shard." + i + ".configHome");
                if (shardConfigHome != null) {
                    // Resolve relative paths
                    if (!shardConfigHome.startsWith("/") && !basePath.isEmpty()) {
                        shardConfigHome = basePath + sep + shardConfigHome;
                    }
                    config.shardConfigPaths.put(i, shardConfigHome);
                }
            }

            logger.info("Loaded shard config: {} shards from {}", config.totalShards, path);
            return config;

        } catch (IOException e) {
            logger.warn("Could not load shards.config from {}, using single-shard defaults: {}",
                    path, e.getMessage());
            config.totalShards = 1;
            config.shardConfigPaths = new HashMap<>();
            config.shardConfigPaths.put(0, configHome);
            return config;
        }
    }

    private static class ShardConfig {
        int totalShards = 1;
        Map<Integer, String> shardConfigPaths = new HashMap<>();
        int timeoutMs = 5000;
    }

    public void setTimeoutMs(int timeoutMs) {
        this.timeoutMs = Math.max(1, timeoutMs);
        for (Integer targetShardId : timeoutMsByShard.keySet()) {
            timeoutMsByShard.put(targetShardId, this.timeoutMs);
        }
    }

    public int getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMsByShard(Map<Integer, Integer> newTimeoutsByShard) {
        if (newTimeoutsByShard == null || newTimeoutsByShard.isEmpty()) {
            return;
        }
        for (Map.Entry<Integer, Integer> entry : newTimeoutsByShard.entrySet()) {
            int targetShardId = entry.getKey();
            int targetTimeoutMs = Math.max(1, entry.getValue());
            if (targetShardId < 0 || targetShardId >= totalShards || targetShardId == shardId) {
                continue;
            }
            timeoutMsByShard.put(targetShardId, targetTimeoutMs);
        }
    }

    public Map<Integer, Integer> getTimeoutMsByShardSnapshot() {
        Map<Integer, Integer> snapshot = new HashMap<>();
        for (int targetShardId = 0; targetShardId < totalShards; targetShardId++) {
            if (targetShardId == shardId) {
                continue;
            }
            snapshot.put(targetShardId, timeoutMsForShard(targetShardId));
        }
        return snapshot;
    }

    // Util
    private void validateTargetShard(int targetShardId) {
        if (targetShardId < 0 || targetShardId >= totalShards) {
            throw new IllegalArgumentException(
                    "Invalid target shard: " + targetShardId +
                            ", total shards: " + totalShards);
        }
        // if (targetShardId == shardId) {
        //     throw new IllegalArgumentException(
        //             "Cannot send cross-shard request to own shard: " + targetShardId);
        // }
    }
    
    public int getShardId() {
        return shardId;
    }
    
    public int getTotalShards() {
        return totalShards;
    }

    public List<Integer> getOtherShardIds() {
        List<Integer> others = new ArrayList<>();
        for (int i = 0; i < totalShards; i++) {
            if (i != shardId) {
                others.add(i);
            }
        }
        return others;
    }

    public boolean isShardConnected(int targetShardId) {
        return connectionStates.get(targetShardId) == ConnectionState.CONNECTED;
    }

    public ConnectionState getConnectionState(int targetShardId) {
        return connectionStates.getOrDefault(targetShardId, ConnectionState.NOT_CONNECTED);
    }

    public void shutdown() {
        logger.info("Shutting down ShardHandler for shard {}", shardId);

        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        proxyLock.writeLock().lock();
        try {
            for (Map.Entry<Integer, ServiceProxy> entry : shardProxies.entrySet()) {
                try {
                    entry.getValue().close();
                    logger.info("Closed proxy for shard {}", entry.getKey());
                } catch (Exception e) {
                    logger.warn("Error closing proxy for shard {}", entry.getKey(), e);
                }
            }
            shardProxies.clear();
            connectionStates.replaceAll((k, v) -> ConnectionState.NOT_CONNECTED);
        } finally {
            proxyLock.writeLock().unlock();
        }
    }
}
