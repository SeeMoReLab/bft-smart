package bftsmart.demo.smallbank2pc;

import bftsmart.tom.core.ShardHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Two-Phase Commit Coordinator for cross-shard transactions.
 *
 * This coordinator orchestrates distributed transactions that span multiple shards
 * using 2PC
 */
public class TwoPhaseCoordinator {
    private static final Logger logger = LoggerFactory.getLogger(TwoPhaseCoordinator.class);

    private final ShardHandler shardHandler;
    private final int myShardId;
    private final int totalShards;

    // Configuration
    private final long prepareTimeoutMs;
    private final long commitTimeoutMs;

    // Executor for parallel operations
    private final ExecutorService executor;

    // Transaction ID generator
    private long txCounter = 0;

    /**
     * Result of a 2PC transaction.
     */
    public static class TransactionResult {
        public final boolean success;
        public final String transactionId;
        public final String errorMessage;
        public final Map<Integer, SmallBankMessage2PC> shardResponses;

        private TransactionResult(boolean success, String txId, String error,
                                  Map<Integer, SmallBankMessage2PC> responses) {
            this.success = success;
            this.transactionId = txId;
            this.errorMessage = error;
            this.shardResponses = responses;
        }

        public static TransactionResult success(String txId, Map<Integer, SmallBankMessage2PC> responses) {
            return new TransactionResult(true, txId, null, responses);
        }

        public static TransactionResult failure(String txId, String error,
                                                Map<Integer, SmallBankMessage2PC> responses) {
            return new TransactionResult(false, txId, error, responses);
        }
    }

    /**
     * Represents the parts of a cross-shard transaction for each participant.
     */
    public static class CrossShardTransaction {
        public final String transactionId;
        public final Map<Integer, SmallBankMessage2PC> shardRequests; // shardId -> request for that shard

        public CrossShardTransaction(String txId) {
            this.transactionId = txId;
            this.shardRequests = new HashMap<>();
        }

        public void addShardRequest(int shardId, SmallBankMessage2PC request) {
            shardRequests.put(shardId, request);
        }

        public Set<Integer> getParticipantShards() {
            return shardRequests.keySet();
        }
    }

    /**
     * Create a coordinator with default timeouts.
     */
    public TwoPhaseCoordinator(ShardHandler shardHandler) {
        this(shardHandler, 30000, 30000); // 30 second timeouts
    }

    /**
     * Create a coordinator with custom timeouts.
     */
    public TwoPhaseCoordinator(ShardHandler shardHandler, long prepareTimeoutMs, long commitTimeoutMs) {
        this.shardHandler = shardHandler;
        this.myShardId = shardHandler.getShardId();
        this.totalShards = shardHandler.getTotalShards();
        this.prepareTimeoutMs = prepareTimeoutMs;
        this.commitTimeoutMs = commitTimeoutMs;
        this.executor = Executors.newCachedThreadPool();

        logger.info("TwoPhaseCoordinator initialized for shard {}, total shards: {}", myShardId, totalShards);
    }

    // Partitioning Logic

    /**
     * Determine which shard owns an account based on account ID.
     */
    public int getShardForAccount(long accountId) {
        return (int) (accountId % totalShards);
    }

    /**
     * Check if a transaction requires cross-shard coordination.
     */
    public boolean isCrossShardTransaction(SmallBankMessage2PC request) {
        int primaryShard = getShardForAccount(request.getCustomerId());

        switch (request.getTxType()) {
            case SEND_PAYMENT:
            case AMALGAMATE:
                // These involve two accounts
                int destShard = getShardForAccount(request.getDestCustomerId());
                return primaryShard != destShard;

            default:
                // Single account operations are never cross-shard
                return false;
        }
    }

    /**
     * Get all shards involved in a transaction.
     */
    public Set<Integer> getInvolvedShards(SmallBankMessage2PC request) {
        Set<Integer> shards = new HashSet<>();
        shards.add(getShardForAccount(request.getCustomerId()));

        if (request.getDestCustomerId() > 0) {
            shards.add(getShardForAccount(request.getDestCustomerId()));
        }

        return shards;
    }

    // Transaction ID Generation
    public synchronized String generateTransactionId() {
        return String.format("tx-%d-%d-%d", myShardId, System.currentTimeMillis(), txCounter++);
    }

    // Main 2PC

    /**
     * Execute a cross-shard transaction using 2PC.
     *
     * This is the main entry point for cross-shard transactions. It:
     * 1. Splits the transaction into per-shard operations
     * 2. Sends PREPARE to all shards in parallel
     * 3. Collects votes
     * 4. Sends COMMIT or ABORT based on votes
     */
    public TransactionResult executeCrossShardTransaction(SmallBankMessage2PC request) {
        String txId = generateTransactionId();
        logger.info("Starting 2PC transaction {} for request: {}", txId, request.getTxType());

        // Build cross-shard transaction
        CrossShardTransaction crossTx = buildCrossShardTransaction(txId, request);

        if (crossTx.shardRequests.isEmpty()) {
            logger.error("No shard requests generated for transaction {}", txId);
            return TransactionResult.failure(txId, "Failed to build cross-shard transaction", null);
        }

        logger.info("Transaction {} involves shards: {}", txId, crossTx.getParticipantShards());

        // Phase 1: PREPARE
        Map<Integer, SmallBankMessage2PC> prepareResponses = executePreparePhase(crossTx);

        // Check if all participants voted YES
        boolean allPrepared = checkAllPrepared(prepareResponses, crossTx.getParticipantShards());

        // Phase 2: COMMIT or ABORT
        Map<Integer, SmallBankMessage2PC> phase2Responses;
        if (allPrepared) {
            logger.info("All shards voted YES for transaction {}, committing", txId);
            phase2Responses = executeCommitPhase(crossTx);
            return TransactionResult.success(txId, phase2Responses);
        } else {
            String failReason = getFailureReason(prepareResponses);
            logger.warn("Transaction {} failed prepare phase: {}", txId, failReason);
            phase2Responses = executeAbortPhase(crossTx);
            return TransactionResult.failure(txId, failReason, phase2Responses);
        }
    }

    /**
     * Execute a transaction that may or may not be cross-shard.
     */
    public TransactionResult executeTransaction(SmallBankMessage2PC request) {
        if (isCrossShardTransaction(request)) {
            return executeCrossShardTransaction(request);
        } else {
            // Single-shard transaction - route directly
            return executeSingleShardTransaction(request);
        }
    }

    /**
     * Execute a single-shard transaction.
     */
    private TransactionResult executeSingleShardTransaction(SmallBankMessage2PC request) {
        String txId = generateTransactionId();
        int targetShard = getShardForAccount(request.getCustomerId());

        logger.debug("Executing single-shard transaction {} on shard {}", txId, targetShard);

        byte[] response = shardHandler.invokeOrderedOnShard(targetShard, request.getBytes());
        if (response == null) {
            return TransactionResult.failure(txId, "No response from shard " + targetShard, null);
        }

        SmallBankMessage2PC reply = SmallBankMessage2PC.getObject(response);
        Map<Integer, SmallBankMessage2PC> responses = new HashMap<>();
        responses.put(targetShard, reply);

        if (reply.getResult() == 0) {
            return TransactionResult.success(txId, responses);
        } else {
            return TransactionResult.failure(txId, reply.getErrorMsg(), responses);
        }
    }

    //  Cross-Shard Transaction Building
    /**
     * Build per-shard requests for a cross-shard transaction.
     */
    private CrossShardTransaction buildCrossShardTransaction(String txId, SmallBankMessage2PC request) {
        CrossShardTransaction crossTx = new CrossShardTransaction(txId);

        switch (request.getTxType()) {
            case SEND_PAYMENT:
                buildSendPaymentTransaction(crossTx, request);
                break;

            case AMALGAMATE:
                buildAmalgamateTransaction(crossTx, request);
                break;

            default:
                logger.warn("Unsupported transaction type for cross-shard: {}", request.getTxType());
        }

        return crossTx;
    }

    /**
     * Build cross-shard SEND_PAYMENT transaction.
     */
    private void buildSendPaymentTransaction(CrossShardTransaction crossTx, SmallBankMessage2PC request) {
        long srcAccount = request.getCustomerId();
        long destAccount = request.getDestCustomerId();
        double amount = request.getAmount();

        int srcShard = getShardForAccount(srcAccount);
        int destShard = getShardForAccount(destAccount);

        // Source shard: WRITE_CHECK (debit)
        SmallBankMessage2PC srcRequest = SmallBankMessage2PC.newPrepareRequest(
                crossTx.transactionId, myShardId, srcShard,
                SmallBankMessage2PC.TransactionType.WRITE_CHECK,
                srcAccount, amount);
        crossTx.addShardRequest(srcShard, srcRequest);

        // Destination shard: DEPOSIT_CHECKING (credit)
        SmallBankMessage2PC destRequest = SmallBankMessage2PC.newPrepareRequest(
                crossTx.transactionId, myShardId, destShard,
                SmallBankMessage2PC.TransactionType.DEPOSIT_CHECKING,
                destAccount, amount);
        crossTx.addShardRequest(destShard, destRequest);

        logger.debug("Built SEND_PAYMENT: shard {} (debit {}), shard {} (credit {})",
                srcShard, srcAccount, destShard, destAccount);
    }

    /**
     * Build cross-shard AMALGAMATE transaction.
     */
    private void buildAmalgamateTransaction(CrossShardTransaction crossTx, SmallBankMessage2PC request) {
        long account1 = request.getCustomerId();
        long account2 = request.getDestCustomerId();

        int shard1 = getShardForAccount(account1);
        int shard2 = getShardForAccount(account2);

        SmallBankMessage2PC shard2Request = SmallBankMessage2PC.newPrepareRequest(
                crossTx.transactionId, myShardId, shard2,
                SmallBankMessage2PC.TransactionType.WRITE_CHECK,
                account2, 0);
        crossTx.addShardRequest(shard2, shard2Request);

        SmallBankMessage2PC shard1Request = SmallBankMessage2PC.newPrepareRequest(
                crossTx.transactionId, myShardId, shard1,
                SmallBankMessage2PC.TransactionType.DEPOSIT_CHECKING,
                account1, 0); // Amount will be determined
        crossTx.addShardRequest(shard1, shard1Request);

        logger.debug("Built AMALGAMATE: shard {} (account {}), shard {} (account {})",
                shard1, account1, shard2, account2);
    }

    //Phase 1 (PREPARE)
    /**
     * Execute PREPARE phase. send PREPARE to all participants in parallel.
     */
    private Map<Integer, SmallBankMessage2PC> executePreparePhase(CrossShardTransaction crossTx) {
        logger.info("Phase 1 (PREPARE) starting for transaction {}", crossTx.transactionId);

        Map<Integer, Future<SmallBankMessage2PC>> futures = new HashMap<>();

        // Send PREPARE to all participants in parallel
        for (Map.Entry<Integer, SmallBankMessage2PC> entry : crossTx.shardRequests.entrySet()) {
            int shardId = entry.getKey();
            SmallBankMessage2PC prepareRequest = entry.getValue();

            Future<SmallBankMessage2PC> future = executor.submit(() -> {
                logger.debug("Sending PREPARE to shard {} for tx {}", shardId, crossTx.transactionId);
                byte[] response = shardHandler.invokeOrderedOnShard(shardId, prepareRequest.getBytes());
                if (response == null) {
                    logger.error("No response from shard {} for PREPARE", shardId);
                    return SmallBankMessage2PC.newPrepareFail(crossTx.transactionId,
                            "No response from shard " + shardId);
                }
                return SmallBankMessage2PC.getObject(response);
            });

            futures.put(shardId, future);
        }

        // Collect responses
        Map<Integer, SmallBankMessage2PC> responses = new HashMap<>();
        for (Map.Entry<Integer, Future<SmallBankMessage2PC>> entry : futures.entrySet()) {
            int shardId = entry.getKey();
            try {
                SmallBankMessage2PC response = entry.getValue().get(prepareTimeoutMs, TimeUnit.MILLISECONDS);
                responses.put(shardId, response);
                logger.info("PREPARE response from shard {}: {}", shardId,
                        response.isPrepareOk() ? "OK" : "FAIL - " + response.getErrorMsg());
            } catch (TimeoutException e) {
                logger.error("Timeout waiting for PREPARE response from shard {}", shardId);
                responses.put(shardId, SmallBankMessage2PC.newPrepareFail(
                        crossTx.transactionId, "Timeout waiting for shard " + shardId));
            } catch (Exception e) {
                logger.error("Error getting PREPARE response from shard {}", shardId, e);
                responses.put(shardId, SmallBankMessage2PC.newPrepareFail(
                        crossTx.transactionId, "Error: " + e.getMessage()));
            }
        }

        return responses;
    }

    /**
     * Check if all participants voted PREPARE_OK.
     */
    private boolean checkAllPrepared(Map<Integer, SmallBankMessage2PC> responses, Set<Integer> expectedShards) {
        // Check we have responses from all expected shards
        if (responses.size() != expectedShards.size()) {
            logger.warn("Missing responses: expected {} shards, got {}",
                    expectedShards.size(), responses.size());
            return false;
        }

        // Check all responses are PREPARE_OK
        for (Map.Entry<Integer, SmallBankMessage2PC> entry : responses.entrySet()) {
            SmallBankMessage2PC response = entry.getValue();
            if (!response.isPrepareOk()) {
                return false;
            }
        }

        return true;
    }

    /**
     * Extract failure reason from prepare responses.
     */
    private String getFailureReason(Map<Integer, SmallBankMessage2PC> responses) {
        StringBuilder reasons = new StringBuilder();
        for (Map.Entry<Integer, SmallBankMessage2PC> entry : responses.entrySet()) {
            SmallBankMessage2PC response = entry.getValue();
            if (response.isPrepareFail()) {
                if (reasons.length() > 0) reasons.append("; ");
                reasons.append("Shard ").append(entry.getKey()).append(": ").append(response.getErrorMsg());
            }
        }
        return reasons.length() > 0 ? reasons.toString() : "Unknown failure";
    }

    // Phase 2: COMMIT

    /**
     * Execute COMMIT phase - send COMMIT to all participants.
     */
    private Map<Integer, SmallBankMessage2PC> executeCommitPhase(CrossShardTransaction crossTx) {
        logger.info("Phase 2 (COMMIT) starting for transaction {}", crossTx.transactionId);
        return executePhase2(crossTx, true);
    }

    // Phase 2: ABORT

    /**
     * Execute ABORT phase - send ABORT to all participants.
     */
    private Map<Integer, SmallBankMessage2PC> executeAbortPhase(CrossShardTransaction crossTx) {
        logger.info("Phase 2 (ABORT) starting for transaction {}", crossTx.transactionId);
        return executePhase2(crossTx, false);
    }

    /**
     * Execute phase 2 (COMMIT or ABORT) to all participants.
     */
    private Map<Integer, SmallBankMessage2PC> executePhase2(CrossShardTransaction crossTx, boolean commit) {
        Map<Integer, Future<SmallBankMessage2PC>> futures = new HashMap<>();

        SmallBankMessage2PC phase2Message = commit
                ? SmallBankMessage2PC.newCommit(crossTx.transactionId, myShardId)
                : SmallBankMessage2PC.newAbort(crossTx.transactionId, myShardId);

        // Send to all participants in parallel
        for (int shardId : crossTx.getParticipantShards()) {
            Future<SmallBankMessage2PC> future = executor.submit(() -> {
                logger.debug("Sending {} to shard {} for tx {}",
                        commit ? "COMMIT" : "ABORT", shardId, crossTx.transactionId);
                byte[] response = shardHandler.invokeOrderedOnShard(shardId, phase2Message.getBytes());
                if (response == null) {
                    logger.error("No response from shard {} for phase 2", shardId);
                    return SmallBankMessage2PC.newErrorMessage("No response from shard " + shardId);
                }
                return SmallBankMessage2PC.getObject(response);
            });

            futures.put(shardId, future);
        }

        // Collect responses
        Map<Integer, SmallBankMessage2PC> responses = new HashMap<>();
        for (Map.Entry<Integer, Future<SmallBankMessage2PC>> entry : futures.entrySet()) {
            int shardId = entry.getKey();
            try {
                SmallBankMessage2PC response = entry.getValue().get(commitTimeoutMs, TimeUnit.MILLISECONDS);
                responses.put(shardId, response);
                logger.info("{} ACK from shard {}", commit ? "COMMIT" : "ABORT", shardId);
            } catch (Exception e) {
                logger.error("Error getting phase 2 response from shard {}", shardId, e);
                responses.put(shardId, SmallBankMessage2PC.newErrorMessage("Error: " + e.getMessage()));
            }
        }

        logger.info("Phase 2 ({}) completed for transaction {}",
                commit ? "COMMIT" : "ABORT", crossTx.transactionId);

        return responses;
    }

    // Helpers
    public TransactionResult sendCrossShardPayment(long sourceAccount, long destAccount, double amount) {
        SmallBankMessage2PC request = SmallBankMessage2PC.newSendPaymentRequest(
                sourceAccount, destAccount, amount);
        return executeTransaction(request);
    }

    /**
     * Shutdown the coordinator.
     */
    public void shutdown() {
        logger.info("Shutting down TwoPhaseCoordinator");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
