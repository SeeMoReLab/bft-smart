package bftsmart.demo.smallbank2pc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.demo.util.TimeoutLearningWindowMetrics;
import bftsmart.rlrpc.LearningAgentGrpc;
import bftsmart.rlrpc.PbftReport;
import bftsmart.rlrpc.Protocol;
import bftsmart.rlrpc.Reward;
import bftsmart.rlrpc.ReportLocal;
import bftsmart.rlrpc.TimeoutRequest;
import bftsmart.rlrpc.TimeoutStatus;
import bftsmart.rlrpc.TwoPcOverPbftReport;
import bftsmart.rlrpc.TwoPcOverPbftReward;
import bftsmart.rlrpc.TwoPcOverPbftTimeout;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.ShardHandler;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import bftsmart.tom.util.FailureInjectionCliArgs;
import bftsmart.tom.util.FailureInjectionController;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class SmallBankServer2PC extends DefaultRecoverable {
    private static final Logger logger = LoggerFactory.getLogger(SmallBankServer2PC.class);
    private static final boolean _debug = false;
    private static final int REPORT_TICK_INTERVAL = 10;
    private static final long REPORT_TRIGGER_ELAPSED_MS = 5000L;
    private static final int MAX_REPORT_LENGTH = 500;
    private static final int POLL_INTERVAL_MS = 50;

    // Account data
    private HashMap<Long, String> accounts;
    private HashMap<Long, Double> checking;
    private HashMap<Long, Double> savings;

    // 2PC state management
    private final Map<String, PendingTransaction> pendingTransactions = new ConcurrentHashMap<>();
    private final Set<Long> lockedAccounts = ConcurrentHashMap.newKeySet();

    private boolean logPrinted = false;

    /* Adaptive timers */
    private TimeoutLearningWindowMetrics learningMetrics;
    // Legacy snapshot field kept for backward compatibility.
    private long iterations = 0;
    private int lastProcessedConsensusId = -1;
    private ServiceReplica replica;
    private ManagedChannel learnerChannel;
    private LearningAgentGrpc.LearningAgentBlockingStub learnerStub;
    private final Object pollerLock = new Object();
    private Thread timeoutPollerThread;
    private volatile boolean pollerStopRequested = false;
    private volatile TimeoutDecision pollerDecision = null;
    private volatile int pollerEpisode = -1;
    private int currentTimeoutMs = -1;
    private int lastTimeoutUsedMs = -1;
    private TwoPcOverPbftReport pendingRewardReport;
    private int pendingRewardEpisode;
    private int pendingRewardTimeoutMs;
    private int singleShardTransactionCount;
    private final boolean learning;

    // Shard configuration (for logging/debugging)
    private ReplicaContext replicaContext;
    private int myReplicaId;
    private int myShardId;
    private int totalShards;

    private int currentEpisode = 1;
    private int episodeStartTick = 0;
    private long episodeStartWallClockMs = -1L;
    private boolean reportSentForEpisode = false;
    private boolean reachedReportCapForEpisode = false;
    private int capApplyDeadlineTick = -1;
    private int capRewardDeadlineTick = -1;
    private EpisodeWindow selectedWindow = null;
    private boolean applyHandledForEpisode = false;
    private boolean rewardCapturedForEpisode = false;
    private boolean waitingForRecommendation = false;

    private static final class TimeoutDecision {
        private final int timeoutMs;
        private final int startTick;
        private final int reportSeq;
        private final int reportLength;

        private TimeoutDecision(int timeoutMs, int startTick, int reportSeq, int reportLength) {
            this.timeoutMs = timeoutMs;
            this.startTick = startTick;
            this.reportSeq = reportSeq;
            this.reportLength = reportLength;
        }
    }

    private static final class EpisodeWindow {
        private final int startTick;
        private final int reportSeq;
        private final int reportLength;
        private final int applyTick;
        private final int rewardTick;

        private EpisodeWindow(int startTick, int reportSeq, int reportLength) {
            this.startTick = startTick;
            this.reportSeq = reportSeq;
            this.reportLength = reportLength;
            this.applyTick = reportSeq + (reportLength / 2);
            this.rewardTick = reportSeq + reportLength;
        }
    }

    /**
     * Represents a transaction that has been prepared but not yet committed/aborted.
     *
     * Simplified design:
     * - PREPARE: validate + acquire locks (no state changes)
     * - COMMIT: execute transaction + release locks
     * - ABORT: just release locks
     */
    private static class PendingTransaction {
        final String transactionId;
        final Set<Long> lockedAccountIds;
        final long prepareTime;

        // Transaction details (needed for execution on commit)
        final SmallBankMessage2PC.TransactionType txType;
        final long customerId;
        final double amount;

        PendingTransaction(String txId, Set<Long> locks,
                          SmallBankMessage2PC.TransactionType txType,
                          long customerId, double amount) {
            this.transactionId = txId;
            this.lockedAccountIds = locks;
            this.prepareTime = System.currentTimeMillis();
            this.txType = txType;
            this.customerId = customerId;
            this.amount = amount;
        }
    }

    public static void main(String[] args) throws Exception {
        ParsedArgs parsedArgs = parseArgs(args);
        configureFailureInjection(parsedArgs.failureArgs);
        List<String> positionalArgs = parsedArgs.failureArgs.getPositionalArgs();

        if (positionalArgs.size() == 2) {
            new SmallBankServer2PC(
                    Integer.parseInt(positionalArgs.get(0)),
                    Integer.parseInt(positionalArgs.get(1)),
                    1,
                    parsedArgs.configDir,
                    parsedArgs.learning);
        } else if (positionalArgs.size() == 3) {
            new SmallBankServer2PC(
                    Integer.parseInt(positionalArgs.get(0)),  // shardId
                    Integer.parseInt(positionalArgs.get(1)),  // replicaId
                    Integer.parseInt(positionalArgs.get(2)),  // totalShards
                    parsedArgs.configDir,
                    parsedArgs.learning
            );
        } else {
            System.out.println("Usage: java ... SmallBankServer2PC <shard_id> <replica_id> "
                    + "[<total_shards>] [--config-dir <path>] "
                    + "[--learning] "
                    + "[--failure-spec <path>] [--failure-start-unix-ms <ms>]");
        }
    }

    private static void configureFailureInjection(FailureInjectionCliArgs.Parsed cliArgs) {
        if (!cliArgs.hasFailureInjection()) {
            FailureInjectionController.disable();
            return;
        }
        FailureInjectionController.configure(cliArgs.getFailureSpecPath(), cliArgs.getFailureStartUnixMs());
    }

    private static ParsedArgs parseArgs(String[] args) {
        List<String> remaining = new ArrayList<>();
        String configDir = null;
        boolean learning = false;
        boolean consultFlagConfigured = false;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("--config-dir".equals(arg)) {
                if (configDir != null) {
                    throw new IllegalArgumentException("Duplicate flag: --config-dir");
                }
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing value for --config-dir");
                }
                configDir = args[++i].trim();
                if (configDir.isEmpty()) {
                    throw new IllegalArgumentException("Missing value for --config-dir");
                }
                continue;
            }
            if (arg.startsWith("--config-dir=")) {
                if (configDir != null) {
                    throw new IllegalArgumentException("Duplicate flag: --config-dir");
                }
                configDir = arg.substring("--config-dir=".length()).trim();
                if (configDir.isEmpty()) {
                    throw new IllegalArgumentException("Missing value for --config-dir");
                }
                continue;
            }
            if ("--learning".equals(arg)) {
                if (consultFlagConfigured) {
                    throw new IllegalArgumentException("Duplicate flag: --learning");
                }
                learning = true;
                consultFlagConfigured = true;
                continue;
            }
            remaining.add(arg);
        }

        FailureInjectionCliArgs.Parsed failureArgs = FailureInjectionCliArgs.parse(remaining.toArray(new String[0]));
        return new ParsedArgs(configDir, learning, failureArgs);
    }

    private static final class ParsedArgs {
        private final String configDir;
        private final boolean learning;
        private final FailureInjectionCliArgs.Parsed failureArgs;

        private ParsedArgs(String configDir,
                           boolean learning,
                           FailureInjectionCliArgs.Parsed failureArgs) {
            this.configDir = configDir;
            this.learning = learning;
            this.failureArgs = failureArgs;
        }
    }

    private SmallBankServer2PC(int shardId, int id) {
        this(shardId, id, 1, null, false);
    }

    /**
     * Constructor with shard configuration for leader-based 2PC.
     *
     * @param shardId     This shard's ID
     * @param replicaId   This replica's ID within the shard
     * @param totalShards Total number of shards in the system
     * @param configHome  Configuration directory (null for default)
     */
    public SmallBankServer2PC(int shardId, int replicaId, int totalShards, String configHome) {
        this(shardId, replicaId, totalShards, configHome, false);
    }

    public SmallBankServer2PC(int shardId,
                              int replicaId,
                              int totalShards,
                              String configHome,
                              boolean learning) {
        this.myShardId = shardId;
        this.myReplicaId = replicaId;
        this.totalShards = totalShards;
        this.learning = learning;
        this.accounts = new HashMap<>();
        this.checking = new HashMap<>();
        this.savings = new HashMap<>();
        this.learningMetrics = new TimeoutLearningWindowMetrics(MAX_REPORT_LENGTH * 2);

        if (configHome != null) {
            replica = new ServiceReplica(shardId, replicaId, configHome, this, this);
        } else {
            replica = new ServiceReplica(shardId, replicaId, this, this);
        }
    }

    @Override
    public void setReplicaContext(ReplicaContext ctx) {
        super.setReplicaContext(ctx);
        this.replicaContext = ctx;
        this.myReplicaId = ctx.getSVController().getStaticConf().getProcessId();
        logger.info("ReplicaContext set: shardId={}, replicaId={}", myShardId, myReplicaId);
        if (learning) {
            initLearningAgentClient();
        } else {
            logger.info("Learning-agent timeout recommendation is disabled.");
        }
        if (currentTimeoutMs < 0) {
            currentTimeoutMs = ctx.getSVController().getStaticConf().getRequestTimeout();
            lastTimeoutUsedMs = currentTimeoutMs;
        }
    }

    @Override
    public byte[][] appExecuteBatch(byte[][] commands, MessageContext[] msgCtx, boolean fromConsensus) {
        byte[][] replies = new byte[commands.length][];
        Map<Integer, Integer> batchSizesByConsensus = buildBatchSizesByConsensus(msgCtx);
        int index = 0;
        for (byte[] command : commands) {
            MessageContext currentMsgCtx = (msgCtx != null) ? msgCtx[index] : null;
            if (currentMsgCtx != null && currentMsgCtx.getConsensusId() % 1000 == 0 && !logPrinted) {
                System.out.println("SmallBankServer2PC executing CID: " + currentMsgCtx.getConsensusId());
                logPrinted = true;
            } else {
                logPrinted = false;
            }

            if (currentMsgCtx != null) {
                int consensusId = currentMsgCtx.getConsensusId();
                if (consensusId > lastProcessedConsensusId) {
                    lastProcessedConsensusId = consensusId;
                    int batchSize = batchSizesByConsensus.getOrDefault(consensusId, 1);
                    handleConsensusProgress(consensusId, currentMsgCtx, batchSize);
                }
            }

            SmallBankMessage2PC request = SmallBankMessage2PC.getObject(command);
            SmallBankMessage2PC reply = SmallBankMessage2PC.newErrorMessage("Unknown error");

            if (request == null) {
                replies[index] = reply.getBytes();
                continue;
            }

            if (_debug) {
                System.out.println("[INFO] Processing ordered request: " + request);
            }

            try {
                // Handle 2PC messages (PREPARE, COMMIT, ABORT) from coordinator
                if (request.is2PCMessage()) {
                    reply = handle2PCMessage(request);
                    replies[index++] = reply.getBytes();
                    continue;
                }

                singleShardTransactionCount++;

                // Regular transaction processing
                switch (request.getTxType()) {
                    case CREATE_ACCOUNT: {
                        long custId = request.getCustomerId();
                        if (accounts.containsKey(custId)) {
                            reply = SmallBankMessage2PC.newErrorMessage("Account already exists");
                        } else {
                            accounts.put(custId, request.getCustomerName());
                            checking.put(custId, request.getCheckingBalance());
                            savings.put(custId, request.getSavingsBalance());
                            reply = SmallBankMessage2PC.newResponse(0);
                        }
                        break;
                    }

                    case DEPOSIT_CHECKING: {
                        long custId = request.getCustomerId();
                        if (!checking.containsKey(custId)) {
                            reply = SmallBankMessage2PC.newErrorMessage("Account not found");
                        } else {
                            double balance = checking.get(custId) + request.getAmount();
                            checking.put(custId, balance);
                            System.out.println("New checking balance for customer " + custId + ": " + balance);
                            reply = SmallBankMessage2PC.newResponse(0);
                        }
                        break;
                    }

                    case TRANSACT_SAVINGS: {
                        long custId = request.getCustomerId();
                        if (!savings.containsKey(custId)) {
                            reply = SmallBankMessage2PC.newErrorMessage("Account not found");
                        } else {
                            double balance = savings.get(custId) + request.getAmount();
                            if (balance < 0) {
                                reply = SmallBankMessage2PC.newErrorMessage("Insufficient funds");
                            } else {
                                savings.put(custId, balance);
                                reply = SmallBankMessage2PC.newResponse(0);
                            }
                        }
                        break;
                    }

                    case WRITE_CHECK: {
                        long custId = request.getCustomerId();
                        if (!checking.containsKey(custId)) {
                            reply = SmallBankMessage2PC.newErrorMessage("Account not found");
                        } else {
                            double balance = checking.get(custId) - request.getAmount();
                            if (balance < 0) {
                                reply = SmallBankMessage2PC.newErrorMessage("Insufficient funds");
                            } else {
                                checking.put(custId, balance);
                                reply = SmallBankMessage2PC.newResponse(0);
                            }
                        }
                        break;
                    }

                    case SEND_PAYMENT: {
                        long srcId = request.getCustomerId();
                        long destId = request.getDestCustomerId();

                        if (!checking.containsKey(srcId)) {
                            reply = SmallBankMessage2PC.newErrorMessage("Source account not found");
                        } else if (!checking.containsKey(destId)) {
                            reply = SmallBankMessage2PC.newErrorMessage("Destination account not found");
                        } else {
                            double srcBalance = checking.get(srcId) - request.getAmount();
                            if (srcBalance < 0) {
                                reply = SmallBankMessage2PC.newErrorMessage("Insufficient funds");
                            } else {
                                double destBalance = checking.get(destId) + request.getAmount();
                                checking.put(srcId, srcBalance);
                                checking.put(destId, destBalance);
                                reply = SmallBankMessage2PC.newResponse(0);
                            }
                        }
                        break;
                    }

                    case AMALGAMATE: {
                        long custId1 = request.getCustomerId();
                        long custId2 = request.getDestCustomerId();

                        if (!checking.containsKey(custId1) || !savings.containsKey(custId1)) {
                            reply = SmallBankMessage2PC.newErrorMessage("Account 1 not found");
                        } else if (!checking.containsKey(custId2) || !savings.containsKey(custId2)) {
                            reply = SmallBankMessage2PC.newErrorMessage("Account 2 not found");
                        } else {
                            // Transfer all from custId2's checking to custId1's savings
                            double amountToTransfer = checking.get(custId2);
                            checking.put(custId2, 0.0);
                            savings.put(custId1, savings.get(custId1) + amountToTransfer);
                            reply = SmallBankMessage2PC.newResponse(0);
                        }
                        break;
                    }

                    default:
                        reply = SmallBankMessage2PC.newErrorMessage("Unknown operation type");
                        break;
                }
            } catch (Exception e) {
                reply = SmallBankMessage2PC.newErrorMessage("Exception: " + e.getMessage());
                if (_debug) {
                    e.printStackTrace();
                }
            }

            if (_debug) {
                System.out.println("[INFO] Sending reply");
            }
            replies[index++] = reply.getBytes();
        }
        return replies;
    }

    @Override
    public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
        SmallBankMessage2PC request = SmallBankMessage2PC.getObject(command);
        SmallBankMessage2PC reply = SmallBankMessage2PC.newErrorMessage("Unknown error");

        if (request == null) {
            return reply.getBytes();
        }

        switch (request.getTxType()) {
            case BALANCE:
                long custId = request.getCustomerId();
                if (!checking.containsKey(custId) || !savings.containsKey(custId)) {
                    reply = SmallBankMessage2PC.newErrorMessage("Account not found");
                } else {
                    double checkingBalance = checking.get(custId);
                    double savingsBalance = savings.get(custId);
                    reply = SmallBankMessage2PC.newResponseWithBalances(0, checkingBalance, savingsBalance);
                }
                return reply.getBytes();
        }

        return reply.getBytes();
    }

    @Override
    public void installSnapshot(byte[] state) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(state);
            ObjectInput in = new ObjectInputStream(bis);
            accounts = (HashMap<Long, String>) in.readObject();
            checking = (HashMap<Long, Double>) in.readObject();
            savings = (HashMap<Long, Double>) in.readObject();
            try {
                iterations = in.readLong();
            } catch (EOFException e) {
                iterations = 0;
            }
            in.close();
            bis.close();
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[ERROR] Error deserializing state: "
                    + e.getMessage());
        }
    }

    @Override
    public byte[] getSnapshot() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(accounts);
            out.writeObject(checking);
            out.writeObject(savings);
            out.writeLong(iterations);
            out.flush();
            bos.flush();
            out.close();
            bos.close();
            return bos.toByteArray();
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error serializing state: "
                    + ioe.getMessage());
            return "ERROR".getBytes();
        }
    }

    // 2PC Handler Methods

    /**
     * Main dispatcher for 2PC messages.
     */
    private SmallBankMessage2PC handle2PCMessage(SmallBankMessage2PC request) {
        System.out.println("[INFO] Handling 2PC message: " + request);
        System.out.println("[INFO] Current locked accounts: " + lockedAccounts);
        System.out.println("[INFO] TwoPhaseType: " + request.getTwoPhaseType());
        switch (request.getTwoPhaseType()) {
            case PREPARE:
                return handlePrepare(request);
            case COMMIT:
                return handleCommit(request);
            case ABORT:
                return handleAbort(request);
            default:
                logger.warn("Unknown 2PC message type: {}", request.getTwoPhaseType());
                return SmallBankMessage2PC.newErrorMessage("Unknown 2PC message type");
        }
    }

    /**
     * Handle PREPARE phase of 2PC.
     * Validates the transaction can succeed and acquires locks.
     * No state changes are made - just validation and locking.
     */
    private SmallBankMessage2PC handlePrepare(SmallBankMessage2PC request) {
        String txId = request.getTransactionId();
        logger.info("PREPARE received for txId={}, type={}, customerId={}, amount={}",
                   txId, request.getTxType(), request.getCustomerId(), request.getAmount());

        // Check if we already have this transaction prepared
        if (pendingTransactions.containsKey(txId)) {
            logger.info("Transaction {} already prepared", txId);
            return SmallBankMessage2PC.newPrepareOk(txId);
        }

        // Determine which accounts need to be locked
        Set<Long> accountsToLock = getAccountsToLock(request);

        // Try to acquire locks
        long lockWaitStartNs = System.nanoTime();
        boolean lockAcquired = tryAcquireLocks(accountsToLock);
        recordParticipantLockWait(System.nanoTime() - lockWaitStartNs);
        if (!lockAcquired) {
            recordParticipantLockContention();
            logger.info("Cannot acquire locks for transaction {}, accounts {} are locked", txId, accountsToLock);
            return SmallBankMessage2PC.newPrepareFail(txId, "Cannot acquire locks - accounts busy");
        }

        // Validate the transaction can succeed
        String validationError = validateTransaction(request);
        if (validationError != null) {
            // Release locks on validation failure
            releaseLocks(accountsToLock);
            logger.info("Validation failed for transaction {}: {}", txId, validationError);
            return SmallBankMessage2PC.newPrepareFail(txId, validationError);
        }

        // Create pending transaction - just track locks and transaction details
        // No state changes yet - those happen in COMMIT
        PendingTransaction pending = new PendingTransaction(
            txId, accountsToLock,
            request.getTxType(), request.getCustomerId(), request.getAmount()
        );
        pendingTransactions.put(txId, pending);

        logger.info("PREPARE successful for txId={}", txId);
        return SmallBankMessage2PC.newPrepareOk(txId);
    }

    /**
     * Handle COMMIT phase of 2PC.
     * Executes the actual transaction and releases locks.
     * Transaction details come from the COMMIT message.
     */
    private SmallBankMessage2PC handleCommit(SmallBankMessage2PC request) {
        String txId = request.getTransactionId();
        logger.info("COMMIT received for txId={}, type={}, customerId={}, amount={}",
                   txId, request.getTxType(), request.getCustomerId(), request.getAmount());

        PendingTransaction pending = pendingTransactions.remove(txId);
        if (pending == null) {
            logger.warn("No pending transaction found for commit: {}", txId);
            // Still try to execute if we have details in the request
            // This can happen in edge cases
            if (request.getTxType() != null) {
                executeTransaction(request.getTxType(), request.getCustomerId(), request.getAmount());
            }
            return SmallBankMessage2PC.newAck(txId);
        }

        // Execute the actual transaction using details from the COMMIT message
        // (or fall back to pending transaction details if not in COMMIT)
        SmallBankMessage2PC.TransactionType txType = request.getTxType() != null
            ? request.getTxType() : pending.txType;
        long customerId = request.getCustomerId() > 0
            ? request.getCustomerId() : pending.customerId;
        double amount = request.getAmount() > 0
            ? request.getAmount() : pending.amount;

        executeTransaction(txType, customerId, amount);
        recordParticipantLockHold(pending);

        // Release locks
        releaseLocks(pending.lockedAccountIds);

        logger.info("COMMIT successful for txId={}", txId);
        return SmallBankMessage2PC.newAck(txId);
    }

    /**
     * Execute the actual transaction (called during COMMIT phase).
     */
    private void executeTransaction(SmallBankMessage2PC.TransactionType txType,
                                    long customerId, double amount) {
        switch (txType) {
            case DEPOSIT_CHECKING:
                if (checking.containsKey(customerId)) {
                    double balance = checking.get(customerId) + amount;
                    checking.put(customerId, balance);
                    logger.debug("DEPOSIT_CHECKING: {} += {} = {}", customerId, amount, balance);
                }
                break;

            case WRITE_CHECK:
                if (checking.containsKey(customerId)) {
                    double balance = checking.get(customerId) - amount;
                    checking.put(customerId, balance);
                    logger.debug("WRITE_CHECK: {} -= {} = {}", customerId, amount, balance);
                }
                break;

            case TRANSACT_SAVINGS:
                if (savings.containsKey(customerId)) {
                    double balance = savings.get(customerId) + amount;
                    savings.put(customerId, balance);
                    logger.debug("TRANSACT_SAVINGS: {} += {} = {}", customerId, amount, balance);
                }
                break;

            default:
                logger.warn("Unsupported transaction type in commit: {}", txType);
        }
    }

    /**
     * Handle ABORT phase of 2PC.
     * Discards the prepared transaction and releases locks.
     */
    private SmallBankMessage2PC handleAbort(SmallBankMessage2PC request) {
        String txId = request.getTransactionId();
        logger.info("ABORT received for txId={}", txId);

        PendingTransaction pending = pendingTransactions.remove(txId);
        if (pending == null) {
            logger.warn("No pending transaction found for abort: {}", txId);
            return SmallBankMessage2PC.newAck(txId);
        }

        // Just release locks - no changes to apply
        recordParticipantLockHold(pending);
        releaseLocks(pending.lockedAccountIds);

        logger.info("ABORT successful for txId={}", txId);
        return SmallBankMessage2PC.newAck(txId);
    }

    // 2PC Helpers
    private Set<Long> getAccountsToLock(SmallBankMessage2PC request) {
        Set<Long> accounts = new HashSet<>();
        accounts.add(request.getCustomerId());
        if (request.getDestCustomerId() > 0) {
            accounts.add(request.getDestCustomerId());
        }
        return accounts;
    }

    /**
     * Try to acquire locks on the specified accounts.
     */
    private boolean tryAcquireLocks(Set<Long> accountIds) {
        // Sort accounts to acquire locks in consistent order (prevents deadlock)
        List<Long> sortedAccounts = new ArrayList<>(accountIds);
        Collections.sort(sortedAccounts);

        List<Long> acquiredLocks = new ArrayList<>();
        for (Long accountId : sortedAccounts) {
            if (lockedAccounts.add(accountId)) {
                acquiredLocks.add(accountId);
            } else {
                // Failed to acquire lock - release all acquired locks
                for (Long acquired : acquiredLocks) {
                    logger.info("Releasing lock on account {} due to failure to acquire lock on account {}", acquired, accountId);
                    lockedAccounts.remove(acquired);
                }
                return false;
            }
        }
        return true;
    }
    
    private void releaseLocks(Set<Long> accountIds) {
        lockedAccounts.removeAll(accountIds);
    }

    /**
     * Validate that a transaction can be executed.
     * Returns null if valid, or an error message if invalid.
     */
    private String validateTransaction(SmallBankMessage2PC request) {
        long custId = request.getCustomerId();

        switch (request.getTxType()) {
            case DEPOSIT_CHECKING:
                if (!checking.containsKey(custId)) {
                    return "Account not found: " + custId;
                }
                break;

            case TRANSACT_SAVINGS:
                if (!savings.containsKey(custId)) {
                    return "Account not found: " + custId;
                }
                double savBal = savings.get(custId) + request.getAmount();
                if (savBal < 0) {
                    return "Insufficient funds in savings";
                }
                break;

            case WRITE_CHECK:
                if (!checking.containsKey(custId)) {
                    return "Account not found: " + custId;
                }
                double chkBal = checking.get(custId) - request.getAmount();
                if (chkBal < 0) {
                    return "Insufficient funds in checking";
                }
                break;

            case SEND_PAYMENT:
                long destId = request.getDestCustomerId();
                if (!checking.containsKey(custId)) {
                    return "Source account not found: " + custId;
                }
                if (!checking.containsKey(destId)) {
                    return "Destination account not found: " + destId;
                }
                double srcBal = checking.get(custId) - request.getAmount();
                if (srcBal < 0) {
                    return "Insufficient funds for payment";
                }
                break;

            case AMALGAMATE:
                long custId2 = request.getDestCustomerId();
                if (!checking.containsKey(custId) || !savings.containsKey(custId)) {
                    return "Account 1 not found: " + custId;
                }
                if (!checking.containsKey(custId2) || !savings.containsKey(custId2)) {
                    return "Account 2 not found: " + custId2;
                }
                break;

            default:
                return "Unsupported transaction type for 2PC: " + request.getTxType();
        }

        return null;
    }

    private void initLearningAgentClient() {
        if (!learning) {
            return;
        }
        if (learnerStub != null || replicaContext == null) {
            return;
        }
        String host = replicaContext.getSVController().getStaticConf().getHost(myReplicaId);
        int port = replicaContext.getSVController().getStaticConf().getLearnerPort(myReplicaId);
        if (port <= 0) {
            logger.warn("Learner port not configured for replica {}. Reports will not be sent.", myReplicaId);
            return;
        }
        learnerChannel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        learnerStub = LearningAgentGrpc.newBlockingStub(learnerChannel);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                learnerChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        logger.info("LearningAgent client configured for {}:{}", host, port);
    }

    private TwoPcOverPbftReport buildReportFromStorage() {
        PbftReport pbftReport = learningMetrics.buildReport();
        if (pbftReport == null) {
            return null;
        }
        ShardHandler shardHandler = replica != null ? replica.getShardHandler() : null;
        if (shardHandler == null) {
            return TwoPcOverPbftReport.newBuilder()
                    .setPbft(pbftReport)
                    .setTotalTransactions(Math.max(singleShardTransactionCount, 0))
                    .setSingleShardTransactions(Math.max(singleShardTransactionCount, 0))
                    .setCrossShardTransactions(0)
                    .build();
        }
        return shardHandler.buildTwoPcOverPbftReport(pbftReport, singleShardTransactionCount);
    }

    private Map<Integer, Integer> buildBatchSizesByConsensus(MessageContext[] msgCtx) {
        Map<Integer, Integer> batchSizesByConsensus = new HashMap<>();
        if (msgCtx == null) {
            return batchSizesByConsensus;
        }
        for (MessageContext ctx : msgCtx) {
            if (ctx == null) {
                continue;
            }
            int consensusId = ctx.getConsensusId();
            batchSizesByConsensus.put(consensusId, batchSizesByConsensus.getOrDefault(consensusId, 0) + 1);
        }
        return batchSizesByConsensus;
    }

    private void handleConsensusProgress(int consensusId, MessageContext context, int batchSize) {
        learningMetrics.recordConsensus(context, batchSize, currentTimeoutMs);
        if (episodeStartWallClockMs < 0L) {
            episodeStartWallClockMs = System.currentTimeMillis();
            episodeStartTick = consensusId;
        }
        maybeConsumeRecommendation(consensusId);
        if (consensusId % REPORT_TICK_INTERVAL == 0) {
            maybeSendReportTick(consensusId);
        }
        maybeHandleApplyDeadline(consensusId);
        maybeHandleRewardDeadline(consensusId);
    }

    private void maybeSendReportTick(int consensusId) {
        if (!learning || learnerStub == null) {
            return;
        }
        if (selectedWindow != null || reachedReportCapForEpisode) {
            return;
        }
        int reportLength = consensusId - episodeStartTick;
        if (reportLength <= 0) {
            return;
        }
        long elapsedMs = System.currentTimeMillis() - episodeStartWallClockMs;
        boolean shouldSendInitial = !reportSentForEpisode
                && (elapsedMs >= REPORT_TRIGGER_ELAPSED_MS || reportLength >= MAX_REPORT_LENGTH);
        boolean shouldSendFollowup = reportSentForEpisode && waitingForRecommendation;
        if (!shouldSendInitial && !shouldSendFollowup) {
            return;
        }

        int effectiveReportLength = Math.min(reportLength, MAX_REPORT_LENGTH);
        int reportSeq = episodeStartTick + effectiveReportLength;
        sendStateReport(currentEpisode, episodeStartTick, reportSeq);
        reportSentForEpisode = true;
        waitingForRecommendation = true;
        maybeStartTimeoutPolling(currentEpisode);

        if (effectiveReportLength >= MAX_REPORT_LENGTH) {
            reachedReportCapForEpisode = true;
            EpisodeWindow capWindow = new EpisodeWindow(episodeStartTick, reportSeq, effectiveReportLength);
            capApplyDeadlineTick = capWindow.applyTick;
            capRewardDeadlineTick = capWindow.rewardTick;
        }
    }

    private void maybeConsumeRecommendation(int consensusId) {
        if (!learning || selectedWindow != null) {
            return;
        }
        TimeoutDecision decision = pollerDecision;
        if (decision == null || pollerEpisode != currentEpisode) {
            return;
        }
        selectedWindow = new EpisodeWindow(decision.startTick, decision.reportSeq, decision.reportLength);
        waitingForRecommendation = false;
        stopTimeoutPolling();
        logger.info(
                "[learning] received recommendation: episode={} report_seq={} apply_tick={} reward_tick={} timeout_ms={} current_cid={}",
                currentEpisode,
                selectedWindow.reportSeq,
                selectedWindow.applyTick,
                selectedWindow.rewardTick,
                decision.timeoutMs,
                consensusId
        );
        if (consensusId > selectedWindow.applyTick) {
            applyHandledForEpisode = true;
            lastTimeoutUsedMs = currentTimeoutMs;
            logger.info(
                    "[learning] ignored late recommendation: episode={} report_seq={} apply_tick={} current_cid={}",
                    currentEpisode,
                    selectedWindow.reportSeq,
                    selectedWindow.applyTick,
                    consensusId
            );
        }
    }

    private void maybeHandleApplyDeadline(int consensusId) {
        if (applyHandledForEpisode) {
            return;
        }
        if (selectedWindow != null) {
            if (consensusId < selectedWindow.applyTick) {
                return;
            }
            boolean appliedRecommendation = false;
            if (consensusId == selectedWindow.applyTick && pollerDecision != null) {
                currentTimeoutMs = pollerDecision.timeoutMs;
                replica.getRequestsTimer().setShortTimeoutPreservingEffectiveTimeout(currentTimeoutMs);
                appliedRecommendation = true;
            }
            applyHandledForEpisode = true;
            lastTimeoutUsedMs = currentTimeoutMs;
            if (appliedRecommendation) {
                logger.info(
                        "[learning] applied recommendation on time: episode={} report_seq={} apply_tick={} current_cid={} timeout_ms={}",
                        currentEpisode,
                        selectedWindow.reportSeq,
                        selectedWindow.applyTick,
                        consensusId,
                        currentTimeoutMs
                );
            } else {
                logger.info(
                        "[learning] apply deadline reached without recommendation update: episode={} report_seq={} apply_tick={} current_cid={} timeout_ms={}",
                        currentEpisode,
                        selectedWindow.reportSeq,
                        selectedWindow.applyTick,
                        consensusId,
                        currentTimeoutMs
                );
            }
            return;
        }
        if (reachedReportCapForEpisode && capApplyDeadlineTick >= 0 && consensusId >= capApplyDeadlineTick) {
            waitingForRecommendation = false;
            applyHandledForEpisode = true;
            lastTimeoutUsedMs = currentTimeoutMs;
            stopTimeoutPolling();
            logger.info(
                    "[learning] recommendation unavailable at cap apply deadline: episode={} cap_apply_tick={} current_cid={} timeout_ms={}",
                    currentEpisode,
                    capApplyDeadlineTick,
                    consensusId,
                    currentTimeoutMs
            );
        }
    }

    private void maybeHandleRewardDeadline(int consensusId) {
        if (rewardCapturedForEpisode) {
            return;
        }
        int rewardDeadline = -1;
        String rewardSource = "none";
        if (selectedWindow != null) {
            rewardDeadline = selectedWindow.rewardTick;
            rewardSource = "recommended_window";
        } else if (reachedReportCapForEpisode && capRewardDeadlineTick >= 0) {
            rewardDeadline = capRewardDeadlineTick;
            rewardSource = "cap_window";
        }
        if (rewardDeadline < 0 || consensusId < rewardDeadline) {
            return;
        }
        captureReward(currentEpisode);
        rewardCapturedForEpisode = true;
        stopTimeoutPolling();
        logger.info(
                "[learning] captured reward: episode={} reward_tick={} current_cid={} source={} timeout_ms={}",
                currentEpisode,
                rewardDeadline,
                consensusId,
                rewardSource,
                lastTimeoutUsedMs
        );
        startNextEpisode(consensusId);
    }

    private void startNextEpisode(int startTick) {
        currentEpisode++;
        episodeStartTick = startTick;
        episodeStartWallClockMs = System.currentTimeMillis();
        reportSentForEpisode = false;
        reachedReportCapForEpisode = false;
        capApplyDeadlineTick = -1;
        capRewardDeadlineTick = -1;
        selectedWindow = null;
        applyHandledForEpisode = false;
        rewardCapturedForEpisode = false;
        waitingForRecommendation = false;
        pollerDecision = null;
        pollerEpisode = -1;
        resetWindowMetrics();
    }

    private void sendStateReport(int episode, int startTick, int reportSeq) {
        if (!learning || learnerStub == null) {
            return;
        }
        TwoPcOverPbftReport report = buildReportFromStorage();
        if (report == null) {
            return;
        }
        int nodeId = (myReplicaId >= 0) ? myReplicaId : replica.getId();
        ReportLocal.Builder localBuilder = ReportLocal.newBuilder()
                .setNodeId(nodeId)
                .setEpisode(episode)
                .setProtocol(Protocol.PROTOCOL_TWO_PC_OVER_PBFT)
                .setStartTick(startTick)
                .setReportSeq(reportSeq)
                .setTwoPcOverPbftState(report);

        if (pendingRewardReport != null) {
            int prepareTimeoutMs = getPrepareTimeoutMs();
            TwoPcOverPbftReward twoPcReward = TwoPcOverPbftReward.newBuilder()
                    .setEpisode(pendingRewardEpisode)
                    .setReport(pendingRewardReport)
                    .setTimeoutUsed(TwoPcOverPbftTimeout.newBuilder()
                            .setElectionTimeoutMilliseconds(Math.max(0, pendingRewardTimeoutMs))
                            .setPrepareTimeoutMilliseconds(Math.max(0, prepareTimeoutMs))
                            .build())
                    .build();
            Reward reward = Reward.newBuilder()
                    .setTwoPcOverPbft(twoPcReward)
                    .build();
            localBuilder.setReward(reward);
        }

        try {
            learnerStub.sendReport(localBuilder.build());
            if (pendingRewardReport != null) {
                pendingRewardReport = null;
            }
        } catch (Exception e) {
            logger.warn("Exception in sending report to agent: {}", e.getMessage());
        }
    }

    private void maybeStartTimeoutPolling(int episode) {
        if (!learning || learnerStub == null) {
            return;
        }
        synchronized (pollerLock) {
            if (pollerEpisode == episode
                    && timeoutPollerThread != null
                    && timeoutPollerThread.isAlive()) {
                return;
            }
            pollerStopRequested = true;
            if (timeoutPollerThread != null) {
                timeoutPollerThread.interrupt();
            }
            pollerStopRequested = false;
            pollerDecision = null;
            pollerEpisode = episode;
            timeoutPollerThread = new Thread(() -> pollForTimeout(episode));
            timeoutPollerThread.setDaemon(true);
            timeoutPollerThread.start();
        }
    }

    private void stopTimeoutPolling() {
        synchronized (pollerLock) {
            pollerStopRequested = true;
            if (timeoutPollerThread != null) {
                timeoutPollerThread.interrupt();
            }
        }
    }

    private void pollForTimeout(int episode) {
        if (!learning) {
            return;
        }
        TimeoutRequest request = TimeoutRequest.newBuilder()
                .setEpisode(episode)
                .setProtocol(Protocol.PROTOCOL_TWO_PC_OVER_PBFT)
                .build();
        while (true) {
            if (pollerStopRequested || pollerEpisode != episode) {
                return;
            }
            try {
                TimeoutStatus status = learnerStub.getTimeout(request);
                if (status.getStatus() == TimeoutStatus.Status.READY
                        && status.hasTimeout()
                        && status.getTimeout().hasTwoPcOverPbft()) {
                    int timeoutMs = (int) status.getTimeout().getTwoPcOverPbft()
                            .getElectionTimeoutMilliseconds();
                    int startTick = (int) status.getStartTick();
                    int reportSeq = (int) status.getReportSeq();
                    int reportLength = reportSeq - startTick;
                    if (reportSeq > startTick && reportLength > 0
                            && !pollerStopRequested && pollerEpisode == episode) {
                        pollerDecision = new TimeoutDecision(timeoutMs, startTick, reportSeq, reportLength);
                        logger.info(
                                "[learning] timeout READY: episode={} start_tick={} report_seq={} report_length={} timeout_ms={}",
                                episode,
                                startTick,
                                reportSeq,
                                reportLength,
                                timeoutMs
                        );
                        return;
                    }
                }
            } catch (Exception e) {
                logger.warn("Exception while polling timeout: {}", e.getMessage());
            }
            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void captureReward(int episode) {
        if (!learning) {
            return;
        }
        TwoPcOverPbftReport rewardReport = buildReportFromStorage();
        if (rewardReport == null) {
            return;
        }
        pendingRewardReport = rewardReport;
        pendingRewardEpisode = episode;
        pendingRewardTimeoutMs = lastTimeoutUsedMs;
    }

    private void resetWindowMetrics() {
        learningMetrics.reset();
        singleShardTransactionCount = 0;
        ShardHandler shardHandler = replica != null ? replica.getShardHandler() : null;
        if (shardHandler != null) {
            shardHandler.resetLearningWindowMetrics();
        }
    }

    private void recordParticipantLockWait(long durationNs) {
        ShardHandler shardHandler = replica != null ? replica.getShardHandler() : null;
        if (shardHandler != null) {
            shardHandler.recordParticipantLockWaitNanos(durationNs);
        }
    }

    private void recordParticipantLockContention() {
        ShardHandler shardHandler = replica != null ? replica.getShardHandler() : null;
        if (shardHandler != null) {
            shardHandler.recordParticipantLockContention();
        }
    }

    private void recordParticipantLockHold(PendingTransaction pending) {
        if (pending == null) {
            return;
        }
        long holdMs = System.currentTimeMillis() - pending.prepareTime;
        if (holdMs <= 0) {
            return;
        }
        ShardHandler shardHandler = replica != null ? replica.getShardHandler() : null;
        if (shardHandler != null) {
            shardHandler.recordParticipantLockHoldMillis(holdMs);
        }
    }

    private int getPrepareTimeoutMs() {
        ShardHandler shardHandler = replica != null ? replica.getShardHandler() : null;
        if (shardHandler == null) {
            return 0;
        }
        return Math.max(0, shardHandler.getTimeoutMs());
    }
}
