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

import bftsmart.rlrpc.LearningAgentGrpc;
import bftsmart.rlrpc.Report;
import bftsmart.rlrpc.ReportLocal;
import bftsmart.rlrpc.Reward;
import bftsmart.rlrpc.TimeoutRequest;
import bftsmart.rlrpc.TimeoutStatus;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import bftsmart.tom.util.FailureInjectionCliArgs;
import bftsmart.tom.util.FailureInjectionController;
import bftsmart.tom.util.Storage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class SmallBankServer2PC extends DefaultRecoverable {
    private static final Logger logger = LoggerFactory.getLogger(SmallBankServer2PC.class);
    private static final boolean _debug = false;
    private static final int EPISODE_LENGTH = 1000;
    private static final int REPORT_TRIGGER_OFFSET = 499;
    private static final int APPLY_TRIGGER_OFFSET = 799;
    private static final int EPISODE_END_OFFSET = 999;
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
    private Storage consensusLatency;
    // Legacy snapshot field kept for backward compatibility.
    private long iterations = 0;
    private int lastProcessedConsensusId = -1;
    private ServiceReplica replica;
    private ManagedChannel learnerChannel;
    private LearningAgentGrpc.LearningAgentBlockingStub learnerStub;
    private final Object pollerLock = new Object();
    private Thread timeoutPollerThread;
    private volatile boolean pollerStopRequested = false;
    private volatile Integer pollerRecommendationMs = null;
    private volatile int pollerEpisode = -1;
    private int currentTimeoutMs = -1;
    private int lastTimeoutUsedMs = -1;
    private Report pendingRewardReport;
    private int pendingRewardEpisode;
    private int pendingRewardTimeoutMs;
    private final boolean learning;

    // Shard configuration (for logging/debugging)
    private ReplicaContext replicaContext;
    private int myReplicaId;
    private int myShardId;
    private int totalShards;

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
        this.consensusLatency = new Storage(EPISODE_LENGTH);

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
                    int offsetInEpisode = Math.floorMod(consensusId, EPISODE_LENGTH);
                    int episode = Math.floorDiv(consensusId, EPISODE_LENGTH) + 1;

                    if (currentMsgCtx.getFirstInBatch() != null) {
                        consensusLatency.store(currentMsgCtx.getFirstInBatch().decisionTime
                                - currentMsgCtx.getFirstInBatch().consensusStartTime);
                    }
                    if (offsetInEpisode == REPORT_TRIGGER_OFFSET) {
                        if (learning) {
                            sendStateReport(episode);
                            startTimeoutPolling(episode);
                        }
                    } else if (offsetInEpisode == APPLY_TRIGGER_OFFSET) {
                        applyTimeoutIfReady(episode);
                        consensusLatency.reset();
                    } else if (offsetInEpisode == EPISODE_END_OFFSET) {
                        captureReward(episode);
                        consensusLatency.reset();
                    }
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
        if (!tryAcquireLocks(accountsToLock)) {
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

    private Report buildReportFromStorage() {
        int sampleCount = consensusLatency.getCount();
        if (sampleCount == 0) {
            return null;
        }
        return Report.newBuilder()
                .setProcessedTransactions(sampleCount)
                .setAvgMessageDelay((float) (consensusLatency.getAverage(false) / 1_000_000.0))
                .setMaxMessageDelay((float) (consensusLatency.getMax(false) / 1_000_000.0))
                .setMinMessageDelay((float) (consensusLatency.getMin(false) / 1_000_000.0))
                .setStdMessageDelay((float) (consensusLatency.getDP(true) / 1_000_000.0))
                .build();
    }

    private void sendStateReport(int episode) {
        if (!learning) {
            return;
        }
        if (learnerStub == null) {
            return;
        }
        Report report = buildReportFromStorage();
        if (report == null) {
            return;
        }
        int nodeId = (myReplicaId >= 0) ? myReplicaId : replica.getId();
        ReportLocal.Builder localBuilder = ReportLocal.newBuilder()
                .setNodeId(nodeId)
                .setEpisode(episode)
                .setState(report);

        if (pendingRewardReport != null) {
            Reward reward = Reward.newBuilder()
                    .setEpisode(pendingRewardEpisode)
                    .setReport(pendingRewardReport)
                    .setTimeoutMillisecondsUsed(pendingRewardTimeoutMs)
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

    private void startTimeoutPolling(int episode) {
        if (!learning) {
            return;
        }
        if (learnerStub == null) {
            return;
        }
        synchronized (pollerLock) {
            pollerStopRequested = true;
            if (timeoutPollerThread != null) {
                timeoutPollerThread.interrupt();
            }
            pollerStopRequested = false;
            pollerRecommendationMs = null;
            pollerEpisode = episode;
            timeoutPollerThread = new Thread(() -> pollForTimeout(episode));
            timeoutPollerThread.setDaemon(true);
            timeoutPollerThread.start();
        }
    }

    private void pollForTimeout(int episode) {
        if (!learning) {
            return;
        }
        TimeoutRequest request = TimeoutRequest.newBuilder()
                .setEpisode(episode)
                .build();
        while (true) {
            if (pollerStopRequested || pollerEpisode != episode) {
                return;
            }
            try {
                TimeoutStatus status = learnerStub.getTimeout(request);
                if (status.getStatus() == TimeoutStatus.Status.READY && status.hasTimeout()) {
                    if (!pollerStopRequested && pollerEpisode == episode) {
                        pollerRecommendationMs = (int) status.getTimeout().getTimeoutMilliseconds();
                    }
                    return;
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

    private void applyTimeoutIfReady(int episode) {
        if (!learning) {
            lastTimeoutUsedMs = currentTimeoutMs;
            return;
        }
        synchronized (pollerLock) {
            pollerStopRequested = true;
            if (timeoutPollerThread != null) {
                timeoutPollerThread.interrupt();
            }
        }
        Integer recommendation = pollerRecommendationMs;
        if (recommendation != null && pollerEpisode == episode) {
            currentTimeoutMs = recommendation;
            replica.getRequestsTimer().setShortTimeout(currentTimeoutMs);
        }
        lastTimeoutUsedMs = currentTimeoutMs;
    }

    private void captureReward(int episode) {
        if (!learning) {
            return;
        }
        Report rewardReport = buildReportFromStorage();
        if (rewardReport == null) {
            return;
        }
        pendingRewardReport = rewardReport;
        pendingRewardEpisode = episode;
        pendingRewardTimeoutMs = lastTimeoutUsedMs;
    }
}
