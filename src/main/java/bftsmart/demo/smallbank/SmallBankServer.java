package bftsmart.demo.smallbank;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import bftsmart.demo.util.TimeoutLearningWindowMetrics;
import bftsmart.rlrpc.LearningAgentGrpc;
import bftsmart.rlrpc.Report;
import bftsmart.rlrpc.ReportLocal;
import bftsmart.rlrpc.Reward;
import bftsmart.rlrpc.TimeoutRequest;
import bftsmart.rlrpc.TimeoutStatus;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import bftsmart.tom.util.FailureInjectionCliArgs;
import bftsmart.tom.util.FailureInjectionController;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class SmallBankServer extends DefaultRecoverable {
    private static final boolean _debug = false;
    private static final int EPISODE_LENGTH = 1000;
    private static final int REPORT_TRIGGER_OFFSET = 499;
    private static final int APPLY_TRIGGER_OFFSET = 799;
    private static final int EPISODE_END_OFFSET = 999;
    private static final int POLL_INTERVAL_MS = 50;
    private HashMap<Long, String> accounts;
    private HashMap<Long, Double> checking;
    private HashMap<Long, Double> savings;

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
    private volatile Integer pollerRecommendationMs = null;
    private volatile int pollerEpisode = -1;
    private int currentTimeoutMs;
    private int lastTimeoutUsedMs;
    private Report pendingRewardReport;
    private int pendingRewardEpisode;
    private int pendingRewardTimeoutMs;
    private final boolean learning;

    public static void main(String[] args) throws Exception {
        ParsedArgs parsedArgs = parseArgs(args);
        configureFailureInjection(parsedArgs.failureArgs);
        List<String> positionalArgs = parsedArgs.failureArgs.getPositionalArgs();

        if (positionalArgs.size() == 1) {
            int replicaId = Integer.parseInt(positionalArgs.get(0));
            if (parsedArgs.configDir == null || parsedArgs.configDir.trim().isEmpty()) {
                new SmallBankServer(replicaId, null, parsedArgs.learning);
            } else {
                new SmallBankServer(replicaId, parsedArgs.configDir,
                        parsedArgs.learning);
            }
        } else {
            System.out.println("Usage: java ... SmallBankServer <replica_id> [--config-dir <path>] "
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

    private SmallBankServer(int id) {
        this(id, null, false);
    }

    private SmallBankServer(int id, String configHome) {
        this(id, configHome, false);
    }

    private SmallBankServer(int id, String configHome, boolean learning) {
        this.learning = learning;
        this.accounts = new HashMap<>();
        this.checking = new HashMap<>();
        this.savings = new HashMap<>();
        this.learningMetrics = new TimeoutLearningWindowMetrics(EPISODE_LENGTH);
        if (configHome == null) {
            replica = new ServiceReplica(id, this, this);
        } else {
            replica = new ServiceReplica(id, configHome, this, this, null, null, null);
        }
        if (this.learning) {
            initLearningAgentClient();
        } else {
            System.out.println("Learning-agent timeout recommendation is disabled.");
        }
        this.currentTimeoutMs = replica.getReplicaContext().getStaticConfiguration().getRequestTimeout();
        this.lastTimeoutUsedMs = currentTimeoutMs;
    }

    @Override
    public byte[][] appExecuteBatch(byte[][] commands, MessageContext[] msgCtx, boolean fromConsensus) {
        byte[][] replies = new byte[commands.length][];
        Map<Integer, Integer> batchSizesByConsensus = buildBatchSizesByConsensus(msgCtx);
        int index = 0;
        for (byte[] command : commands) {
            MessageContext currentMsgCtx = (msgCtx != null) ? msgCtx[index] : null;
            if (currentMsgCtx != null && currentMsgCtx.getConsensusId() % 1000 == 0 && !logPrinted) {
                System.out.println("SmallBankServer executing CID: " + currentMsgCtx.getConsensusId());
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
                    int batchSize = batchSizesByConsensus.getOrDefault(consensusId, 1);
                    learningMetrics.recordConsensus(currentMsgCtx, batchSize, currentTimeoutMs);

                    if (offsetInEpisode == REPORT_TRIGGER_OFFSET) {
                        if (learning) {
                            sendStateReport(episode);
                            startTimeoutPolling(episode);
                        }
                    } else if (offsetInEpisode == APPLY_TRIGGER_OFFSET) {
                        applyTimeoutIfReady(episode);
                        learningMetrics.reset();
                    } else if (offsetInEpisode == EPISODE_END_OFFSET) {
                        captureReward(episode);
                        learningMetrics.reset();
                    }
                }
            }

            SmallBankMessage request = SmallBankMessage.getObject(command);
            SmallBankMessage reply = SmallBankMessage.newErrorMessage(
                    "Unknown error",
                    SmallBankMessage.StatusCode.SYSTEM_ERROR);

            if (request == null) {
                replies[index] = reply.getBytes();
                continue;
            }

            if (_debug) {
                System.out.println("[INFO] Processing ordered request: " + request.getTxType());
            }

            try {
                switch (request.getTxType()) {
                    case CREATE_ACCOUNT: {
                        // System.out.println("[INFO] Creating account for " + request);
                        long custId = request.getCustomerId();
                        if (accounts.containsKey(custId)) {
                            reply = SmallBankMessage.newErrorMessage(
                                    "Account already exists",
                                    SmallBankMessage.StatusCode.BUSINESS_ERROR);
                        } else {
                            accounts.put(custId, request.getCustomerName());
                            checking.put(custId, request.getCheckingBalance());
                            savings.put(custId, request.getSavingsBalance());
                            reply = SmallBankMessage.newResponse(0);
                        }
                        break;
                    }

                    case DEPOSIT_CHECKING: {
                        long custId = request.getCustomerId();
                        if (!checking.containsKey(custId)) {
                            reply = SmallBankMessage.newErrorMessage(
                                    "Account not found",
                                    SmallBankMessage.StatusCode.BUSINESS_ERROR);
                        } else {
                            double balance = checking.get(custId) + request.getAmount();
                            checking.put(custId, balance);
                            reply = SmallBankMessage.newResponse(0);
                        }
                        break;
                    }

                    case TRANSACT_SAVINGS: {
                        long custId = request.getCustomerId();
                        if (!savings.containsKey(custId)) {
                            reply = SmallBankMessage.newErrorMessage(
                                    "Account not found",
                                    SmallBankMessage.StatusCode.BUSINESS_ERROR);
                        } else {
                            double balance = savings.get(custId) + request.getAmount();
                            if (balance < 0) {
                                reply = SmallBankMessage.newErrorMessage(
                                        "Insufficient funds",
                                        SmallBankMessage.StatusCode.INSUFFICIENT_FUNDS);
                            } else {
                                savings.put(custId, balance);
                                reply = SmallBankMessage.newResponse(0);
                            }
                        }
                        break;
                    }

                    case WRITE_CHECK: {
                        long custId = request.getCustomerId();
                        if (!checking.containsKey(custId)) {
                            reply = SmallBankMessage.newErrorMessage(
                                    "Account not found",
                                    SmallBankMessage.StatusCode.BUSINESS_ERROR);
                        } else {
                            double balance = checking.get(custId) - request.getAmount();
                            if (balance < 0) {
                                reply = SmallBankMessage.newErrorMessage(
                                        "Insufficient funds",
                                        SmallBankMessage.StatusCode.INSUFFICIENT_FUNDS);
                            } else {
                                checking.put(custId, balance);
                                reply = SmallBankMessage.newResponse(0);
                            }
                        }
                        break;
                    }

                    case SEND_PAYMENT: {
                        long srcId = request.getCustomerId();
                        long destId = request.getDestCustomerId();

                        if (!checking.containsKey(srcId)) {
                            reply = SmallBankMessage.newErrorMessage(
                                    "Source account not found",
                                    SmallBankMessage.StatusCode.BUSINESS_ERROR);
                        } else if (!checking.containsKey(destId)) {
                            reply = SmallBankMessage.newErrorMessage(
                                    "Destination account not found",
                                    SmallBankMessage.StatusCode.BUSINESS_ERROR);
                        } else {
                            double srcBalance = checking.get(srcId) - request.getAmount();
                            if (srcBalance < 0) {
                                reply = SmallBankMessage.newErrorMessage(
                                        "Insufficient funds",
                                        SmallBankMessage.StatusCode.INSUFFICIENT_FUNDS);
                            } else {
                                double destBalance = checking.get(destId) + request.getAmount();
                                checking.put(srcId, srcBalance);
                                checking.put(destId, destBalance);
                                reply = SmallBankMessage.newResponse(0);
                            }
                        }
                        break;
                    }

                    case AMALGAMATE: {
                        long custId1 = request.getCustomerId();
                        long custId2 = request.getDestCustomerId();

                        if (!checking.containsKey(custId1) || !savings.containsKey(custId1)) {
                            reply = SmallBankMessage.newErrorMessage(
                                    "Account 1 not found",
                                    SmallBankMessage.StatusCode.BUSINESS_ERROR);
                        } else if (!checking.containsKey(custId2) || !savings.containsKey(custId2)) {
                            reply = SmallBankMessage.newErrorMessage(
                                    "Account 2 not found",
                                    SmallBankMessage.StatusCode.BUSINESS_ERROR);
                        } else {
                            // Transfer all from custId2's checking to custId1's savings
                            double amountToTransfer = checking.get(custId2);
                            checking.put(custId2, 0.0);
                            savings.put(custId1, savings.get(custId1) + amountToTransfer);
                            reply = SmallBankMessage.newResponse(0);
                        }
                        break;
                    }

                    default:
                        reply = SmallBankMessage.newErrorMessage(
                                "Unknown operation type",
                                SmallBankMessage.StatusCode.BUSINESS_ERROR);
                        break;
                }
            } catch (Exception e) {
                reply = SmallBankMessage.newErrorMessage(
                        "Exception: " + e.getMessage(),
                        SmallBankMessage.StatusCode.SYSTEM_ERROR);
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
        SmallBankMessage request = SmallBankMessage.getObject(command);
        SmallBankMessage reply = SmallBankMessage.newErrorMessage(
                "Unknown error",
                SmallBankMessage.StatusCode.SYSTEM_ERROR);

        if (request == null) {
            return reply.getBytes();
        }

        switch (request.getTxType()) {
            case BALANCE:
                long custId = request.getCustomerId();
                if (!checking.containsKey(custId) || !savings.containsKey(custId)) {
                    reply = SmallBankMessage.newErrorMessage(
                            "Account not found",
                            SmallBankMessage.StatusCode.BUSINESS_ERROR);
                } else {
                    double checkingBalance = checking.get(custId);
                    double savingsBalance = savings.get(custId);
                    reply = SmallBankMessage.newResponseWithBalances(0, checkingBalance, savingsBalance);
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

    private void initLearningAgentClient() {
        if (!learning) {
            return;
        }
        int replicaId = replica.getReplicaContext().getStaticConfiguration().getProcessId();
        String host = replica.getReplicaContext().getStaticConfiguration().getHost(replicaId);
        int port = replica.getReplicaContext().getStaticConfiguration().getLearnerPort(replicaId);
        if (port <= 0) {
            System.out.println("Learner port not configured for replica " + replicaId + ". Reports will not be sent.");
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
    }

    private Report buildReportFromStorage() {
        return learningMetrics.buildReport();
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
        ReportLocal.Builder localBuilder = ReportLocal.newBuilder()
                .setNodeId(replica.getId())
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
            System.out.println("Exception in sending report to agent: " + e.getMessage());
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
                // System.out.println("Exception while polling timeout: " + e.getMessage());
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
