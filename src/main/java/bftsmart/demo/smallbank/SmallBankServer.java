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
import bftsmart.rlrpc.PbftReport;
import bftsmart.rlrpc.PbftReward;
import bftsmart.rlrpc.PbftTimeout;
import bftsmart.rlrpc.Protocol;
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
    private static final int REPORT_TICK_INTERVAL = 10;
    private static final long REPORT_TRIGGER_ELAPSED_MS = 5000L;
    private static final int MAX_REPORT_LENGTH = 500;
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
    private String learnerHost;
    private int learnerPort = -1;
    private final Object pollerLock = new Object();
    private Thread timeoutPollerThread;
    private volatile boolean pollerStopRequested = false;
    private volatile TimeoutDecision pollerDecision = null;
    private volatile int pollerEpisode = -1;
    private int currentTimeoutMs;
    private int lastTimeoutUsedMs;
    private PbftReport pendingRewardReport;
    private int pendingRewardEpisode;
    private int pendingRewardTimeoutMs;
    private final boolean learning;

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
        this.learningMetrics = new TimeoutLearningWindowMetrics(MAX_REPORT_LENGTH * 2);
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
                    int batchSize = batchSizesByConsensus.getOrDefault(consensusId, 1);
                    handleConsensusProgress(consensusId, currentMsgCtx, batchSize);
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
        learnerHost = replica.getReplicaContext().getStaticConfiguration().getHost(replicaId);
        learnerPort = replica.getReplicaContext().getStaticConfiguration().getLearnerPort(replicaId);
        if (learnerPort <= 0) {
            System.out.println("Learner port not configured for replica " + replicaId + ". Reports will not be sent.");
            return;
        }
        learnerChannel = ManagedChannelBuilder.forAddress(learnerHost, learnerPort)
                .usePlaintext()
                .build();
        learnerStub = LearningAgentGrpc.newBlockingStub(learnerChannel);
        System.out.println(
                "[learning] replica " + replicaId + " learner target " + learnerHost + ":" + learnerPort);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                learnerChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }

    private PbftReport buildReportFromStorage() {
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
        if (consensusId > selectedWindow.applyTick) {
            applyHandledForEpisode = true;
            lastTimeoutUsedMs = currentTimeoutMs;
            System.out.println(
                    "[learning] ignored late recommendation: episode=" + currentEpisode
                            + " report_seq=" + selectedWindow.reportSeq
                            + " apply_tick=" + selectedWindow.applyTick
                            + " current_cid=" + consensusId);
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
            if (consensusId == selectedWindow.applyTick && pollerDecision != null) {
                currentTimeoutMs = pollerDecision.timeoutMs;
                replica.getRequestsTimer().setShortTimeout(currentTimeoutMs);
            }
            applyHandledForEpisode = true;
            lastTimeoutUsedMs = currentTimeoutMs;
            return;
        }
        if (reachedReportCapForEpisode && capApplyDeadlineTick >= 0 && consensusId >= capApplyDeadlineTick) {
            waitingForRecommendation = false;
            applyHandledForEpisode = true;
            lastTimeoutUsedMs = currentTimeoutMs;
            stopTimeoutPolling();
        }
    }

    private void maybeHandleRewardDeadline(int consensusId) {
        if (rewardCapturedForEpisode) {
            return;
        }
        int rewardDeadline = -1;
        if (selectedWindow != null) {
            rewardDeadline = selectedWindow.rewardTick;
        } else if (reachedReportCapForEpisode && capRewardDeadlineTick >= 0) {
            rewardDeadline = capRewardDeadlineTick;
        }
        if (rewardDeadline < 0 || consensusId < rewardDeadline) {
            return;
        }
        captureReward(currentEpisode);
        rewardCapturedForEpisode = true;
        stopTimeoutPolling();
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
        learningMetrics.reset();
    }

    private void sendStateReport(int episode, int startTick, int reportSeq) {
        if (!learning) {
            return;
        }
        if (learnerStub == null) {
            System.out.println(
                    "[learning] episode " + episode + " skipped report: learner stub not initialized");
            return;
        }
        PbftReport report = buildReportFromStorage();
        if (report == null) {
            System.out.println(
                    "[learning] episode " + episode + " skipped report: metrics report unavailable");
            return;
        }
        ReportLocal.Builder localBuilder = ReportLocal.newBuilder()
                .setNodeId(replica.getId())
                .setEpisode(episode)
                .setProtocol(Protocol.PROTOCOL_PBFT)
                .setStartTick(startTick)
                .setReportSeq(reportSeq)
                .setPbftState(report);

        if (pendingRewardReport != null) {
            PbftReward pbftReward = PbftReward.newBuilder()
                    .setEpisode(pendingRewardEpisode)
                    .setReport(pendingRewardReport)
                    .setTimeoutUsed(PbftTimeout.newBuilder()
                            .setElectionTimeoutMilliseconds(Math.max(0, pendingRewardTimeoutMs))
                            .build())
                    .build();
            Reward reward = Reward.newBuilder()
                    .setPbft(pbftReward)
                    .build();
            localBuilder.setReward(reward);
        }

        try {
            learnerStub.sendReport(localBuilder.build());
            System.out.println(
                    "[learning] sent report: replica=" + replica.getId()
                            + " episode=" + episode
                            + " start_tick=" + startTick
                            + " report_seq=" + reportSeq
                            + " target=" + learnerHost + ":" + learnerPort);
            if (pendingRewardReport != null) {
                pendingRewardReport = null;
            }
        } catch (Exception e) {
            System.out.println(
                    "Exception in sending report to agent (" + learnerHost + ":" + learnerPort + "): "
                            + e.getMessage());
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
                .setProtocol(Protocol.PROTOCOL_PBFT)
                .build();
        while (true) {
            if (pollerStopRequested || pollerEpisode != episode) {
                return;
            }
            try {
                TimeoutStatus status = learnerStub.getTimeout(request);
                if (status.getStatus() == TimeoutStatus.Status.READY
                        && status.hasTimeout()
                        && status.getTimeout().hasPbft()) {
                    int timeoutMs = (int) status.getTimeout().getPbft()
                            .getElectionTimeoutMilliseconds();
                    int startTick = (int) status.getStartTick();
                    int reportSeq = (int) status.getReportSeq();
                    int reportLength = reportSeq - startTick;
                    if (reportSeq > startTick && reportLength > 0
                            && !pollerStopRequested && pollerEpisode == episode) {
                        pollerDecision = new TimeoutDecision(timeoutMs, startTick, reportSeq, reportLength);
                        return;
                    }
                }
            } catch (Exception e) {
                // Keep polling; intermittent RPC errors are expected.
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
        PbftReport rewardReport = buildReportFromStorage();
        if (rewardReport == null) {
            return;
        }
        pendingRewardReport = rewardReport;
        pendingRewardEpisode = episode;
        pendingRewardTimeoutMs = lastTimeoutUsedMs;
    }
}
