package bftsmart.demo.dataexchange;

import bftsmart.rlrpc.ConsensusGrpc;
import bftsmart.rlrpc.LearningAgentGrpc;
import bftsmart.rlrpc.ReportBatch;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataExchangeServer extends DefaultSingleRecoverable {

    private static final Logger logger = LoggerFactory.getLogger(DataExchangeServer.class);
    private static final int EPISODE_CLIENT_ID_BASE = 10000;

    private final ServiceReplica replica;
    private final Server grpcServer;
    private final AtomicInteger sequence = new AtomicInteger(0);
    private final int senderId;
    private final ManagedChannel learnerChannel;
    private final LearningAgentGrpc.LearningAgentBlockingStub learnerStub;
    private final String learnerHost;
    private final int learnerPort;
    private final int dataExchangePort;

    private volatile int lastEpisode = -1;
    private volatile long totalReports = 0;

    public DataExchangeServer(int replicaId) throws IOException {
        this.replica = new ServiceReplica(replicaId, this, this);
        this.senderId = EPISODE_CLIENT_ID_BASE + replicaId;
        this.learnerHost = replica.getReplicaContext().getStaticConfiguration().getHost(replicaId);
        this.learnerPort = replica.getReplicaContext().getStaticConfiguration().getLearnerPort(replicaId);
        this.dataExchangePort = replica.getReplicaContext().getStaticConfiguration().getDataExchangePort(replicaId);

        if (dataExchangePort <= 0) {
            throw new IllegalStateException(
                    "Data exchange port not configured for replica " + replicaId + ". Cannot start DataExchange gRPC server.");
        }

        if (learnerPort > 0) {
            this.learnerChannel = ManagedChannelBuilder.forAddress(learnerHost, learnerPort)
                    .usePlaintext()
                    .build();
            this.learnerStub = LearningAgentGrpc.newBlockingStub(learnerChannel);
            logger.info("LearningAgent client configured for {}:{}", learnerHost, learnerPort);
        } else {
            this.learnerChannel = null;
            this.learnerStub = null;
            logger.warn("Learner port not configured for replica {}. DeliverConsensus will be skipped.", replicaId);
        }

        if (!replica.getReplicaContext().getStaticConfiguration().useEpisodeRequests()) {
            logger.warn("system.optimizations.episode_requests is disabled. Episode-based dedupe will not be active.");
        }

        if (replica.getReplicaContext().getStaticConfiguration().getUseSignatures() == 1) {
            logger.warn(
                    "system.communication.useSignatures=1 is enabled. Episode requests are not signed and may be rejected.");
        }

        this.grpcServer = ServerBuilder.forPort(dataExchangePort)
                .addService(new ConsensusService(replica, senderId, sequence))
                .build()
                .start();

        logger.info("DataExchange gRPC server started on port {} (replica {})", dataExchangePort, replicaId);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: demo.dataexchange.DataExchangeServer <replica_id>");
            System.exit(-1);
        }

        int replicaId = Integer.parseInt(args[0]);
        new DataExchangeServer(replicaId);
    }

    private void shutdown() throws InterruptedException {
        grpcServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        if (learnerChannel != null) {
            learnerChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Override
    public byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx) {
        try {
            ReportBatch batch = ReportBatch.parseFrom(command);
            logger.info("Executing ordered ReportBatch episode {} with {} reports: {}",
                    batch.getEpisode(), batch.getReportsCount(), batch);
            lastEpisode = batch.getEpisode();
            totalReports += batch.getReportsCount();
            logger.info("Ordered episode {} with {} reports (totalReports={})",
                    batch.getEpisode(), batch.getReportsCount(), totalReports);
            deliverConsensus(batch);
        } catch (Exception e) {
            logger.error("Failed to parse ReportBatch", e);
        }
        return new byte[0];
    }

    @Override
    public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
        return new byte[0];
    }

    @Override
    public byte[] getSnapshot() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(lastEpisode);
            dos.writeLong(totalReports);
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            logger.error("Failed to serialize snapshot", e);
            return new byte[0];
        }
    }

    @Override
    public void installSnapshot(byte[] state) {
        if (state == null || state.length == 0) {
            return;
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(state);
                DataInputStream dis = new DataInputStream(bais)) {
            lastEpisode = dis.readInt();
            totalReports = dis.readLong();
        } catch (IOException e) {
            logger.error("Failed to install snapshot", e);
        }
    }

    private static final class ConsensusService extends ConsensusGrpc.ConsensusImplBase {
        private final ServiceReplica replica;
        private final int senderId;
        private final AtomicInteger sequence;

        private ConsensusService(ServiceReplica replica, int senderId, AtomicInteger sequence) {
            this.replica = replica;
            this.senderId = senderId;
            this.sequence = sequence;
        }

        @Override
        public void submitReportBatch(ReportBatch request, StreamObserver<Empty> responseObserver) {
            try {
                logger.info("Received gRPC ReportBatch episode {} with {} reports: {}",
                        request.getEpisode(), request.getReportsCount(), request);
                TOMMessage message = buildMessage(request);
                replica.submitClientRequest(message);
                responseObserver.onNext(Empty.getDefaultInstance());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }

        private TOMMessage buildMessage(ReportBatch request) {
            int seq = sequence.getAndIncrement();
            int session = 0;
            int episode = request.getEpisode();
            int viewId = replica.getReplicaContext().getSVController().getCurrentViewId();
            TOMMessage message = new TOMMessage(senderId, session, seq, episode,
                    request.toByteArray(), viewId, TOMMessageType.ORDERED_REQUEST);
            message.serializedMessage = TOMMessage.messageToBytes(message);
            return message;
        }
    }

    private void deliverConsensus(ReportBatch batch) {
        if (learnerStub == null) {
            return;
        }
        try {
            learnerStub.deliverConsensus(batch);
            logger.info("Delivered consensus ReportBatch episode {} to learner {}:{}",
                    batch.getEpisode(), learnerHost, learnerPort);
        } catch (Exception e) {
            logger.warn("Failed to deliver consensus ReportBatch episode {} to learner {}:{}",
                    batch.getEpisode(), learnerHost, learnerPort, e);
        }
    }
}
