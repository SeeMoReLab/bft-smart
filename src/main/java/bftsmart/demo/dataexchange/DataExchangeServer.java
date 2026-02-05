package bftsmart.demo.dataexchange;

import bftsmart.rlrpc.DataExchangeGrpc;
import bftsmart.rlrpc.ReportShared;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import com.google.protobuf.Empty;
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

    private volatile int lastEpisode = -1;
    private volatile long totalReports = 0;

    public DataExchangeServer(int replicaId, int grpcPort) throws IOException {
        this.replica = new ServiceReplica(replicaId, this, this);
        this.senderId = EPISODE_CLIENT_ID_BASE + replicaId;

        if (!replica.getReplicaContext().getStaticConfiguration().useEpisodeRequests()) {
            logger.warn("system.optimizations.episode_requests is disabled. Episode-based dedupe will not be active.");
        }

        if (replica.getReplicaContext().getStaticConfiguration().getUseSignatures() == 1) {
            logger.warn("system.communication.useSignatures=1 is enabled. Episode requests are not signed and may be rejected.");
        }

        this.grpcServer = ServerBuilder.forPort(grpcPort)
                .addService(new DataExchangeService(replica, senderId, sequence))
                .build()
                .start();

        logger.info("DataExchange gRPC server started on port {} (replica {})", grpcPort, replicaId);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: demo.dataexchange.DataExchangeServer <replica_id> <grpc_port>");
            System.exit(-1);
        }

        int replicaId = Integer.parseInt(args[0]);
        int grpcPort = Integer.parseInt(args[1]);
        new DataExchangeServer(replicaId, grpcPort);
    }

    private void shutdown() throws InterruptedException {
        grpcServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Override
    public byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx) {
        try {
            ReportShared shared = ReportShared.parseFrom(command);
            logger.info("Executing ordered ReportShared episode {} with {} reports: {}",
                    shared.getEpisode(), shared.getReportsCount(), shared);
            lastEpisode = shared.getEpisode();
            totalReports += shared.getReportsCount();
            logger.info("Ordered episode {} with {} reports (totalReports={})",
                    shared.getEpisode(), shared.getReportsCount(), totalReports);
        } catch (Exception e) {
            logger.error("Failed to parse ReportShared", e);
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

    private static final class DataExchangeService extends DataExchangeGrpc.DataExchangeImplBase {
        private final ServiceReplica replica;
        private final int senderId;
        private final AtomicInteger sequence;

        private DataExchangeService(ServiceReplica replica, int senderId, AtomicInteger sequence) {
            this.replica = replica;
            this.senderId = senderId;
            this.sequence = sequence;
        }

        @Override
        public void exchangeData(ReportShared request, StreamObserver<Empty> responseObserver) {
            try {
                logger.info("Received gRPC ReportShared episode {} with {} reports: {}",
                        request.getEpisode(), request.getReportsCount(), request);
                TOMMessage message = buildMessage(request);
                replica.submitClientRequest(message);
                responseObserver.onNext(Empty.getDefaultInstance());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }

        private TOMMessage buildMessage(ReportShared request) {
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
}
