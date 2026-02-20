package bftsmart.demo.dataexchange;

import bftsmart.rlrpc.ReportBatch;
import bftsmart.rlrpc.PbftReport;
import bftsmart.rlrpc.Protocol;
import bftsmart.rlrpc.ReportLocal;
import bftsmart.rlrpc.ConsensusGrpc;
import com.google.protobuf.Empty;
import bftsmart.reconfiguration.util.HostsConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class DataExchangeClient {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: demo.dataexchange.DataExchangeClient <node_id> [episode] [reports]");
            System.exit(-1);
        }

        int nodeId = Integer.parseInt(args[0]);
        HostsConfig hosts = new HostsConfig("", "");
        String host = hosts.getHost(nodeId);
        int port = hosts.getDataExchangePort(nodeId);
        if (host == null || port <= 0) {
            System.out.println("Data exchange host/port not configured for node " + nodeId + " in config/hosts.config");
            System.exit(-1);
        }

        int episode = (args.length >= 2) ? Integer.parseInt(args[1]) : ThreadLocalRandom.current().nextInt(1_000_000);
        int reports = (args.length >= 3) ? Integer.parseInt(args[2]) : 3;

        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        try {
            ConsensusGrpc.ConsensusBlockingStub stub = ConsensusGrpc.newBlockingStub(channel);
            ReportBatch request = buildReportBatch(episode, reports);
            Empty resp = stub.submitReportBatch(request);
            System.out.println("Sent episode " + episode + " with " + reports + " reports. Response: " + resp);
        } finally {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static ReportBatch buildReportBatch(int episode, int reports) {
        ReportBatch.Builder builder = ReportBatch.newBuilder()
                .setEpisode(episode)
                .setProtocol(Protocol.PROTOCOL_PBFT);

        for (int i = 0; i < reports; i++) {
            builder.addReports(buildReportLocal(episode, i + 1));
        }
        return builder.build();
    }

    private static ReportLocal buildReportLocal(int episode, int nodeId) {
        return ReportLocal.newBuilder()
                .setNodeId(nodeId)
                .setEpisode(episode)
                .setProtocol(Protocol.PROTOCOL_PBFT)
                .setPbftState(randomReport())
                .build();
    }

    private static PbftReport randomReport() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        int tx = rnd.nextInt(1, 10_000);
        int consensus = Math.max(1, tx / 10);
        return PbftReport.newBuilder()
                .setTotalTransactions(tx)
                .setTotalConsensusInstances(consensus)
                .setAvgConsensusLatencyMs(rnd.nextFloat() * 50)
                .setP95ConsensusLatencyMs(rnd.nextFloat() * 75)
                .setP99ConsensusLatencyMs(rnd.nextFloat() * 100)
                .setThroughputTps(rnd.nextFloat() * 5_000)
                .setTimeoutViolationRate(rnd.nextFloat())
                .setAvgBatchSize((float) tx / consensus)
                .setP95BatchSize(rnd.nextFloat() * 32)
                .build();
    }
}
