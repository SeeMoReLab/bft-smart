package bftsmart.demo.dataexchange;

import bftsmart.rlrpc.Report;
import bftsmart.rlrpc.ReportBatch;
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
                .setEpisode(episode);

        for (int i = 0; i < reports; i++) {
            builder.addReports(buildReportLocal(episode, i + 1));
        }
        return builder.build();
    }

    private static ReportLocal buildReportLocal(int episode, int nodeId) {
        return ReportLocal.newBuilder()
                .setNodeId(nodeId)
                .setEpisode(episode)
                .setState(randomReport())
                .build();
    }

    private static Report randomReport() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        return Report.newBuilder()
                .setProcessedTransactions(rnd.nextInt(1, 10_000))
                .setAvgMessageDelay(rnd.nextFloat() * 100)
                .setMaxMessageDelay(rnd.nextFloat() * 200)
                .setMinMessageDelay(rnd.nextFloat() * 10)
                .setStdMessageDelay(rnd.nextFloat() * 50)
                .build();
    }
}
