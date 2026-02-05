package bftsmart.demo.dataexchange;

import bftsmart.rlrpc.DataExchangeGrpc;
import bftsmart.rlrpc.Report;
import bftsmart.rlrpc.ReportShared;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class DataExchangeClient {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: demo.dataexchange.DataExchangeClient <host> <port> [episode] [reports]");
            System.exit(-1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int episode = (args.length >= 3) ? Integer.parseInt(args[2]) : ThreadLocalRandom.current().nextInt(1_000_000);
        int reports = (args.length >= 4) ? Integer.parseInt(args[3]) : 3;

        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        try {
            DataExchangeGrpc.DataExchangeBlockingStub stub = DataExchangeGrpc.newBlockingStub(channel);
            ReportShared request = buildReportShared(episode, reports);
            Empty resp = stub.exchangeData(request);
            System.out.println("Sent episode " + episode + " with " + reports + " reports. Response: " + resp);
        } finally {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static ReportShared buildReportShared(int episode, int reports) {
        ReportShared.Builder builder = ReportShared.newBuilder()
                .setEpisode(episode);

        for (int i = 0; i < reports; i++) {
            builder.addReports(randomReport());
        }
        return builder.build();
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
