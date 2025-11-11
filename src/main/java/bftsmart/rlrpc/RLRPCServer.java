package bftsmart.rlrpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.tom.core.TOMLayer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RLRPCServer {
    private static final Logger logger = LoggerFactory.getLogger(RLRPCServer.class);
    
    private Server server;
    private final int port;
    private final int replicaId;
    private TOMLayer tomLayer;
    
    public RLRPCServer(int replicaId, int port) {
        this.replicaId = replicaId;
        this.port = port;
    }
    
    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new BFTSmartRLRPCServiceImpl(this.tomLayer))
                .build()
                .start();
        System.out.println("gRPC starting");
        logger.info("BFT-SMaRt RLRPC Server [Replica {}] started on port {}", replicaId, port);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down BFT-SMaRt RLRPC server");
            try {
                RLRPCServer.this.stop();
            } catch (InterruptedException e) {
                logger.error("Error during shutdown", e);
            }
        }));
    }
    
    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }
    
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public void setTomLayer(TOMLayer tomLayer) {
        System.out.println("Setting TOMLayer");
        this.tomLayer = tomLayer;
        System.out.println("Done setting TOMLayer");
    }
    
    /**
     * Service implementation integrated with BFT-SMaRt
     */
    class BFTSmartRLRPCServiceImpl extends RLRPCGrpc.RLRPCImplBase {
        private TOMLayer tomLayer;
        public BFTSmartRLRPCServiceImpl(TOMLayer tomLayer) {
            this.tomLayer = tomLayer;
        }

        @Override
        public void setTimer(SetTimerRequest request, StreamObserver<SetTimerResponse> responseObserver) {
            long timerValue = request.getTimer();
            logger.info("[Replica {}] Received SetTimer request: {}", replicaId, timerValue);
            
            try {
                // Update the timer value
                if (this.tomLayer == null) {
                    logger.error("TOMLayer of BFTSmartRLRPCServiceImpl is null");
                    throw new RuntimeException("TOMLayer of BFTSmartRLRPCServiceImpl is null");
                }
                
                this.tomLayer.requestsTimer.setShortTimeout(request.getTimer());
                
                System.out.println("[Replica " + replicaId + "] Timer updated to: " + timerValue);
                
                // Build response
                SetTimerResponse response = SetTimerResponse.newBuilder()
                        .setTimer(this.tomLayer.requestsTimer.getTimeout())
                        .build();
                
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                
            } catch (Exception e) {
                logger.error("Error processing SetTimer", e);
                responseObserver.onError(e);
            }
        }
    }
}
