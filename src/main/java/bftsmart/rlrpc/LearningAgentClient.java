package bftsmart.rlrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import com.google.protobuf.Empty;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LearningAgentClient {
    private static final Logger logger = Logger.getLogger(LearningAgentClient.class.getName());
    
    private final ManagedChannel channel;
    private final LearningAgentGrpc.LearningAgentBlockingStub blockingStub;
    private final LearningAgentGrpc.LearningAgentStub asyncStub;

    /**
     * Construct client for accessing LearningAgent server at {host:port}.
     */
    public LearningAgentClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build());
    }

    /**
     * Construct client.
     */
    public LearningAgentClient(ManagedChannel channel) {
        this.channel = channel;
        this.blockingStub = LearningAgentGrpc.newBlockingStub(channel);
        this.asyncStub = LearningAgentGrpc.newStub(channel);
    }

    /**
     * Predict the next action for a given state.
     */
    public Prediction predict(State state) {
        logger.info("Requesting prediction for state: " + state);
        try {
            Prediction prediction = blockingStub.predict(state);
            logger.info("Received prediction: " + prediction);
            return prediction;
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            throw e;
        }
    }

    /**
     * Predict with custom state parameters.
     */
    public Prediction predict(int processedTransactions, 
                             float avgDelay, 
                             float maxDelay, 
                             float minDelay, 
                             float stdDelay) {
        State state = State.newBuilder()
                .setProcessedTransactions(processedTransactions)
                .setAvgMessageDelay(avgDelay)
                .setMaxMessageDelay(maxDelay)
                .setMinMessageDelay(minDelay)
                .setStdMessageDelay(stdDelay)
                .build();
        
        return predict(state);
    }

    /**
     * Send feedback to update the model.
     */
    public void learn(String predictionId, float reward) {
        Feedback feedback = Feedback.newBuilder()
                .setPredictionId(predictionId)
                .setReward(reward)
                .build();
        
        learn(feedback);
    }

    /**
     * Send feedback to update the model.
     */
    public void learn(Feedback feedback) {
        logger.info("Sending feedback: " + feedback);
        try {
            blockingStub.learn(feedback);
            logger.info("Feedback sent successfully");
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            throw e;
        }
    }

    /**
     * Reset the agent's internal state/model.
     */
    public void reset() {
        logger.info("Resetting agent");
        try {
            blockingStub.reset(Empty.getDefaultInstance());
            logger.info("Agent reset successfully");
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            throw e;
        }
    }

    /**
     * Shutdown the channel.
     */
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * Example usage of the client.
     */
    public static void main(String[] args) throws Exception {
        // Create client
        LearningAgentClient client = new LearningAgentClient("localhost", 50051);
        
        try {
            // Reset the agent
            client.reset();
            
            // Create a state
            State state = State.newBuilder()
                    .setProcessedTransactions(1000)
                    .setAvgMessageDelay(15.5f)
                    .setMaxMessageDelay(50.0f)
                    .setMinMessageDelay(5.0f)
                    .setStdMessageDelay(8.2f)
                    .build();
            
            // Get prediction
            Prediction prediction = client.predict(state);
            System.out.println("Prediction ID: " + prediction.getPredictionId());
            System.out.println("Suggested timeout: " + 
                    prediction.getAction().getTimeoutMilliseconds() + " ms");
            
            // Send feedback after evaluating the action
            float reward = 0.85f; // Example reward
            client.learn(prediction.getPredictionId(), reward);
            
            // Alternative: Use the convenience method
            Prediction prediction2 = client.predict(1500, 20.0f, 60.0f, 8.0f, 10.5f);
            System.out.println("Second prediction timeout: " + 
                    prediction2.getAction().getTimeoutMilliseconds() + " ms");
            
        } finally {
            client.shutdown();
        }
    }
}
