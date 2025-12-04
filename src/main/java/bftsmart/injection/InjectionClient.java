package bftsmart.injection;

import bftsmart.tom.ServiceProxy;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class InjectionClient {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final List<InjectionConfig> configs;
    private final ScheduledExecutorService scheduler;
    private final long startTimeMillis;
    private final ServiceProxy proxy;

    public InjectionClient(String configPath, int clientId) throws IOException {
        this.configs = readConfig(configPath);
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.startTimeMillis = System.currentTimeMillis();
        this.proxy = new ServiceProxy(clientId);
        logger.info("InjectionClient initialized with {} configs", configs.size());
    }

    /**
     * Start monitoring and applying injection configs based on their scheduled time
     */
    public void start() {
        if (configs.isEmpty()) {
            logger.warn("No injection configs to apply");
            return;
        }

        // Schedule each config based on its time
        long lastInjectionTime = 0;
        for (InjectionConfig config : configs) {
            long delaySeconds = config.getTime();
            scheduler.schedule(() -> {
                inject(config);
            }, delaySeconds, TimeUnit.SECONDS);
            lastInjectionTime = Math.max(delaySeconds, lastInjectionTime);
            logger.info("Scheduled injection config to apply at {} seconds", delaySeconds);
        }

        final long shutdownDelay = lastInjectionTime + 5;
        scheduler.schedule(this::shutdown,  shutdownDelay, TimeUnit.SECONDS);
        logger.info("Scheduled injector shutdown at {} seconds", shutdownDelay);
    }

    /**
     * Apply an injection config to the current node
     */
    private void inject(InjectionConfig config) {
        logger.info("Applying injection config at time {} seconds", config.getTime());

        try {
            // Update the injection object
            byte[] request = InjectionConfig.toBytes(config);
            proxy.invokeInjection(request);

            // Log what was applied
            logAppliedConfig(config);

        } catch (Exception e) {
            logger.error("Failed to apply injection config", e);
        }
    }

    /**
     * Log details of the applied configuration
     */
    private void logAppliedConfig(InjectionConfig config) {
        logger.info("=== Applied Injection Config ===");
        logger.info("Config Time: {} seconds", config.getTime());
        logger.info("Current elapsed milliseconds: {}", (System.currentTimeMillis() - startTimeMillis));
        logger.info("Nodes alive: {}", config.getAlive());

        // Log ignored nodes
        if (config.getIgnoreNodes() != null && config.getIgnoreNodes().isEmpty()) {
            for (IgnoreNodesConfig ignoreNodesConfig : config.getIgnoreNodes()) {
                logger.info("{} ignoring {}", ignoreNodesConfig.getSrcNode(), ignoreNodesConfig.getIgnoredNodes());
            }
        }

        // Log delays
        List<DelayConfig> delayConfigs = config.getDelayConfigs();
        if (!delayConfigs.isEmpty()) {
            for (DelayConfig delayConfig : delayConfigs) {
                int srcNode = delayConfig.getSrcNode();
                String msgType = delayConfig.getMessageType();
                List<TargetDelayConfig> targetDelays = delayConfig.getTargetDelays();
                logger.info("Src Node: {}, Msg type: {}", srcNode, msgType);
                for (TargetDelayConfig targetDelay : targetDelays) {
                    logger.info("Target: {}, Delay {}-{} ms", targetDelay.getTargetNode(), targetDelay.getDelayBounds().getMinDelay(), targetDelay.getDelayBounds().getMaxDelay());
                }
            }
        }

        logger.info("================================");
    }

    /**
     * Shutdown the injection client
     */
    public void shutdown() {
        logger.info("Shutting down InjectionClient");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Read config from injection JSON file
     * @param filePath path to the injection config JSON
     * @return List of injection configs to be applied at different times
     * @throws IOException
     */

    public static List<InjectionConfig> readConfig(String filePath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(
                new File(filePath),
                new TypeReference<List<InjectionConfig>>() {}
        );
    }
}