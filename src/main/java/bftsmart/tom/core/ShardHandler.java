package bftsmart.tom.core;

import bftsmart.tom.ServiceProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Handles cross-shard communication for multi-shard BFT-SMaRt deployments.
 * Manages ServiceProxy connections to other shards.
 */
public class ShardHandler {
    private static final Logger logger = LoggerFactory.getLogger(ShardHandler.class);

    // Base client ID for cross-shard proxies
    private static final int BASE_CROSS_SHARD_CLIENT_ID = 8000;

    // This replica's identity
    private final int shardId;
    private final int replicaId;
    private final String configHome;

    // Shard configuration
    private final int totalShards;
    private final Map<Integer, String> shardConfigPaths;

    private final Map<Integer, ServiceProxy> shardProxies;
    private final ReentrantReadWriteLock proxyLock;

    // Connection states
    private final Map<Integer, ConnectionState> connectionStates;

    // Executor for parallel cross-shard requests
    private final ExecutorService executor;

    // Configuration for retry behavior
    private int maxRetries = 3;
    private long retryDelayMs = 1000;

    public enum ConnectionState {
        NOT_CONNECTED,
        CONNECTING,
        CONNECTED,
        FAILED
    }

    /**
     * Simple constructor.
     *
     * @param shardId This replica's shard ID
     */
    public ShardHandler(int shardId) {
        this(shardId, 0, "config");
    }

    /**
     * Primary constructor: loads shard configuration from shards.config.
     *
     * @param shardId    This replica's shard ID
     * @param replicaId  This replica's ID within the shard
     * @param configHome Path to this replica's config directory
     */
    public ShardHandler(int shardId, int replicaId, String configHome) {
        this.shardId = shardId;
        this.replicaId = replicaId;
        this.configHome = configHome;
        this.shardProxies = new ConcurrentHashMap<>();
        this.connectionStates = new ConcurrentHashMap<>();
        this.proxyLock = new ReentrantReadWriteLock();
        this.executor = Executors.newCachedThreadPool();

        // Load shard configuration
        ShardConfig config = loadShardConfig(configHome);
        this.totalShards = config.totalShards;
        this.shardConfigPaths = config.shardConfigPaths;
        this.maxRetries = config.maxRetries;
        this.retryDelayMs = config.retryDelayMs;

        // Initialize connection states for other shards
        for (int i = 0; i < totalShards; i++) {
            if (i != shardId) {
                connectionStates.put(i, ConnectionState.NOT_CONNECTED);
            }
        }

        logger.info("ShardHandler initialized for shard {} replica {}, total shards: {}",
                shardId, replicaId, totalShards);
    }

    /**
     * Alternate constructor.
     *
     * @param shardId          This replica's shard ID
     * @param replicaId        This replica's ID within the shard
     * @param totalShards      Total number of shards
     * @param shardConfigPaths Map of shardId to config directory path
     */
    public ShardHandler(int shardId, int replicaId, int totalShards,
                        Map<Integer, String> shardConfigPaths) {
        this.shardId = shardId;
        this.replicaId = replicaId;
        this.configHome = "";
        this.totalShards = totalShards;
        this.shardConfigPaths = new ConcurrentHashMap<>(shardConfigPaths);
        this.shardProxies = new ConcurrentHashMap<>();
        this.connectionStates = new ConcurrentHashMap<>();
        this.proxyLock = new ReentrantReadWriteLock();
        this.executor = Executors.newCachedThreadPool();

        for (int i = 0; i < totalShards; i++) {
            if (i != shardId) {
                connectionStates.put(i, ConnectionState.NOT_CONNECTED);
            }
        }

        logger.info("ShardHandler initialized programmatically for shard {} replica {}, total shards: {}",
                shardId, replicaId, totalShards);
    }


    /**
     * Send an ordered request to a specific shard and wait for response.
     * Thread-safe: multiple threads can call this concurrently.
     *
     * @param targetShardId The shard to send the request to
     * @param request       The serialized request payload
     * @return The response from the target shard, or null on timeout/failure
     * @throws IllegalArgumentException if targetShardId is invalid
     */
    public byte[] invokeOrderedOnShard(int targetShardId, byte[] request) {
        validateTargetShard(targetShardId);
        ServiceProxy proxy = getOrCreateProxy(targetShardId);

        if (proxy == null) {
            logger.error("Failed to get proxy for shard {}", targetShardId);
            return null;
        }

        try {
            return proxy.invokeOrdered(request);
        } catch (Exception e) {
            logger.error("Error invoking ordered request on shard {}", targetShardId, e);
            handleProxyFailure(targetShardId, e);
            return null;
        }
    }

    /**
     * Send an unordered request to a specific shard.
     *
     * @param targetShardId The shard to send the request to
     * @param request       The serialized request payload
     * @return The response from the target shard, or null on timeout/failure
     */
    public byte[] invokeUnorderedOnShard(int targetShardId, byte[] request) {
        validateTargetShard(targetShardId);
        ServiceProxy proxy = getOrCreateProxy(targetShardId);

        if (proxy == null) {
            logger.error("Failed to get proxy for shard {}", targetShardId);
            return null;
        }

        try {
            return proxy.invokeUnordered(request);
        } catch (Exception e) {
            logger.error("Error invoking unordered request on shard {}", targetShardId, e);
            handleProxyFailure(targetShardId, e);
            return null;
        }
    }

    /**
     * Send ordered requests to multiple shards in parallel.
     *
     * @param targetShards Map of shardId to request payload
     * @return Map of shardId to response
     */
    public Map<Integer, byte[]> invokeOrderedOnMultipleShards(Map<Integer, byte[]> targetShards) {
        Map<Integer, Future<byte[]>> futures = new HashMap<>();

        for (Map.Entry<Integer, byte[]> entry : targetShards.entrySet()) {
            int targetShardId = entry.getKey();
            byte[] request = entry.getValue();

            Future<byte[]> future = executor.submit(() ->
                    invokeOrderedOnShard(targetShardId, request)
            );
            futures.put(targetShardId, future);
        }

        // Collect results
        Map<Integer, byte[]> results = new HashMap<>();
        for (Map.Entry<Integer, Future<byte[]>> entry : futures.entrySet()) {
            try {
                byte[] response = entry.getValue().get(30, TimeUnit.SECONDS);
                results.put(entry.getKey(), response);
            } catch (Exception e) {
                logger.error("Error getting response from shard {}", entry.getKey(), e);
                results.put(entry.getKey(), null);
            }
        }

        return results;
    }

    /**
     * Invoke with automatic retry on failure.
     *
     * @param targetShardId The shard to send the request to
     * @param request       The serialized request payload
     * @return The response, or null if all retries failed
     */
    public byte[] invokeOrderedWithRetry(int targetShardId, byte[] request) {
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            byte[] response = invokeOrderedOnShard(targetShardId, request);
            if (response != null) {
                return response;
            }

            logger.warn("Attempt {} failed for shard {}, retrying...", attempt + 1, targetShardId);

            // Try reconnecting
            reconnectToShard(targetShardId);

            try {
                Thread.sleep(retryDelayMs * (attempt + 1));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        logger.error("All {} retry attempts failed for shard {}", maxRetries, targetShardId);
        return null;
    }

    // Proxy Management

    /**
     * Get or create a ServiceProxy for the target shard.
     */
    private ServiceProxy getOrCreateProxy(int targetShardId) {
        // check if proxy exists
        ServiceProxy proxy = shardProxies.get(targetShardId);
        if (proxy != null) {
            return proxy;
        }

        // create proxy with proper synchronization
        proxyLock.writeLock().lock();
        try {
            // Double-check after acquiring lock
            proxy = shardProxies.get(targetShardId);
            if (proxy != null) {
                return proxy;
            }

            return createProxyForShard(targetShardId);
        } finally {
            proxyLock.writeLock().unlock();
        }
    }

    /**
     * Create a new ServiceProxy for the target shard.
     * Must be called while holding write lock.
     */
    private ServiceProxy createProxyForShard(int targetShardId) {
        connectionStates.put(targetShardId, ConnectionState.CONNECTING);

        String targetConfigHome = shardConfigPaths.get(targetShardId);
        if (targetConfigHome == null) {
            logger.error("No config path found for shard {}", targetShardId);
            connectionStates.put(targetShardId, ConnectionState.FAILED);
            return null;
        }

        int clientId = generateCrossShardClientId(targetShardId);

        logger.info("Creating ServiceProxy for shard {} with clientId {} and configHome {}",
                targetShardId, clientId, targetConfigHome);

        try {
            ServiceProxy proxy = new ServiceProxy(clientId, targetConfigHome);
            shardProxies.put(targetShardId, proxy);
            connectionStates.put(targetShardId, ConnectionState.CONNECTED);
            logger.info("Successfully created proxy for shard {}", targetShardId);
            return proxy;
        } catch (Exception e) {
            logger.error("Failed to create proxy for shard {}", targetShardId, e);
            connectionStates.put(targetShardId, ConnectionState.FAILED);
            return null;
        }
    }

    /**
     * Generate a unique client ID for cross-shard communication.
     */
    private int generateCrossShardClientId(int targetShardId) {
        return BASE_CROSS_SHARD_CLIENT_ID +
                (shardId * 1000) +
                (replicaId * 100) +
                targetShardId;
    }

    // Connections

    public void initializeAllConnections() {
        logger.info("Eagerly initializing connections to {} other shards", totalShards - 1);

        for (int i = 0; i < totalShards; i++) {
            if (i != shardId) {
                getOrCreateProxy(i);
            }
        }

        long connected = connectionStates.values().stream()
                .filter(s -> s == ConnectionState.CONNECTED)
                .count();

        logger.info("Initialized {} of {} cross-shard connections", connected, totalShards - 1);
    }

    private void handleProxyFailure(int targetShardId, Exception e) {
        proxyLock.writeLock().lock();
        try {
            // Close existing proxy if any
            ServiceProxy oldProxy = shardProxies.remove(targetShardId);
            if (oldProxy != null) {
                try {
                    oldProxy.close();
                } catch (Exception closeEx) {
                    logger.warn("Error closing failed proxy for shard {}", targetShardId, closeEx);
                }
            }

            connectionStates.put(targetShardId, ConnectionState.FAILED);
        } finally {
            proxyLock.writeLock().unlock();
        }
    }

    public boolean reconnectToShard(int targetShardId) {
        ConnectionState state = connectionStates.get(targetShardId);
        if (state == ConnectionState.CONNECTED) {
            return true;
        }

        proxyLock.writeLock().lock();
        try {
            // Remove old proxy if exists
            ServiceProxy oldProxy = shardProxies.remove(targetShardId);
            if (oldProxy != null) {
                try {
                    oldProxy.close();
                } catch (Exception e) {
                    logger.warn("Error closing old proxy for shard {}", targetShardId);
                }
            }

            // Try to create new proxy
            ServiceProxy proxy = createProxyForShard(targetShardId);
            return proxy != null;
        } finally {
            proxyLock.writeLock().unlock();
        }
    }

    // Config
    private ShardConfig loadShardConfig(String configHome) {
        ShardConfig config = new ShardConfig();
        String sep = System.getProperty("file.separator");
        String path = configHome + sep + "shards.config";

        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            Properties props = new Properties();
            props.load(reader);

            config.totalShards = Integer.parseInt(
                    props.getProperty("system.shards.count", "1"));

            String basePath = props.getProperty("system.shards.basePath", "");

            config.shardConfigPaths = new HashMap<>();
            for (int i = 0; i < config.totalShards; i++) {
                String shardConfigHome = props.getProperty("shard." + i + ".configHome");
                if (shardConfigHome != null) {
                    // Resolve relative paths
                    if (!shardConfigHome.startsWith("/") && !basePath.isEmpty()) {
                        shardConfigHome = basePath + sep + shardConfigHome;
                    }
                    config.shardConfigPaths.put(i, shardConfigHome);
                }
            }

            // Load retry configuration
            config.maxRetries = Integer.parseInt(
                    props.getProperty("system.shards.maxRetries", "3"));
            config.retryDelayMs = Long.parseLong(
                    props.getProperty("system.shards.retryDelayMs", "1000"));

            logger.info("Loaded shard config: {} shards from {}", config.totalShards, path);
            return config;

        } catch (IOException e) {
            logger.warn("Could not load shards.config from {}, using single-shard defaults: {}",
                    path, e.getMessage());
            config.totalShards = 1;
            config.shardConfigPaths = new HashMap<>();
            config.shardConfigPaths.put(0, configHome);
            return config;
        }
    }

    private static class ShardConfig {
        int totalShards = 1;
        Map<Integer, String> shardConfigPaths = new HashMap<>();
        int maxRetries = 3;
        long retryDelayMs = 1000;
    }

    // Util
    private void validateTargetShard(int targetShardId) {
        if (targetShardId < 0 || targetShardId >= totalShards) {
            throw new IllegalArgumentException(
                    "Invalid target shard: " + targetShardId +
                            ", total shards: " + totalShards);
        }
        if (targetShardId == shardId) {
            throw new IllegalArgumentException(
                    "Cannot send cross-shard request to own shard: " + targetShardId);
        }
    }
    
    public int getShardId() {
        return shardId;
    }
    
    public int getTotalShards() {
        return totalShards;
    }

    public List<Integer> getOtherShardIds() {
        List<Integer> others = new ArrayList<>();
        for (int i = 0; i < totalShards; i++) {
            if (i != shardId) {
                others.add(i);
            }
        }
        return others;
    }

    public boolean isShardConnected(int targetShardId) {
        return connectionStates.get(targetShardId) == ConnectionState.CONNECTED;
    }

    public ConnectionState getConnectionState(int targetShardId) {
        return connectionStates.getOrDefault(targetShardId, ConnectionState.NOT_CONNECTED);
    }

    public void shutdown() {
        logger.info("Shutting down ShardHandler for shard {}", shardId);

        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        proxyLock.writeLock().lock();
        try {
            for (Map.Entry<Integer, ServiceProxy> entry : shardProxies.entrySet()) {
                try {
                    entry.getValue().close();
                    logger.info("Closed proxy for shard {}", entry.getKey());
                } catch (Exception e) {
                    logger.warn("Error closing proxy for shard {}", entry.getKey(), e);
                }
            }
            shardProxies.clear();
            connectionStates.replaceAll((k, v) -> ConnectionState.NOT_CONNECTED);
        } finally {
            proxyLock.writeLock().unlock();
        }
    }
}
