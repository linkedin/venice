package com.linkedin.venice.pushstatushelper;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.pushstatus.PushStatusValue;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is a helper class for Venice controller to read PushStatus / Heartbeat messages.
 */
public class PushStatusStoreReader implements Closeable {
  public enum InstanceStatus {
    ALIVE, DEAD, BOOTSTRAPPING
  }

  private static final Logger LOGGER = LogManager.getLogger(PushStatusStoreReader.class);
  private static final int DEFAULT_HEARTBEAT_READ_TIMEOUT_SECONDS = 3;
  private final Map<String, AvroSpecificStoreClient<PushStatusKey, PushStatusValue>> veniceClients =
      new VeniceConcurrentHashMap<>();
  private final D2Client d2Client;
  private final String clusterDiscoveryD2ServiceName;
  private final long heartbeatExpirationTimeInSeconds;
  private final Function<String, String> clusterResolver;
  private final ScheduledExecutorService clusterReadTimeoutExecutor;
  private final ExecutorService clusterReadExecutor;
  private volatile boolean closed;

  // if store limit is less than this then batchGet will fail
  private static final int PUSH_STATUS_READER_BATCH_GET_LIMIT = 100;

  public PushStatusStoreReader(
      D2Client d2Client,
      String clusterDiscoveryD2ServiceName,
      long heartbeatExpirationTimeInSeconds) {
    this(d2Client, clusterDiscoveryD2ServiceName, heartbeatExpirationTimeInSeconds, null);
  }

  public PushStatusStoreReader(
      D2Client d2Client,
      String clusterDiscoveryD2ServiceName,
      long heartbeatExpirationTimeInSeconds,
      Function<String, String> clusterResolver) {
    this.d2Client = d2Client;
    this.clusterDiscoveryD2ServiceName = clusterDiscoveryD2ServiceName;
    this.heartbeatExpirationTimeInSeconds = heartbeatExpirationTimeInSeconds;
    this.clusterResolver = clusterResolver;
    this.clusterReadTimeoutExecutor =
        Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("cluster-push-status-read-timeout"));
    // No backlog is retained when discovery blocks both workers; excess reads use legacy fallback.
    this.clusterReadExecutor = new ThreadPoolExecutor(
        2,
        2,
        0,
        TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        new DaemonThreadFactory("cluster-push-status-read"));
  }

  private <T> CompletableFuture<T> readCluster(Supplier<CompletableFuture<T>> request) {
    CompletableFuture<T> result = new CompletableFuture<>();
    try {
      ScheduledFuture<?> timeout = clusterReadTimeoutExecutor.schedule(
          () -> result.completeExceptionally(new TimeoutException("Cluster push status read timed out")),
          1,
          TimeUnit.SECONDS);
      result.whenComplete((value, error) -> timeout.cancel(false));
      clusterReadExecutor.execute(() -> {
        if (result.isDone()) {
          return;
        }
        try {
          request.get().whenComplete((value, error) -> {
            if (error == null) {
              result.complete(value);
            } else {
              result.completeExceptionally(error);
            }
          });
        } catch (RuntimeException e) {
          result.completeExceptionally(e);
        }
      });
    } catch (RuntimeException e) {
      result.completeExceptionally(e);
    }
    return result.exceptionally(error -> {
      LOGGER.warn("Cluster push status read failed; falling back to the per-store system store", error);
      return null;
    });
  }

  private CompletableFuture<PushStatusValue> get(String storeName, PushStatusKey key) {
    CompletableFuture<PushStatusValue> legacy = getVeniceClient(storeName).get(key);
    if (clusterResolver == null) {
      return legacy;
    }
    return getClusterStatus(storeName, key).thenCombine(legacy, PushStatusStoreReader::merge);
  }

  private CompletableFuture<PushStatusValue> getClusterStatus(String storeName, PushStatusKey key) {
    return readCluster(() -> {
      String clusterName = clusterResolver.apply(storeName);
      return clusterName == null
          ? CompletableFuture.completedFuture(null)
          : getClusterVeniceClient(clusterName).get(PushStatusStoreUtils.getClusterPushStatusKey(storeName, key));
    });
  }

  private static PushStatusValue merge(PushStatusValue primary, PushStatusValue legacy) {
    if (primary == null && legacy == null) {
      return null;
    }
    PushStatusValue result = new PushStatusValue();
    Map<CharSequence, Integer> instances = new HashMap<>();
    if (primary != null && primary.instances != null) {
      primary.instances.forEach((instance, status) -> instances.put(instance.toString(), status));
    }
    // Legacy remains authoritative during dual writes; cluster updates are best-effort and have no report ordering.
    if (legacy != null && legacy.instances != null) {
      legacy.instances.forEach((instance, status) -> instances.put(instance.toString(), status));
    }
    result.instances = (primary != null && legacy != null) || (primary != null && primary.instances != null)
        || (legacy != null && legacy.instances != null) ? instances : null;
    result.reportTimestamp = primary == null ? null : primary.reportTimestamp;
    if (legacy != null && legacy.reportTimestamp != null) {
      result.reportTimestamp = legacy.reportTimestamp;
    }
    return result;
  }

  private CompletableFuture<Map<PushStatusKey, PushStatusValue>> batchGet(String storeName, Set<PushStatusKey> keys) {
    CompletableFuture<Map<PushStatusKey, PushStatusValue>> legacy = getVeniceClient(storeName).batchGet(keys);
    if (clusterResolver == null) {
      return legacy;
    }
    return this.<Map<PushStatusKey, PushStatusValue>>readCluster(() -> {
      String clusterName = clusterResolver.apply(storeName);
      if (clusterName == null) {
        return CompletableFuture.completedFuture(null);
      }
      Set<PushStatusKey> clusterKeys = keys.stream()
          .map(key -> PushStatusStoreUtils.getClusterPushStatusKey(storeName, key))
          .collect(Collectors.toSet());
      return getClusterVeniceClient(clusterName).batchGet(clusterKeys);
    }).thenCombine(legacy, (primary, old) -> {
      if (old == null) {
        throw new VeniceException("Per-store push status batch read returned null");
      }
      Map<PushStatusKey, PushStatusValue> result = new HashMap<>();
      for (PushStatusKey key: keys) {
        PushStatusValue value = merge(
            primary == null ? null : primary.get(PushStatusStoreUtils.getClusterPushStatusKey(storeName, key)),
            old.get(key));
        if (value != null) {
          result.put(key, value);
        }
      }
      return result;
    });
  }

  public Map<CharSequence, Integer> getVersionStatus(
      String storeName,
      int version,
      Optional<String> incrementalPushVersion) {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getPushKey(version, incrementalPushVersion);
    try {
      PushStatusValue pushStatusValue = get(storeName, pushStatusKey).get(60, TimeUnit.SECONDS);
      if (pushStatusValue == null) {
        // Don't return empty map yet, because caller cannot differentiate between DaVinci not migrated to new mode and
        // DaVinci writing empty map.
        return null;
      } else {
        return pushStatusValue.instances;
      }
    } catch (Exception e) {
      LOGGER.error("Failed to read push status of store:{} version:{}", storeName, version, e);
      throw new VeniceException(e);
    }
  }

  public Map<CharSequence, Integer> getPartitionStatus(
      String storeName,
      int version,
      int partitionId,
      Optional<String> incrementalPushVersion) {
    return getPartitionStatus(storeName, version, partitionId, incrementalPushVersion, Optional.empty());
  }

  public Map<CharSequence, Integer> getPartitionStatus(
      String storeName,
      int version,
      int partitionId,
      Optional<String> incrementalPushVersion,
      Optional<String> incrementalPushPrefix) {
    // Reuse the async method but block here
    try {
      return getPartitionStatusAsync(storeName, version, partitionId, incrementalPushVersion, incrementalPushPrefix)
          .get(60, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOGGER.error("Failed to read push status of partition:{} store:{}", partitionId, storeName, e);
      throw new VeniceException(e);
    }
  }

  public CompletableFuture<Map<CharSequence, Integer>> getPartitionStatusAsync(
      String storeName,
      int version,
      int partitionId,
      Optional<String> incrementalPushVersion,
      Optional<String> incrementalPushPrefix) {
    try {
      PushStatusKey pushStatusKey =
          PushStatusStoreUtils.getPushKey(version, partitionId, incrementalPushVersion, incrementalPushPrefix);

      // Get the CompletableFuture from the client and transform it
      return get(storeName, pushStatusKey).<Map<CharSequence, Integer>>thenApply(pushStatusValue -> {
        if (pushStatusValue == null) {
          return Collections.emptyMap();
        } else {
          return pushStatusValue.instances;
        }
      }).exceptionally(e -> {
        LOGGER.error("Failed to read push status of partition:{} store:{}", partitionId, storeName, e);
        throw new VeniceException(e);
      });
    } catch (Exception e) {
      // Handle any exceptions during setup by returning a failed future
      LOGGER.error(
          "Failed to set up async request for partition:{} store:{} to get partition status",
          partitionId,
          storeName,
          e);
      CompletableFuture<Map<CharSequence, Integer>> failedFuture = new CompletableFuture<>();
      failedFuture.completeExceptionally(new VeniceException(e));
      return failedFuture;
    }
  }

  /**
   * If it is batch reporting, then only query with version-level key; else query with partition-level key.
   * @param storeName store name
   * @param version version
   * @param partitionId partition id
   * @param incrementalPushVersion incremental push version
   * @param incrementalPushPrefix incremental push prefix
   * @param isBatchReporting whether it is batch reporting, if batch reporting, then only query with version.
   * @return a CompletableFuture containing the partition status
   */
  public CompletableFuture<Map<CharSequence, Integer>> getPartitionOrVersionStatusAsync(
      String storeName,
      int version,
      int partitionId,
      Optional<String> incrementalPushVersion,
      Optional<String> incrementalPushPrefix,
      boolean isBatchReporting) {
    try {
      PushStatusKey pushStatusKey = isBatchReporting
          ? PushStatusStoreUtils.getPushKey(version, incrementalPushVersion)
          : PushStatusStoreUtils.getPushKey(version, partitionId, incrementalPushVersion, incrementalPushPrefix);

      // Get the CompletableFuture from the client and transform it
      return get(storeName, pushStatusKey).<Map<CharSequence, Integer>>thenApply(pushStatusValue -> {
        if (pushStatusValue == null) {
          return Collections.emptyMap();
        } else {
          return pushStatusValue.instances;
        }
      }).exceptionally(e -> {
        LOGGER.error("Failed to read push status of partition:{} store:{}", partitionId, storeName, e);
        throw new VeniceException(e);
      });
    } catch (Exception e) {
      // Handle any exceptions during setup by returning a failed future
      LOGGER.error(
          "Failed to set up async request for partition:{} store:{} to get partition status",
          partitionId,
          storeName,
          e);
      CompletableFuture<Map<CharSequence, Integer>> failedFuture = new CompletableFuture<>();
      failedFuture.completeExceptionally(new VeniceException(e));
      return failedFuture;
    }
  }

  /**
   * Return statuses of all replicas belonging to partitions with partitionIds in the range [0 (inclusive), numberOfPartitions (exclusive))
   * {partitionId: {instance:status, instance:status,...},...}
   */
  public Map<Integer, Map<CharSequence, Integer>> getPartitionStatuses(
      String storeName,
      int storeVersion,
      String incrementalPushVersion,
      int numberOfPartitions) {
    Set<Integer> partitionIds = IntStream.range(0, numberOfPartitions).boxed().collect(Collectors.toSet());
    return getPartitionStatuses(
        storeName,
        storeVersion,
        incrementalPushVersion,
        partitionIds,
        Optional.of(PUSH_STATUS_READER_BATCH_GET_LIMIT));
  }

  public Map<Integer, Map<CharSequence, Integer>> getPartitionStatuses(
      String storeName,
      int storeVersion,
      String incrementalPushVersion,
      int numberOfPartitions,
      int batchGetLimit) {
    Set<Integer> partitionIds = IntStream.range(0, numberOfPartitions).boxed().collect(Collectors.toSet());
    return getPartitionStatuses(
        storeName,
        storeVersion,
        incrementalPushVersion,
        partitionIds,
        Optional.of(batchGetLimit));
  }

  /**
   * Return statuses of all replicas belonging to partitions mentioned in partitionIds. If status is not available for
   * a partition then empty map will be returned as a value for that partition.
   * {partitionId: {instance:status, instance:status,...},...}
   */
  public Map<Integer, Map<CharSequence, Integer>> getPartitionStatuses(
      String storeName,
      int storeVersion,
      String incrementalPushVersion,
      Set<Integer> partitionIds,
      Optional<Integer> batchGetLimitOption) {
    // create push status key for each partition mentioned in partitionIds
    List<PushStatusKey> pushStatusKeys = new ArrayList<>(partitionIds.size());
    for (int partitionId: partitionIds) {
      pushStatusKeys.add(
          PushStatusStoreUtils.getServerIncrementalPushKey(
              storeVersion,
              partitionId,
              incrementalPushVersion,
              PushStatusStoreUtils.SERVER_INCREMENTAL_PUSH_PREFIX));
    }
    List<CompletableFuture<Map<PushStatusKey, PushStatusValue>>> completableFutures = new ArrayList<>();
    Map<PushStatusKey, PushStatusValue> pushStatusMap = new HashMap<>();
    int batchGetLimit = PUSH_STATUS_READER_BATCH_GET_LIMIT;
    if (batchGetLimitOption.isPresent()) {
      batchGetLimit = batchGetLimitOption.get();
    }
    int numberOfBatchRequests =
        partitionIds.size() / batchGetLimit + (partitionIds.size() % batchGetLimit == 0 ? 0 : 1);
    try {
      for (int i = 0; i < numberOfBatchRequests; i++) {
        int start = i * batchGetLimit;
        int end = Math.min(pushStatusKeys.size(), start + batchGetLimit);
        Set<PushStatusKey> keySet = new HashSet<>(pushStatusKeys.subList(start, end));
        CompletableFuture<Map<PushStatusKey, PushStatusValue>> completableFuture = batchGet(storeName, keySet);
        completableFutures.add(completableFuture);
      }
      for (CompletableFuture<Map<PushStatusKey, PushStatusValue>> completableFuture: completableFutures) {
        Map<PushStatusKey, PushStatusValue> statuses = completableFuture.get(60, TimeUnit.SECONDS);
        if (statuses == null) {
          LOGGER.warn("Failed to get incremental push status of some partitions. BatchGet returned null.");
          throw new VeniceException("Failed to get incremental push status of some partitions");
        }
        pushStatusMap.putAll(statuses);
      }
    } catch (InterruptedException | ExecutionException | VeniceClientException | TimeoutException e) {
      LOGGER.error(
          "Failed to get statuses of partitions. store:{}, storeVersion:{} incrementalPushVersion:{} "
              + "partitionIds:{} Exception:{}",
          storeName,
          storeVersion,
          incrementalPushVersion,
          partitionIds,
          e);
      throw new VeniceException("Failed to fetch push statuses from the push status store");
    }
    Map<Integer, Map<CharSequence, Integer>> result = new HashMap<>();
    for (PushStatusKey pushStatusKey: pushStatusKeys) {
      PushStatusValue pushStatusValue = pushStatusMap.get(pushStatusKey);
      result.put(
          PushStatusStoreUtils.getPartitionIdFromServerIncrementalPushKey(pushStatusKey),
          (pushStatusValue == null || pushStatusValue.instances == null)
              ? Collections.emptyMap()
              : pushStatusValue.instances);
    }
    return result;
  }

  /**
   * @param instanceName = [hostname + appName]
   */
  public long getHeartbeat(String storeName, String instanceName) {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getHeartbeatKey(instanceName);
    try {
      CompletableFuture<PushStatusValue> heartbeat = getVeniceClient(storeName).get(pushStatusKey);
      if (clusterResolver != null) {
        heartbeat = heartbeat.thenCompose(legacy -> {
          if (legacy != null && legacy.reportTimestamp != null) {
            return CompletableFuture.completedFuture(legacy);
          }
          return getClusterStatus(storeName, pushStatusKey).thenApply(primary -> merge(primary, legacy));
        });
      }
      PushStatusValue pushStatusValue = heartbeat.get(DEFAULT_HEARTBEAT_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (pushStatusValue == null) {
        return 0;
      } else {
        return pushStatusValue.reportTimestamp;
      }
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  public boolean isInstanceAlive(String storeName, String instanceName) {
    return isInstanceAlive(getHeartbeat(storeName, instanceName));
  }

  boolean isInstanceAlive(long lastReportTimeStamp) {
    return System.currentTimeMillis() - lastReportTimeStamp <= TimeUnit.SECONDS
        .toMillis(heartbeatExpirationTimeInSeconds);
  }

  public InstanceStatus getInstanceStatus(String storeName, String instanceName) {
    long lastReportTimeStamp = getHeartbeat(storeName, instanceName);
    if (lastReportTimeStamp < 0) {
      return InstanceStatus.BOOTSTRAPPING;
    }

    return isInstanceAlive(lastReportTimeStamp) ? InstanceStatus.ALIVE : InstanceStatus.DEAD;
  }

  public Map<CharSequence, Integer> getSupposedlyOngoingIncrementalPushVersions(String storeName, int storeVersion) {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);
    PushStatusValue pushStatusValue;
    try {
      pushStatusValue = get(storeName, pushStatusKey).get(60, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | VeniceException | TimeoutException e) {
      LOGGER.error("Failed to get ongoing incremental pushes for store:{}.", storeName, e);
      throw new VeniceException(e);
    }
    if (pushStatusValue == null || pushStatusValue.instances == null) {
      return Collections.emptyMap();
    }
    LOGGER.info("Supposedly-ongoing incremental pushes for store:{} - {}", storeName, pushStatusValue.instances);
    return pushStatusValue.instances;
  }

  // visible for testing
  AvroSpecificStoreClient<PushStatusKey, PushStatusValue> getVeniceClient(String storeName) {
    return veniceClients
        .computeIfAbsent(storeName, (s) -> ClientFactory.getAndStartSpecificAvroClient(getClientConfig(storeName)));
  }

  ClientConfig<PushStatusValue> getClientConfig(String storeName) {
    return createClientConfig(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName));
  }

  private ClientConfig<PushStatusValue> createClientConfig(String systemStoreName) {
    return ClientConfig.defaultSpecificClientConfig(systemStoreName, PushStatusValue.class)
        .setD2Client(d2Client)
        .setD2ServiceName(clusterDiscoveryD2ServiceName)
        .setStatTrackingEnabled(false);
  }

  AvroSpecificStoreClient<PushStatusKey, PushStatusValue> getClusterVeniceClient(String clusterName) {
    String systemStoreName = VeniceSystemStoreUtils.getDaVinciPushStatusStoreNameForCluster(clusterName);
    AvroSpecificStoreClient<PushStatusKey, PushStatusValue> client = veniceClients.computeIfAbsent(
        systemStoreName,
        name -> ClientFactory.getAndStartSpecificAvroClient(createClientConfig(name)));
    if (closed) {
      veniceClients.remove(systemStoreName, client);
      client.close();
      throw new VeniceException("Push status reader is closed");
    }
    return client;
  }

  @Override
  public void close() {
    closed = true;
    clusterReadTimeoutExecutor.shutdownNow();
    clusterReadExecutor.shutdownNow();
    veniceClients.forEach((storeName, veniceClient) -> {
      try {
        veniceClient.close();
      } catch (Exception e) {
        LOGGER.error("Can not close VeniceClient. ", e);
      }
    });
  }

}
