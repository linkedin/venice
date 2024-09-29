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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

  // if store limit is less than this then batchGet will fail
  private static final int PUSH_STATUS_READER_BATCH_GET_LIMIT = 100;

  public PushStatusStoreReader(
      D2Client d2Client,
      String clusterDiscoveryD2ServiceName,
      long heartbeatExpirationTimeInSeconds) {
    this.d2Client = d2Client;
    this.clusterDiscoveryD2ServiceName = clusterDiscoveryD2ServiceName;
    this.heartbeatExpirationTimeInSeconds = heartbeatExpirationTimeInSeconds;
  }

  public Map<CharSequence, Integer> getVersionStatus(String storeName, int version) {
    AvroSpecificStoreClient<PushStatusKey, PushStatusValue> client = getVeniceClient(storeName);
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getPushKey(version);
    try {
      PushStatusValue pushStatusValue = client.get(pushStatusKey).get(60, TimeUnit.SECONDS);
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
    AvroSpecificStoreClient<PushStatusKey, PushStatusValue> client = getVeniceClient(storeName);
    PushStatusKey pushStatusKey =
        PushStatusStoreUtils.getPushKey(version, partitionId, incrementalPushVersion, incrementalPushPrefix);
    try {
      PushStatusValue pushStatusValue = client.get(pushStatusKey).get(60, TimeUnit.SECONDS);
      if (pushStatusValue == null) {
        return Collections.emptyMap();
      } else {
        LOGGER.info(" {}/{} Instance status: {}", storeName, partitionId, pushStatusValue.instances);
        return pushStatusValue.instances;
      }
    } catch (Exception e) {
      LOGGER.error("Failed to read push status of partition:{} store:{}", partitionId, storeName, e);
      throw new VeniceException(e);
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
    // get push status store client
    AvroSpecificStoreClient<PushStatusKey, PushStatusValue> storeClient = getVeniceClient(storeName);
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
        CompletableFuture<Map<PushStatusKey, PushStatusValue>> completableFuture = storeClient.batchGet(keySet);
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
    AvroSpecificStoreClient<PushStatusKey, PushStatusValue> client = getVeniceClient(storeName);
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getHeartbeatKey(instanceName);
    try {
      PushStatusValue pushStatusValue =
          client.get(pushStatusKey).get(DEFAULT_HEARTBEAT_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
    AvroSpecificStoreClient<PushStatusKey, PushStatusValue> storeClient = getVeniceClient(storeName);
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);
    PushStatusValue pushStatusValue;
    try {
      pushStatusValue = storeClient.get(pushStatusKey).get(60, TimeUnit.SECONDS);
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
    return veniceClients.computeIfAbsent(storeName, (s) -> {
      ClientConfig clientConfig =
          ClientConfig.defaultGenericClientConfig(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName))
              .setD2Client(d2Client)
              .setD2ServiceName(clusterDiscoveryD2ServiceName)
              .setSpecificValueClass(PushStatusValue.class);
      return ClientFactory.getAndStartSpecificAvroClient(clientConfig);
    });
  }

  @Override
  public void close() {
    veniceClients.forEach((storeName, veniceClient) -> {
      try {
        veniceClient.close();
      } catch (Exception e) {
        LOGGER.error("Can not close VeniceClient. ", e);
      }
    });
  }

}
