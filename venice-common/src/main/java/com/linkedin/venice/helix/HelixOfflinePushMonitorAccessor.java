package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.listener.ListenerManager;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.OfflinePushAccessor;
import com.linkedin.venice.pushmonitor.PartitionStatus;
import com.linkedin.venice.pushmonitor.ReadOnlyPartitionStatus;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.linkedin.venice.utils.Utils;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


/**
 * Helix implementation of {@link OfflinePushAccessor}. All the statuses would be stored on Zookeeper and this
 * class provides the ways to read/write/create/remove status from ZK.
 * <p>
 * As this class is only an accessor but not a repository so it will not cache anything in local memory. In other words
 * it's stateless and Thread-Safe.
 * <p>
 * The data structure on ZK would be:
 * <ul>
 * <li>/OfflinePushes/$topic -> push status for $topic</li>
 * <li>/OfflinePushes/$topic/$partitionId -> partition status including all of replicas statuses for $topic and
 * $partitionId.</li>
 * </ul>
 */
public class HelixOfflinePushMonitorAccessor implements OfflinePushAccessor {
  public static final String OFFLINE_PUSH_SUB_PATH = "OfflinePushes";
  private static final int DEFAULT_ZK_REFRESH_ATTEMPTS = 3;
  private static final long DEFAULT_ZK_REFRESH_INTERVAL = TimeUnit.SECONDS.toMillis(10);

  private static final Logger logger = Logger.getLogger(HelixOfflinePushMonitorAccessor.class);
  private final String clusterName;
  /**
   * Zk accessor for offline push status ZNodes.
   */
  private ZkBaseDataAccessor<OfflinePushStatus> offlinePushStatusAccessor;
  /**
   * Zk accessor for partition status ZNodes.
   */
  private ZkBaseDataAccessor<PartitionStatus> partitionStatusAccessor;

  private final String offlinePushStatusParentPath;
  private final ZkClient zkClient;

  private final ListenerManager<PartitionStatusListener> listenerManager;
  private final PartitionStatusZkListener partitionStatusZkListener;

  private final int refreshAttemptsForZkReconnect;

  private final long refreshIntervalForZkReconnectInMs;

  public HelixOfflinePushMonitorAccessor(String clusterName, ZkClient zkClient, HelixAdapterSerializer adapter) {
    this(clusterName, zkClient, adapter, DEFAULT_ZK_REFRESH_ATTEMPTS, DEFAULT_ZK_REFRESH_INTERVAL);
  }

  public HelixOfflinePushMonitorAccessor(String clusterName, ZkClient zkClient, HelixAdapterSerializer adapter,
      int refreshAttemptsForZkReconnect, long refreshIntervalForZkReconnectInMs) {
    this.clusterName = clusterName;
    this.offlinePushStatusParentPath = getOfflinePushStatuesParentPath();
    this.zkClient = zkClient;
    registerSerializers(adapter);
    this.zkClient.setZkSerializer(adapter);
    this.offlinePushStatusAccessor = new ZkBaseDataAccessor<>(zkClient);
    this.partitionStatusAccessor = new ZkBaseDataAccessor<>(zkClient);
    this.listenerManager = new ListenerManager<>();
    this.partitionStatusZkListener = new PartitionStatusZkListener();
    this.refreshAttemptsForZkReconnect = refreshAttemptsForZkReconnect;
    this.refreshIntervalForZkReconnectInMs = refreshIntervalForZkReconnectInMs;
  }

  private void registerSerializers(HelixAdapterSerializer adapter) {
    String offlinePushStatusPattern = offlinePushStatusParentPath + "/" + PathResourceRegistry.WILDCARD_MATCH_ANY;
    String partitionStatusPattern = offlinePushStatusPattern + "/" + PathResourceRegistry.WILDCARD_MATCH_ANY;
    adapter.registerSerializer(offlinePushStatusPattern, new OfflinePushStatusJSONSerializer());
    adapter.registerSerializer(partitionStatusPattern, new PartitionStatusJSONSerializer());
  }

  @Override
  public List<OfflinePushStatus> loadOfflinePushStatusesAndPartitionStatuses() {
    logger.info("Start loading all offline pushes statuses from ZK in cluster:" + clusterName);
    List<OfflinePushStatus> offlinePushStatuses =
        HelixUtils.getChildren(offlinePushStatusAccessor, offlinePushStatusParentPath, refreshAttemptsForZkReconnect,
            refreshIntervalForZkReconnectInMs);
    Iterator<OfflinePushStatus> iterator = offlinePushStatuses.iterator();
    while (iterator.hasNext()) {
      OfflinePushStatus pushStatus = iterator.next();
      switch (pushStatus.getCurrentStatus()) {
        case ERROR:
        case COMPLETED:
        case STARTED:
          List<PartitionStatus> partitionStatuses = getPartitionStatuses(pushStatus.getKafkaTopic(), pushStatus.getNumberOfPartition());
          pushStatus.setPartitionStatuses(partitionStatuses);
          break;
        default:
          logger.info(
              "Found invalid push statues:" + pushStatus.getCurrentStatus() + " for topic:" + pushStatus.getKafkaTopic()
                  + "in cluster:" + clusterName + ". Will delete it from ZK.");
          HelixUtils.remove(offlinePushStatusAccessor, getOfflinePushStatusPath(pushStatus.getKafkaTopic()));
          iterator.remove();
      }
    }
    logger.info("Loaded " + offlinePushStatuses.size() + " offline pushes statuses from ZK in cluster:" + clusterName);
    return offlinePushStatuses;
  }

  @Override
  public OfflinePushStatus getOfflinePushStatusAndItsPartitionStatuses(String kafkaTopic) {
    OfflinePushStatus offlinePushStatus =
        offlinePushStatusAccessor.get(getOfflinePushStatusPath(kafkaTopic), null, AccessOption.PERSISTENT);
    if (offlinePushStatus == null) {
      throw new VeniceException(
          "Can not find offline push status in ZK from path:" + getOfflinePushStatusPath(kafkaTopic));
    }
    offlinePushStatus.setPartitionStatuses(getPartitionStatuses(kafkaTopic, offlinePushStatus.getNumberOfPartition()));
    return offlinePushStatus;
  }

  @Override
  public void updateOfflinePushStatus(OfflinePushStatus pushStatus) {
    HelixUtils.update(offlinePushStatusAccessor, getOfflinePushStatusPath(pushStatus.getKafkaTopic()), pushStatus);
    logger.info(
        "Updated push status for topic " + pushStatus.getKafkaTopic() + " in cluster:" + clusterName + " to status:"
            + pushStatus.getCurrentStatus());
  }

  @Override
  public synchronized void createOfflinePushStatusAndItsPartitionStatuses(OfflinePushStatus pushStatus) {
    logger.info(
        "Start creating offline push status for topic:" + pushStatus.getKafkaTopic() + " in cluster:" + clusterName);
    HelixUtils.create(offlinePushStatusAccessor, getOfflinePushStatusPath(pushStatus.getKafkaTopic()), pushStatus);
    logger.info("Created offline push status ZNode. Start creating partition statuses.");
    List<String> partitionPaths = new ArrayList<>(pushStatus.getNumberOfPartition());
    List<PartitionStatus> partitionStatuses = new ArrayList<>(pushStatus.getNumberOfPartition());
    for (int partitionId = 0; partitionId < pushStatus.getNumberOfPartition(); partitionId++) {
      partitionPaths.add(getPartitionStatusPath(pushStatus.getKafkaTopic(), partitionId));
      partitionStatuses.add(new PartitionStatus(partitionId));
    }
    HelixUtils.updateChildren(partitionStatusAccessor, partitionPaths, partitionStatuses);
    logger.info("Created " + pushStatus.getNumberOfPartition() + " partition status Znodes for topic : " + pushStatus.getKafkaTopic());
  }

  @Override
  public void deleteOfflinePushStatusAndItsPartitionStatuses(OfflinePushStatus pushStatus) {
    logger.info(
        "Start deleting offline push status for topic: " + pushStatus.getKafkaTopic() + " in cluster: " + clusterName);
    HelixUtils.remove(offlinePushStatusAccessor, getOfflinePushStatusPath(pushStatus.getKafkaTopic()));
    logger.info("Deleted offline push status for topic: " + pushStatus.getKafkaTopic() + " in cluster: " + clusterName);
  }

  @Override
  public void updateReplicaStatus(String topic, int partitionId, String instanceId, ExecutionStatus status,
      long progress, String incrementalPushVersion) {
    compareAndUpdateReplicaStatus(topic, partitionId, instanceId, status, progress, incrementalPushVersion);
  }

  @Override
  public void updateReplicaStatus(String topic, int partitionId, String instanceId, ExecutionStatus status, String incrementalPushVersion) {
    compareAndUpdateReplicaStatus(topic, partitionId, instanceId, status, Integer.MIN_VALUE, incrementalPushVersion);
  }

  /**
   * Because one partition status could contain multiple replicas statuses. So during the updating, the conflicts would
   * happen once there are more than one instance updating its status. In order to handle this conflict, we use a
   * compare and set(CAS) semantic to update.
   * 1. Read the latest partition status ZNode from ZK.
   * 2. Record the version of this ZNode.
   * 3. Apply our change on partition status and update ZNode with the recorded version.
   * 4. If we got BadVersionException, Helix accessor will help us to retry
   * 5. If everything goes well, update succeed.
   * So eventually, all updates will succeed after couples of retries.
   */
  private void compareAndUpdateReplicaStatus(String topic, int partitionId, String instanceId, ExecutionStatus status,
      long progress, String incrementalPushVersion) {
    // If a version was created prior to the deployment of this new push monitor, an exception would be thrown while upgrading venice server.
    // Because the server would try to update replica status but there is no ZNode for that replica. So we add a check here to ignore the update
    // in case of ZNode missing.
    if (!pushStatusExists(topic)) {
      return;
    }
    logger.info(
        "Start update replica status for topic:" + topic + " partition:" + partitionId + " in cluster:" + clusterName);
    HelixUtils.compareAndUpdate(partitionStatusAccessor, getPartitionStatusPath(topic, partitionId), currentData -> {
      currentData.updateReplicaStatus(instanceId, status, incrementalPushVersion);
      if (progress != Integer.MIN_VALUE) {
        currentData.updateProgress(instanceId, progress);
      }
      if (!Utils.isNullOrEmpty(incrementalPushVersion)) {
        currentData.updateIncrementalPushVersion(instanceId, incrementalPushVersion);
      }

      return currentData;
    });
    logger.info("Updated replica status for topic:" + topic + " partition:" + partitionId + " status: " + status
        + " in cluster:" + clusterName);
  }

  @Override
  public void subscribePartitionStatusChange(OfflinePushStatus pushStatus, PartitionStatusListener listener) {
    listenerManager.subscribe(pushStatus.getKafkaTopic(), listener);
    for (int partitionId = 0; partitionId < pushStatus.getNumberOfPartition(); partitionId++) {
      partitionStatusAccessor.subscribeDataChanges(getPartitionStatusPath(pushStatus.getKafkaTopic(), partitionId),
          partitionStatusZkListener);
    }
  }

  @Override
  public void unsubscribePartitionsStatusChange(OfflinePushStatus pushStatus, PartitionStatusListener listener) {
    unsubscribePartitionsStatusChange(pushStatus.getKafkaTopic(), pushStatus.getNumberOfPartition(), listener);
  }

  @Override
  public void unsubscribePartitionsStatusChange(String topicName, int partitionCount, PartitionStatusListener listener) {
    listenerManager.unsubscribe(topicName, listener);
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      partitionStatusAccessor.unsubscribeDataChanges(getPartitionStatusPath(topicName, partitionId), partitionStatusZkListener);
    }
  }

  @Override
  public void subscribePushStatusCreationChange(IZkChildListener childListener) {
    offlinePushStatusAccessor.subscribeChildChanges(getOfflinePushStatuesParentPath(), childListener);
  }

  @Override
  public void unsubscribePushStatusCreationChange(IZkChildListener childListener) {
    offlinePushStatusAccessor.unsubscribeChildChanges(getOfflinePushStatuesParentPath(), childListener);
  }

  /**
   * Get one partition status ZNode from ZK by given topic and partition.
   */
  protected PartitionStatus getPartitionStatus(String topic, int partitionId) {
    PartitionStatus partitionStatus =
        partitionStatusAccessor.get(getPartitionStatusPath(topic, partitionId), null, AccessOption.PERSISTENT);
    logger.debug(
        "Read partition status for topic:" + topic + " in partition:" + partitionId + " in cluster:" + clusterName);
    return partitionStatus;
  }

  /**
   * Get all partition status ZNodes under offline push of given topic from ZK.
   * The partition statuses paths for a topic are created in ZK one by one; Helix doesn't guarantee that all the paths
   * are created atomically; therefore, it's possible that a partial list is returned. This function would take cover of
   * all edges cases -- empty response as well as partial response by filling the missing partitions.
   *
   * The returned partition status list is ordered by partition Id.
   */
  protected List<PartitionStatus> getPartitionStatuses(String topic, int partitionCount) {
    logger.debug("Start reading partition status from ZK for topic:" + topic + " in cluster:" + clusterName);
    List<PartitionStatus> zkResult =
        HelixUtils.getChildren(partitionStatusAccessor, getOfflinePushStatusPath(topic), refreshAttemptsForZkReconnect,
            refreshIntervalForZkReconnectInMs);
    logger.debug("Read " + zkResult.size() + " partition status from ZK for topic:" + topic + " in cluster:" + clusterName);

    if (zkResult.isEmpty()) {
      // Partition status list is empty means that partition status node hasn't been fully created yet.
      // In this case, create placeholder statuses based on the partition count from OfflinePushStatus.
      zkResult = new ArrayList<>(partitionCount);
      for (int i = 0; i < partitionCount; ++i) {
        zkResult.add(new PartitionStatus(i));
      }
      return zkResult;
    }

    // Sort the partition statues list by partition Id
    Collections.sort(zkResult);
    if (zkResult.size() == partitionCount) {
      return zkResult;
    } else {
      List<PartitionStatus> fullResult = new ArrayList<>(partitionCount);
      int zkListIndex = 0;
      for (int resultIndex = 0; resultIndex < partitionCount; resultIndex++) {
        if (zkListIndex < zkResult.size() && resultIndex == zkResult.get(zkListIndex).getPartitionId()) {
          fullResult.add(zkResult.get(zkListIndex++));
        } else {
          fullResult.add(new PartitionStatus(resultIndex));
        }
      }
      return fullResult;
    }
  }

  private String getOfflinePushStatuesParentPath() {
    return HelixUtils.getHelixClusterZkPath(clusterName) + "/" + OFFLINE_PUSH_SUB_PATH;
  }

  private String getOfflinePushStatusPath(String topic) {
    return offlinePushStatusParentPath + "/" + topic;
  }

  private String getPartitionStatusPath(String topic, int partitionId) {
    return getOfflinePushStatusPath(topic) + "/" + partitionId;
  }

  private String parseTopicFromPartitionStatusPath(String path) {
    int lastSlash = path.lastIndexOf('/');
    int secondLastSlash = path.lastIndexOf('/', lastSlash - 1);
    return path.substring(secondLastSlash + 1, lastSlash);
  }

  private boolean pushStatusExists(String topic) {
    if (!partitionStatusAccessor.exists(getOfflinePushStatusPath(topic), AccessOption.PERSISTENT)) {
      logger.warn("Push status does not exist, ignore the subsequent operation. Topic: " + topic);
      return false;
    }
    return true;
  }

  /**
   * Listener that get partition status ZNode data change notification then transfer it to a Venice partition status
   * change event and broadcast this event to Venice subscriber.
   */
  private class PartitionStatusZkListener implements IZkDataListener {
    @Override
    public void handleDataChange(String dataPath, Object data)
        throws Exception {
      if (!(data instanceof PartitionStatus)) {
        throw new VeniceException("Invalid notification, changed data is not:" + PartitionStatus.class.getName());
      }
      String topic = parseTopicFromPartitionStatusPath(dataPath);
      ReadOnlyPartitionStatus partitionStatus = ReadOnlyPartitionStatus.fromPartitionStatus((PartitionStatus) data);
      listenerManager.trigger(topic, listener -> {
        try {
          listener.onPartitionStatusChange(topic, partitionStatus);
        } catch (Exception e) {
          logger.error("Error when invoking callback function for partition status change", e);
        }
        return null;
      });
    }

    @Override
    public void handleDataDeleted(String dataPath) {
      logger.error("Partition status should not be deleted while monitoring the push status. path:" + dataPath);
    }
  }
}
