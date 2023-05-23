package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class serves as a collector of offline push status for both Venice Server and Da Vinci clients.
 * It will try to aggregate push status from Server and Da Vinci and produce the final aggregated result.
 * For Venice Server, it will receive status update and may report directly if push status store is not enabled, othewise
 * it will wait for Da Vinci push status and compute final state.
 * For Da Vinci push status, it will be
 *
 */
public class PushStatusCollector {
  private static final Logger LOGGER = LogManager.getLogger(PushStatusCollector.class);
  private final Consumer<String> pushCompletedHandler;
  private final BiConsumer<String, String> pushErrorHandler;
  private final Map<String, TopicPushStatus> topicToPushStatusMap = new VeniceConcurrentHashMap<>();
  private final PushStatusStoreReader pushStatusStoreReader;
  private final ReadWriteStoreRepository storeRepository;
  private final int daVinciPushStatusScanPeriodInSeconds;
  private final ScheduledExecutorService offlinePushCheckScheduler = Executors.newScheduledThreadPool(1);

  public PushStatusCollector(
      ReadWriteStoreRepository storeRepository,
      PushStatusStoreReader pushStatusStoreReader,
      Consumer<String> pushCompletedHandler,
      BiConsumer<String, String> pushErrorHandler,
      int daVinciPushStatusScanPeriodInSeconds) {
    this.storeRepository = storeRepository;
    this.pushStatusStoreReader = pushStatusStoreReader;
    this.pushCompletedHandler = pushCompletedHandler;
    this.pushErrorHandler = pushErrorHandler;
    this.daVinciPushStatusScanPeriodInSeconds = daVinciPushStatusScanPeriodInSeconds;
  }

  public void start() {
    offlinePushCheckScheduler
        .scheduleAtFixedRate(this::scanDaVinciPushStatus, 0, daVinciPushStatusScanPeriodInSeconds, TimeUnit.SECONDS);
  }

  public void subscribeTopic(String topicName, int partitionCount) {
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    Store store = storeRepository.getStore(storeName);
    if (store == null) {
      LOGGER.warn("Store {} not found in store repository, will not monitor Da Vinci push status", storeName);
    } else {
      if (store.isDaVinciPushStatusStoreEnabled() && Version.parseVersionFromKafkaTopicName(topicName) > 1) {
        LOGGER.info("Will monitor Da Vinci push status for topic {}", topicName);
        topicToPushStatusMap.put(topicName, new TopicPushStatus(partitionCount));
      }
    }
  }

  public void unsubscribeTopic(String topicName) {
    topicToPushStatusMap.remove(topicName);
  }

  public void handleServerPushStatusUpdate(String topicName, ExecutionStatus executionStatus, String detailsString) {

    TopicPushStatus topicPushStatus = topicToPushStatusMap.get(topicName);
    if (topicPushStatus == null) {
      if (executionStatus.equals(ExecutionStatus.COMPLETED)) {
        pushCompletedHandler.accept(topicName);
      } else if (executionStatus.equals(ExecutionStatus.ERROR)) {
        pushErrorHandler.accept(topicName, detailsString);
      }
    } else {
      // Update the server topic status in the data structure and wait for async DVC status scan thread to pick up.
      topicPushStatus.setServerStatus(new ExecutionStatusWithDetails(executionStatus, detailsString));
    }
  }

  private void scanDaVinciPushStatus() {
    for (Map.Entry<String, TopicPushStatus> entry: topicToPushStatusMap.entrySet()) {
      String topicName = entry.getKey();
      TopicPushStatus pushStatus = entry.getValue();
      if (!pushStatus.isMonitoring()) {
        continue;
      }
      ExecutionStatusWithDetails daVinciStatus;
      if (pushStatus.getDaVinciStatus() != null && pushStatus.getDaVinciStatus().getStatus().isTerminal()) {
        daVinciStatus = pushStatus.getDaVinciStatus();
      } else {
        daVinciStatus = PushMonitorUtils.getDaVinciPushStatusAndDetails(
            pushStatusStoreReader,
            topicName,
            pushStatus.getPartitionCount(),
            Optional.empty());
        pushStatus.setDaVinciStatus(daVinciStatus);
      }
      ExecutionStatusWithDetails serverStatus = pushStatus.getServerStatus();
      if (serverStatus == null) {
        continue;
      }
      LOGGER.info(
          "Topic server push status: {}, Da Vinci push status: {}",
          serverStatus.getStatus(),
          daVinciStatus.getStatus());
      if (isOverallPushCompleted(serverStatus, daVinciStatus)) {
        pushStatus.setMonitoring(false);
        pushCompletedHandler.accept(topicName);
      } else if (isOverallPushError(serverStatus, daVinciStatus)) {
        pushStatus.setMonitoring(false);
        String overallErrorString = "";
        if (serverStatus.getStatus().equals(ExecutionStatus.ERROR)) {
          overallErrorString = overallErrorString + "Server push error: " + serverStatus.getDetails() + "\n";
        }
        if (daVinciStatus.getStatus().equals(ExecutionStatus.ERROR)) {
          overallErrorString = overallErrorString + "Da Vinci push error: " + daVinciStatus.getDetails() + "\n";
        }
        pushErrorHandler.accept(topicName, overallErrorString);
      }
    }
  }

  private boolean isOverallPushCompleted(
      ExecutionStatusWithDetails serverStatus,
      ExecutionStatusWithDetails daVinciStatus) {
    return serverStatus.getStatus().equals(ExecutionStatus.COMPLETED)
        && daVinciStatus.getStatus().equals(ExecutionStatus.COMPLETED);
  }

  private boolean isOverallPushError(
      ExecutionStatusWithDetails serverStatus,
      ExecutionStatusWithDetails daVinciStatus) {
    return serverStatus.getStatus().equals(ExecutionStatus.ERROR)
        || daVinciStatus.getStatus().equals(ExecutionStatus.ERROR);
  }

  public void clear() {
    topicToPushStatusMap.clear();
  }

  // Visible for testing.
  Map<String, TopicPushStatus> getTopicToPushStatusMap() {
    return topicToPushStatusMap;
  }

  class TopicPushStatus {
    private final int partitionCount;
    private ExecutionStatusWithDetails serverStatus;
    private ExecutionStatusWithDetails daVinciStatus;

    private boolean isMonitoring;

    public TopicPushStatus(int partitionCount) {
      this.partitionCount = partitionCount;
      this.isMonitoring = true;
    }

    public int getPartitionCount() {
      return partitionCount;
    }

    public void setMonitoring(boolean monitoring) {
      isMonitoring = monitoring;
    }

    public boolean isMonitoring() {
      return isMonitoring;
    }

    public void setServerStatus(ExecutionStatusWithDetails serverStatus) {
      this.serverStatus = serverStatus;
    }

    public ExecutionStatusWithDetails getServerStatus() {
      return serverStatus;
    }

    public void setDaVinciStatus(ExecutionStatusWithDetails daVinciStatus) {
      this.daVinciStatus = daVinciStatus;
    }

    public ExecutionStatusWithDetails getDaVinciStatus() {
      return daVinciStatus;
    }
  }
}
