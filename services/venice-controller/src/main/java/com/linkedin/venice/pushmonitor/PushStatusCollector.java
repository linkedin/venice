package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
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
  private final ExecutorService pushStatusStoreScanExecutor;
  private final boolean daVinciPushStatusScanEnabled;

  public PushStatusCollector(
      ReadWriteStoreRepository storeRepository,
      PushStatusStoreReader pushStatusStoreReader,
      Consumer<String> pushCompletedHandler,
      BiConsumer<String, String> pushErrorHandler,
      boolean daVinciPushStatusScanEnabled,
      int daVinciPushStatusScanIntervalInSeconds,
      int pushStatusScanThreadNumber) {
    this.storeRepository = storeRepository;
    this.pushStatusStoreReader = pushStatusStoreReader;
    this.pushCompletedHandler = pushCompletedHandler;
    this.pushErrorHandler = pushErrorHandler;
    this.daVinciPushStatusScanEnabled = daVinciPushStatusScanEnabled;
    this.daVinciPushStatusScanPeriodInSeconds = daVinciPushStatusScanIntervalInSeconds;
    this.pushStatusStoreScanExecutor = Executors.newFixedThreadPool(pushStatusScanThreadNumber);
  }

  public void start() {
    if (daVinciPushStatusScanEnabled) {
      offlinePushCheckScheduler
          .scheduleAtFixedRate(this::scanDaVinciPushStatus, 0, daVinciPushStatusScanPeriodInSeconds, TimeUnit.SECONDS);
    } else {
      LOGGER.warn("Offline push monitoring Da Vinci push status is not enabled, will only check server push status.");
    }
  }

  public void subscribeTopic(String topicName, int partitionCount) {
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    Store store = storeRepository.getStore(storeName);
    if (store == null) {
      LOGGER.warn("Store {} not found in store repository, will not monitor Da Vinci push status", storeName);
    } else {
      if (daVinciPushStatusScanEnabled && store.isDaVinciPushStatusStoreEnabled()
          && Version.parseVersionFromKafkaTopicName(topicName) > 1) {
        LOGGER.info("Will monitor Da Vinci push status for topic {}", topicName);
        topicToPushStatusMap.put(topicName, new TopicPushStatus(topicName, partitionCount));
      }
    }
  }

  public void unsubscribeTopic(String topicName) {
    topicToPushStatusMap.remove(topicName);
  }

  private void scanDaVinciPushStatus() {
    List<CompletableFuture<TopicPushStatus>> resultList = new ArrayList<>();
    for (Map.Entry<String, TopicPushStatus> entry: topicToPushStatusMap.entrySet()) {
      String topicName = entry.getKey();
      TopicPushStatus pushStatus = entry.getValue();
      if (!pushStatus.isMonitoring()) {
        continue;
      }
      if (pushStatus.getDaVinciStatus() != null && pushStatus.getDaVinciStatus().getStatus().isTerminal()) {
        resultList.add(CompletableFuture.completedFuture(pushStatus));
      } else {
        resultList.add(CompletableFuture.supplyAsync(() -> {
          ExecutionStatusWithDetails statusWithDetails = PushMonitorUtils.getDaVinciPushStatusAndDetails(
              pushStatusStoreReader,
              topicName,
              pushStatus.getPartitionCount(),
              Optional.empty());
          pushStatus.setDaVinciStatus(statusWithDetails);
          return pushStatus;
        }, pushStatusStoreScanExecutor));
      }
    }
    // Collect the executor result and compute aggregate results for ongoing pushes.
    for (CompletableFuture<TopicPushStatus> future: resultList) {
      TopicPushStatus pushStatus;
      try {
        pushStatus = future.get();
      } catch (Exception e) {
        LOGGER.error("Caught exception when getting future result of push status.", e);
        continue;
      }
      ExecutionStatusWithDetails daVinciStatus = pushStatus.getDaVinciStatus();
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
        pushCompletedHandler.accept(pushStatus.getTopicName());
      } else if (isOverallPushError(serverStatus, daVinciStatus)) {
        pushStatus.setMonitoring(false);
        StringBuilder pushErrorDetailStringBuilder = new StringBuilder();
        if (serverStatus.getStatus().equals(ExecutionStatus.ERROR)) {
          pushErrorDetailStringBuilder.append("Server push error: ").append(serverStatus.getDetails()).append("\n");
        }
        if (daVinciStatus.getStatus().equals(ExecutionStatus.ERROR)) {
          pushErrorDetailStringBuilder.append("Da Vinci push error: ").append(daVinciStatus.getDetails()).append("\n");
        }
        pushErrorHandler.accept(pushStatus.getTopicName(), pushErrorDetailStringBuilder.toString());
      }
    }
  }

  public void handleServerPushStatusUpdate(String topicName, ExecutionStatus executionStatus, String detailsString) {

    TopicPushStatus topicPushStatus = topicToPushStatusMap.get(topicName);
    if ((!daVinciPushStatusScanEnabled) || topicPushStatus == null) {
      if (executionStatus.equals(ExecutionStatus.COMPLETED)) {
        pushCompletedHandler.accept(topicName);
      } else if (executionStatus.equals(ExecutionStatus.ERROR)) {
        pushErrorHandler.accept(topicName, detailsString);
      }
    } else {
      // Update the server topic status in the data structure and wait for async DVC status scan thread to pick up.
      topicToPushStatusMap.computeIfPresent(topicName, (topic, pushStatus) -> {
        pushStatus.setServerStatus(new ExecutionStatusWithDetails(executionStatus, detailsString));
        return pushStatus;
      });
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

  static class TopicPushStatus {
    private final String topicName;
    private final int partitionCount;
    private ExecutionStatusWithDetails serverStatus;
    private ExecutionStatusWithDetails daVinciStatus;

    private boolean isMonitoring;

    public TopicPushStatus(String topicName, int partitionCount) {
      this.partitionCount = partitionCount;
      this.isMonitoring = true;
      this.topicName = topicName;
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

    public String getTopicName() {
      return topicName;
    }
  }
}
