package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class serves as a collector of offline push status for both Venice Server and Da Vinci clients.
 * It will try to aggregate push status from Server and Da Vinci and produce the final aggregated result.
 * If push status store is not enabled for the store, it will report directly upon receiving terminal server status,
 * otherwise it will record the server status and keep polling Da Vinci status to determine the aggregate status and will
 * only report if the aggregate status is terminal status.
 */
public class PushStatusCollector {
  private static final Logger LOGGER = LogManager.getLogger(PushStatusCollector.class);
  private final Consumer<String> pushCompletedHandler;
  private final BiConsumer<String, ExecutionStatusWithDetails> pushErrorHandler;
  private final Map<String, TopicPushStatus> topicToPushStatusMap = new VeniceConcurrentHashMap<>();
  private final PushStatusStoreReader pushStatusStoreReader;
  private final ReadWriteStoreRepository storeRepository;
  private final int daVinciPushStatusScanPeriodInSeconds;
  private final int daVinciPushStatusScanThreadNumber;
  private final boolean daVinciPushStatusScanEnabled;
  private final int daVinciPushStatusNoReportRetryMaxAttempts;
  private final int daVinciPushStatusScanMaxOfflineInstanceCount;
  private final double daVinciPushStatusScanMaxOfflineInstanceRatio;
  private ScheduledExecutorService offlinePushCheckScheduler;
  private ExecutorService pushStatusStoreScanExecutor;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  private final Map<String, Integer> topicToNoDaVinciStatusRetryCountMap = new HashMap<>();
  private final boolean useDaVinciSpecificExecutionStatusForError;

  public PushStatusCollector(
      ReadWriteStoreRepository storeRepository,
      PushStatusStoreReader pushStatusStoreReader,
      Consumer<String> pushCompletedHandler,
      BiConsumer<String, ExecutionStatusWithDetails> pushErrorHandler,
      boolean daVinciPushStatusScanEnabled,
      int daVinciPushStatusScanIntervalInSeconds,
      int daVinciPushStatusScanThreadNumber,
      int daVinciPushStatusNoReportRetryMaxAttempts,
      int daVinciPushStatusScanMaxOfflineInstanceCount,
      double daVinciPushStatusScanMaxOfflineInstanceRatio,
      boolean useDaVinciSpecificExecutionStatusForError) {
    this.storeRepository = storeRepository;
    this.pushStatusStoreReader = pushStatusStoreReader;
    this.pushCompletedHandler = pushCompletedHandler;
    this.pushErrorHandler = pushErrorHandler;
    this.daVinciPushStatusScanEnabled = daVinciPushStatusScanEnabled;
    this.daVinciPushStatusScanPeriodInSeconds = daVinciPushStatusScanIntervalInSeconds;
    this.daVinciPushStatusScanThreadNumber = daVinciPushStatusScanThreadNumber;
    this.daVinciPushStatusNoReportRetryMaxAttempts = daVinciPushStatusNoReportRetryMaxAttempts;
    this.daVinciPushStatusScanMaxOfflineInstanceCount = daVinciPushStatusScanMaxOfflineInstanceCount;
    this.daVinciPushStatusScanMaxOfflineInstanceRatio = daVinciPushStatusScanMaxOfflineInstanceRatio;
    this.useDaVinciSpecificExecutionStatusForError = useDaVinciSpecificExecutionStatusForError;
  }

  public void start() {
    if (!daVinciPushStatusScanEnabled) {
      LOGGER.warn("Offline push monitoring Da Vinci push status is not enabled, will only check server push status.");
      return;
    }

    if (isStarted.compareAndSet(false, true)) {
      if (offlinePushCheckScheduler == null || offlinePushCheckScheduler.isShutdown()) {
        offlinePushCheckScheduler = Executors.newScheduledThreadPool(1);
        LOGGER.info("Created a new offline push check scheduler");
      }
      if (pushStatusStoreScanExecutor == null || pushStatusStoreScanExecutor.isShutdown()) {
        pushStatusStoreScanExecutor = Executors.newFixedThreadPool(daVinciPushStatusScanThreadNumber);
        LOGGER.info("Created a new push status store executor with {} threads", daVinciPushStatusScanThreadNumber);
      }
      offlinePushCheckScheduler
          .scheduleAtFixedRate(this::scanDaVinciPushStatus, 0, daVinciPushStatusScanPeriodInSeconds, TimeUnit.SECONDS);
      LOGGER.info(
          "Offline push check scheduler started with {} seconds check interval",
          daVinciPushStatusScanPeriodInSeconds);
    }
  }

  public void subscribeTopic(String topicName, int partitionCount) {
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    Store store = storeRepository.getStore(storeName);
    if (store == null) {
      LOGGER.warn("Store {} not found in store repository, will not monitor Da Vinci push status", storeName);
      return;
    }
    start();
    if (daVinciPushStatusScanEnabled && store.isDaVinciPushStatusStoreEnabled()
        && Version.parseVersionFromKafkaTopicName(topicName) > 1) {
      LOGGER.info("Will monitor Da Vinci push status for topic {}", topicName);
      topicToPushStatusMap.put(topicName, new TopicPushStatus(topicName, partitionCount));
    }
  }

  public void unsubscribeTopic(String topicName) {
    topicToPushStatusMap.remove(topicName);
    topicToNoDaVinciStatusRetryCountMap.remove(topicName);
  }

  private void scanDaVinciPushStatus() {
    List<CompletableFuture<TopicPushStatus>> resultList = new ArrayList<>();
    for (Map.Entry<String, TopicPushStatus> entry: topicToPushStatusMap.entrySet()) {
      String topicName = entry.getKey();
      TopicPushStatus pushStatus = entry.getValue();
      if (!pushStatus.isMonitoring()) {
        continue;
      }
      if (pushStatus.getDaVinciStatus() != null && pushStatus.getDaVinciStatus().getStatus().isTerminal()
          && !pushStatus.getDaVinciStatus().isNoDaVinciStatusReport()) {
        resultList.add(CompletableFuture.completedFuture(pushStatus));
      } else {
        resultList.add(CompletableFuture.supplyAsync(() -> {
          ExecutionStatusWithDetails statusWithDetails = PushMonitorUtils.getDaVinciPushStatusAndDetails(
              pushStatusStoreReader,
              topicName,
              pushStatus.getPartitionCount(),
              Optional.empty(),
              daVinciPushStatusScanMaxOfflineInstanceCount,
              daVinciPushStatusScanMaxOfflineInstanceRatio,
              useDaVinciSpecificExecutionStatusForError);
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
        LOGGER.error("Caught exception when getting future result of push status : " + e.getMessage());
        continue;
      }
      ExecutionStatusWithDetails daVinciStatus = pushStatus.getDaVinciStatus();
      if (daVinciStatus.isNoDaVinciStatusReport()) {
        LOGGER.info("Received empty DaVinci status report for topic: {}", pushStatus.topicName);
        // poll DaVinci status more
        int noDaVinciStatusRetryAttempts = topicToNoDaVinciStatusRetryCountMap.compute(pushStatus.topicName, (k, v) -> {
          if (v == null) {
            return 1;
          }
          return v + 1;
        });
        if (noDaVinciStatusRetryAttempts <= daVinciPushStatusNoReportRetryMaxAttempts) {
          daVinciStatus = new ExecutionStatusWithDetails(ExecutionStatus.NOT_STARTED, daVinciStatus.getDetails());
          pushStatus.setDaVinciStatus(daVinciStatus);
        } else {
          topicToNoDaVinciStatusRetryCountMap.remove(pushStatus.topicName);
        }
      } else {
        topicToNoDaVinciStatusRetryCountMap.remove(pushStatus.topicName);
      }
      LOGGER.info(
          "Received DaVinci status: {} with details: {} for topic: {}",
          daVinciStatus.getStatus(),
          daVinciStatus.getDetails(),
          pushStatus.topicName);
      ExecutionStatusWithDetails serverStatus = pushStatus.getServerStatus();
      if (serverStatus == null) {
        continue;
      }
      LOGGER.info(
          "Topic {} server push status: {}, Da Vinci push status: {}",
          pushStatus.getTopicName(),
          serverStatus.getStatus(),
          daVinciStatus.getStatus());
      try {
        if (serverStatus.getStatus().equals(ExecutionStatus.COMPLETED)
            && daVinciStatus.getStatus().equals(ExecutionStatus.COMPLETED)) {
          pushStatus.setMonitoring(false);
          pushCompletedHandler.accept(pushStatus.getTopicName());
        } else if (serverStatus.getStatus().isError() || daVinciStatus.getStatus().isError()) {
          pushStatus.setMonitoring(false);
          ExecutionStatus errorStatus = null;
          StringBuilder pushErrorDetailStringBuilder = new StringBuilder();
          if (serverStatus.getStatus().isError()) {
            pushErrorDetailStringBuilder.append("Server push error: ").append(serverStatus.getDetails()).append("\n");
            errorStatus = serverStatus.getStatus();
          }
          if (daVinciStatus.getStatus().isError()) {
            pushErrorDetailStringBuilder.append("Da Vinci push error: ")
                .append(daVinciStatus.getDetails())
                .append("\n");
            errorStatus = daVinciStatus.getStatus();
          }
          pushErrorHandler.accept(
              pushStatus.getTopicName(),
              new ExecutionStatusWithDetails(errorStatus, pushErrorDetailStringBuilder.toString()));
        }
      } catch (Exception e) {
        LOGGER.error(
            "Caught exception when calling handler for terminal push status for topic: {}",
            pushStatus.getTopicName(),
            e);
      }
    }
  }

  public void handleServerPushStatusUpdate(String topicName, ExecutionStatus executionStatus, String detailsString) {
    // Update the server topic status in the data structure and wait for async DVC status scan thread to pick up.
    TopicPushStatus topicPushStatus = topicToPushStatusMap.computeIfPresent(topicName, (topic, pushStatus) -> {
      pushStatus.setServerStatus(new ExecutionStatusWithDetails(executionStatus, detailsString));
      return pushStatus;
    });
    // If scanning is not enabled or the topic is not subscribed for DVC push status scanning we will directly handle
    // status update.
    if ((!daVinciPushStatusScanEnabled) || topicPushStatus == null) {
      if (executionStatus.equals(ExecutionStatus.COMPLETED)) {
        pushCompletedHandler.accept(topicName);
      } else if (executionStatus.isError()) {
        pushErrorHandler.accept(topicName, new ExecutionStatusWithDetails(executionStatus, detailsString));
      }
    }
  }

  public void clear() {
    if (isStarted.compareAndSet(true, false)) {
      offlinePushCheckScheduler.shutdown();
      try {
        if (!offlinePushCheckScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          offlinePushCheckScheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      pushStatusStoreScanExecutor.shutdown();
      try {
        if (!pushStatusStoreScanExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          pushStatusStoreScanExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      topicToPushStatusMap.clear();
      topicToNoDaVinciStatusRetryCountMap.clear();
    }
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
