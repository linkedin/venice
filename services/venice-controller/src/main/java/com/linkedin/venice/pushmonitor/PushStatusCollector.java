package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PushStatusCollector {
  private static final Logger LOGGER = LogManager.getLogger(PushStatusCollector.class);

  private final Consumer<String> pushCompletedHandler;
  private final BiConsumer<String, String> pushErrorHandler;
  private final Map<String, TopicPushStatus> daVinciStoreTopicToPartitionCountMap = new VeniceConcurrentHashMap<>();

  private final PushStatusStoreReader pushStatusStoreReader;

  private final ReadWriteStoreRepository storeRepository;

  private final ReentrantLock lock = new ReentrantLock();

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
        lock.lock();
        daVinciStoreTopicToPartitionCountMap.put(topicName, new TopicPushStatus(topicName, partitionCount));
        lock.unlock();
      }
    }
  }

  public void unsubscribeTopic(String topicName) {
    lock.lock();
    daVinciStoreTopicToPartitionCountMap.remove(topicName);
    lock.unlock();
  }

  public void handleServerPushStatusUpdate(String topicName, ExecutionStatus executionStatus, String detailsString) {

    TopicPushStatus topicPushStatus = daVinciStoreTopicToPartitionCountMap.get(topicName);
    if (topicPushStatus == null) {
      if (executionStatus.equals(ExecutionStatus.COMPLETED)) {
        pushCompletedHandler.accept(topicName);
      } else if (executionStatus.equals(ExecutionStatus.ERROR)) {
        pushErrorHandler.accept(topicName, detailsString);
      }
    } else {
      // Update the server topic status in the data structure and wait for async DVC status scan thread to pick up.
      lock.lock();
      topicPushStatus.setServerStatus(new ExecutionStatusWithDetails(executionStatus, detailsString));
      lock.unlock();
    }
  }

  private void scanDaVinciPushStatus() {
    lock.lock();
    for (Map.Entry<String, TopicPushStatus> entry: daVinciStoreTopicToPartitionCountMap.entrySet()) {
      String topicName = entry.getKey();
      TopicPushStatus pushStatus = entry.getValue();
      if (!pushStatus.isMonitoring()) {
        continue;
      }
      ExecutionStatusWithDetails daVinciStatus = null;
      if (pushStatus.getDaVinciStatus() != null && pushStatus.getDaVinciStatus().getStatus().isTerminal()) {
        daVinciStatus = pushStatus.getDaVinciStatus();
      } else {
        Pair<ExecutionStatus, String> topicDaVinciPushStatus = PushMonitorUtils.getDaVinciPushStatusAndDetails(
            pushStatusStoreReader,
            topicName,
            pushStatus.getPartitionCount(),
            Optional.empty());

        daVinciStatus =
            new ExecutionStatusWithDetails(topicDaVinciPushStatus.getFirst(), topicDaVinciPushStatus.getSecond());
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
    lock.unlock();
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
    lock.lock();
    daVinciStoreTopicToPartitionCountMap.clear();
    lock.unlock();
  }

  // Visible for testing.
  Map<String, TopicPushStatus> getDaVinciStoreTopicToPartitionCountMap() {
    return daVinciStoreTopicToPartitionCountMap;
  }

  class TopicPushStatus {
    private final String topicName;
    private final int partitionCount;
    private ExecutionStatusWithDetails serverStatus;
    private ExecutionStatusWithDetails daVinciStatus;

    private boolean isMonitoring;

    public TopicPushStatus(String topicName, int partitionCount) {
      this.topicName = topicName;
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
