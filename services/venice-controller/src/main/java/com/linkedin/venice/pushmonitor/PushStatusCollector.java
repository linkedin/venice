package com.linkedin.venice.pushmonitor;

import static java.lang.Thread.currentThread;

import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PushStatusCollector {
  private static final Logger LOGGER = LogManager.getLogger(PushStatusCollector.class);

  private final Map<String, ExecutionStatus> topicToDaVinciPushStatusMap = new VeniceConcurrentHashMap<>();
  private final Map<String, ExecutionStatus> topicToServerPushStatusMap = new VeniceConcurrentHashMap<>();
  private final Consumer<String> pushCompletedHandler;
  private final BiConsumer<String, String> pushErrorHandler;
  private final Set<String> subscribedTopicSet = new HashSet<>();
  private final Map<String, Integer> topicToPartitionCountMap = new VeniceConcurrentHashMap<>();
  private final Set<String> daVinciPushStatusEnabledTopicSet = new HashSet<>();
  private final PushStatusStoreReader pushStatusStoreReader;

  private final ReadWriteStoreRepository storeRepository;

  private final ScheduledExecutorService offlinePushCheckScheduler = Executors.newScheduledThreadPool(1);

  public PushStatusCollector(
      ReadWriteStoreRepository storeRepository,
      PushStatusStoreReader pushStatusStoreReader,
      Consumer<String> pushCompletedHandler,
      BiConsumer<String, String> pushErrorHandler) {
    this.storeRepository = storeRepository;
    this.pushStatusStoreReader = pushStatusStoreReader;
    this.pushCompletedHandler = pushCompletedHandler;
    this.pushErrorHandler = pushErrorHandler;
  }

  public void subscribeTopic(String topicName, int partitionCount) {
    subscribedTopicSet.add(topicName);
    topicToPartitionCountMap.put(topicName, partitionCount);
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    Store store = storeRepository.getStore(storeName);
    if (store == null) {
      LOGGER.warn("Store {} not found in store repository, will not monitor Da Vinci push status", storeName);
    } else {
      if (store.isDaVinciPushStatusStoreEnabled()) {
        daVinciPushStatusEnabledTopicSet.add(topicName);
      }
    }
  }

  public void unsubscribeTopic(String topicName) {
    subscribedTopicSet.remove(topicName);
    daVinciPushStatusEnabledTopicSet.remove(topicName);
  }

  public void handleServerPushStatusUpdate(
      String topicName,
      ExecutionStatus executionStatus,
      Optional<String> detailsString) {
    if (!daVinciPushStatusEnabledTopicSet.contains(topicName)) {
      if (executionStatus.equals(ExecutionStatus.COMPLETED)) {
        pushCompletedHandler.accept(topicName);
      } else if (executionStatus.equals(ExecutionStatus.ERROR)) {
        pushErrorHandler.accept(topicName, null);
      }
    } else {
      // Update the server topic status in the data structure and wait for async DVC status scan thread to pick up.
      topicToServerPushStatusMap.put(topicName, executionStatus);
    }
  }

  public void scanDaVinciPushStatus() {
    for (String topicName: daVinciPushStatusEnabledTopicSet) {
      Pair<ExecutionStatus, String> topicDaVinciPushStatus = PushMonitorUtils.getDaVinciPushStatusAndDetails(
          pushStatusStoreReader,
          topicName,
          topicToPartitionCountMap.get(topicName),
          Optional.empty());
      // TODO: Create a new object to wrap ExecutionStatus + StatusDetails.
      topicToDaVinciPushStatusMap.put(topicName, topicDaVinciPushStatus.getFirst());
      if (isOverallPushCompleted(topicName)) {
        daVinciPushStatusEnabledTopicSet.remove(topicName);
        subscribedTopicSet.remove(topicName);
        pushCompletedHandler.accept(topicName);
      } else if (isOverallPushError(topicName)) {
        daVinciPushStatusEnabledTopicSet.remove(topicName);
        subscribedTopicSet.remove(topicName);
        pushErrorHandler.accept(topicName, "TODO: Add DVC status details");
      }
    }
  }

  public boolean isOverallPushCompleted(String topicName) {
    ExecutionStatus serverStatus = topicToServerPushStatusMap.getOrDefault(topicName, ExecutionStatus.UNKNOWN);
    ExecutionStatus daVinciStatus = topicToDaVinciPushStatusMap.getOrDefault(topicName, ExecutionStatus.UNKNOWN);
    return serverStatus.equals(ExecutionStatus.COMPLETED) && daVinciStatus.equals(ExecutionStatus.COMPLETED);
  }

  public boolean isOverallPushError(String topicName) {
    ExecutionStatus serverStatus = topicToServerPushStatusMap.getOrDefault(topicName, ExecutionStatus.UNKNOWN);
    ExecutionStatus daVinciStatus = topicToDaVinciPushStatusMap.getOrDefault(topicName, ExecutionStatus.UNKNOWN);
    return serverStatus.equals(ExecutionStatus.ERROR) || daVinciStatus.equals(ExecutionStatus.ERROR);
  }

  public void start() {
    offlinePushCheckScheduler.scheduleAtFixedRate(this::scanDaVinciPushStatus, 0, 30, TimeUnit.SECONDS);
  }

  public void stop() {
    offlinePushCheckScheduler.shutdown();
    try {
      if (!offlinePushCheckScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        offlinePushCheckScheduler.shutdownNow();
        LOGGER.info("offlinePushCheckScheduler has been shutdown.");
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }
  }
}
