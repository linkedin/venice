package com.linkedin.venice.pushmonitor;

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
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PushStatusCollector {
  private static final Logger LOGGER = LogManager.getLogger(PushStatusCollector.class);

  private final Map<String, ExecutionStatusWithDetails> topicToServerPushStatusMap = new VeniceConcurrentHashMap<>();
  private final Consumer<String> pushCompletedHandler;
  private final BiConsumer<String, String> pushErrorHandler;
  private final Map<String, Integer> topicToPartitionCountMap = new VeniceConcurrentHashMap<>();
  private final Set<String> daVinciPushStatusEnabledTopicSet = new HashSet<>();
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
    lock.lock();
    topicToPartitionCountMap.put(topicName, partitionCount);
    lock.unlock();
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    Store store = storeRepository.getStore(storeName);
    if (store == null) {
      LOGGER.warn("Store {} not found in store repository, will not monitor Da Vinci push status", storeName);
    } else {
      if (store.isDaVinciPushStatusStoreEnabled() && Version.parseVersionFromKafkaTopicName(topicName) > 1) {
        LOGGER.info("Will monitor Da Vinci push status for topic {}", topicName);
        lock.lock();
        daVinciPushStatusEnabledTopicSet.add(topicName);
        lock.unlock();
      }
    }
  }

  public void unsubscribeTopic(String topicName) {
    lock.lock();
    daVinciPushStatusEnabledTopicSet.remove(topicName);
    lock.unlock();
  }

  public void handleServerPushStatusUpdate(String topicName, ExecutionStatus executionStatus, String detailsString) {
    if (!daVinciPushStatusEnabledTopicSet.contains(topicName)) {
      if (executionStatus.equals(ExecutionStatus.COMPLETED)) {
        pushCompletedHandler.accept(topicName);
      } else if (executionStatus.equals(ExecutionStatus.ERROR)) {
        pushErrorHandler.accept(topicName, null);
      }
    } else {
      // Update the server topic status in the data structure and wait for async DVC status scan thread to pick up.
      lock.lock();
      topicToServerPushStatusMap.put(topicName, new ExecutionStatusWithDetails(executionStatus, detailsString));
      lock.unlock();
    }
  }

  public void scanDaVinciPushStatus() {
    lock.lock();
    for (String topicName: daVinciPushStatusEnabledTopicSet) {
      Pair<ExecutionStatus, String> topicDaVinciPushStatus = PushMonitorUtils.getDaVinciPushStatusAndDetails(
          pushStatusStoreReader,
          topicName,
          topicToPartitionCountMap.get(topicName),
          Optional.empty());

      ExecutionStatusWithDetails daVinciStatus =
          new ExecutionStatusWithDetails(topicDaVinciPushStatus.getFirst(), topicDaVinciPushStatus.getSecond());
      ExecutionStatusWithDetails serverStatus = topicToServerPushStatusMap.get(topicName);
      if (serverStatus == null) {
        continue;
      }
      if (isOverallPushCompleted(serverStatus, daVinciStatus)) {
        daVinciPushStatusEnabledTopicSet.remove(topicName);
        pushCompletedHandler.accept(topicName);
      } else if (isOverallPushError(serverStatus, daVinciStatus)) {
        daVinciPushStatusEnabledTopicSet.remove(topicName);
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

  public boolean isOverallPushCompleted(
      ExecutionStatusWithDetails serverStatus,
      ExecutionStatusWithDetails daVinciStatus) {
    return serverStatus.getStatus().equals(ExecutionStatus.COMPLETED)
        && daVinciStatus.getStatus().equals(ExecutionStatus.COMPLETED);
  }

  public boolean isOverallPushError(ExecutionStatusWithDetails serverStatus, ExecutionStatusWithDetails daVinciStatus) {
    return serverStatus.getStatus().equals(ExecutionStatus.ERROR)
        && daVinciStatus.getStatus().equals(ExecutionStatus.ERROR);
  }

  public void clear() {
    lock.lock();
    daVinciPushStatusEnabledTopicSet.clear();
    lock.unlock();
  }
}
