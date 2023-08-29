package com.linkedin.venice.controller.systemstore;

import static java.lang.Thread.currentThread;

import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.stats.SystemStoreCheckStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.system.store.MetaStoreReader;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is used to schedule periodic check of user system stores in Venice cluster to make sure system stores has
 * good version and is ingesting correctly. This service is expected to be setup in the leader controller of the cluster,
 * and it will periodically scan all user system stores and collect bad system stores. Parent controller will hit the
 * controller endpoint to collect information about bad system stores.
 */
public class SystemStoreHealthCheckService extends AbstractVeniceService {
  public static final Logger LOGGER = LogManager.getLogger(SystemStoreHealthCheckService.class);
  private final ReadWriteStoreRepository storeRepository;
  private final PushStatusStoreReader pushStatusStoreReader;
  private final PushStatusStoreWriter pushStatusStoreWriter;
  private final MetaStoreReader metaStoreReader;
  private final MetaStoreWriter metaStoreWriter;
  private final AtomicLong badMetaStoreCount = new AtomicLong(0);
  private final AtomicLong badPushStatusStoreCount = new AtomicLong(0);

  private final int checkPeriodInSeconds;
  private final int heartbeatWaitTimeSeconds;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final AtomicReference<Set<String>> unhealthySystemStoreSet = new AtomicReference<>();
  private final SystemStoreCheckStats systemStoreCheckStats;
  private ScheduledExecutorService checkServiceExecutor;

  public SystemStoreHealthCheckService(
      ReadWriteStoreRepository storeRepository,
      MetricsRepository metricsRepository,
      String clusterName,
      MetaStoreReader metaStoreReader,
      MetaStoreWriter metaStoreWriter,
      PushStatusStoreReader pushStatusStoreReader,
      PushStatusStoreWriter pushStatusStoreWriter,
      int systemStoreCheckPeriodInSeconds,
      int systemStoreHealthCheckHeartbeatWaitTimeSeconds) {
    this.storeRepository = storeRepository;
    this.metaStoreWriter = metaStoreWriter;
    this.metaStoreReader = metaStoreReader;
    this.pushStatusStoreWriter = pushStatusStoreWriter;
    this.pushStatusStoreReader = pushStatusStoreReader;
    this.checkPeriodInSeconds = systemStoreCheckPeriodInSeconds;
    this.heartbeatWaitTimeSeconds = systemStoreHealthCheckHeartbeatWaitTimeSeconds;
    this.unhealthySystemStoreSet.set(new HashSet<>());
    this.systemStoreCheckStats =
        new SystemStoreCheckStats(metricsRepository, clusterName, badMetaStoreCount::get, badPushStatusStoreCount::get);
  }

  /**
   * Return unhealthy system store name set. This API is expected to be called by parent controller.
   */
  public Set<String> getUnhealthySystemStoreSet() {
    return unhealthySystemStoreSet.get();
  }

  @Override
  public boolean startInner() {
    checkServiceExecutor = Executors.newScheduledThreadPool(1);
    isRunning.set(true);
    checkServiceExecutor.scheduleWithFixedDelay(
        new SystemStoreHealthCheckTask(),
        checkPeriodInSeconds,
        checkPeriodInSeconds,
        TimeUnit.SECONDS);
    LOGGER.info("System store health check executor service is started.");
    return true;
  }

  @Override
  public void stopInner() {
    isRunning.set(false);
    checkServiceExecutor.shutdownNow();
    try {
      if (!checkServiceExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        LOGGER.warn("Current task in system store health check executor service is not terminated after 5 seconds.");
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }
    LOGGER.info("System store health check executor service is shutdown.");
  }

  class SystemStoreHealthCheckTask implements Runnable {
    @Override
    public void run() {
      if (!getIsRunning().get()) {
        return;
      }
      Set<String> newUnhealthySystemStoreSet = new HashSet<>();
      Map<String, Long> systemStoreToHeartbeatTimestampMap = new VeniceConcurrentHashMap<>();
      checkAndSendHeartbeatToSystemStores(newUnhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);
      try {
        // Sleep for enough time for system store to consume heartbeat messages.
        Thread.sleep(TimeUnit.SECONDS.toMillis(heartbeatWaitTimeSeconds));
      } catch (InterruptedException e) {
        LOGGER.info("Caught interrupted exception, will exit now.");
        return;
      }

      checkSystemStoreHeartbeat(newUnhealthySystemStoreSet, systemStoreToHeartbeatTimestampMap);
      if (!getIsRunning().get()) {
        return;
      }
      // Update the unhealthy system store set.
      unhealthySystemStoreSet.set(newUnhealthySystemStoreSet);
      long newBadMetaSystemStoreCount = newUnhealthySystemStoreSet.stream()
          .filter(x -> VeniceSystemStoreType.getSystemStoreType(x).equals(VeniceSystemStoreType.META_STORE))
          .count();
      badMetaStoreCount.set(newBadMetaSystemStoreCount);
      badPushStatusStoreCount.set(newUnhealthySystemStoreSet.size() - newBadMetaSystemStoreCount);
      LOGGER.info("Collected unhealthy system stores: {}", newUnhealthySystemStoreSet.toString());
    }
  }

  void checkAndSendHeartbeatToSystemStores(
      Set<String> newUnhealthySystemStoreSet,
      Map<String, Long> systemStoreToHeartbeatTimestampMap) {
    for (Store store: getStoreRepository().getAllStores()) {
      if (!getIsRunning().get()) {
        return;
      }
      // For user store, if corresponding system store flag is not true, it indicates system store is not created.
      if (!VeniceSystemStoreUtils.isUserSystemStore(store.getName())) {
        if (!store.isDaVinciPushStatusStoreEnabled()) {
          newUnhealthySystemStoreSet
              .add(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(store.getName()));
        }
        if (!store.isStoreMetaSystemStoreEnabled()) {
          newUnhealthySystemStoreSet.add(VeniceSystemStoreType.META_STORE.getSystemStoreName(store.getName()));
        }
        continue;
      }
      /**
       * System store does not have an online serving version.
       */
      if (store.getCurrentVersion() == 0) {
        newUnhealthySystemStoreSet.add(store.getName());
        continue;
      }
      VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(store.getName());
      String userStoreName = systemStoreType.extractRegularStoreName(store.getName());
      long currentTimestamp = System.currentTimeMillis();
      if (VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.equals(systemStoreType)) {
        getPushStatusStoreWriter().writeHeartbeat(userStoreName, currentTimestamp);
      } else {
        getMetaStoreWriter().writeHeartbeat(userStoreName, currentTimestamp);
      }
      systemStoreToHeartbeatTimestampMap.put(store.getName(), currentTimestamp);
    }
  }

  void checkSystemStoreHeartbeat(
      Set<String> newUnhealthySystemStoreSet,
      Map<String, Long> systemStoreToHeartbeatTimestampMap) {
    for (Map.Entry<String, Long> entry: systemStoreToHeartbeatTimestampMap.entrySet()) {
      if (!getIsRunning().get()) {
        return;
      }
      if (!isSystemStoreIngesting(entry.getKey(), entry.getValue())) {
        newUnhealthySystemStoreSet.add(entry.getKey());
      }
    }
  }

  boolean isSystemStoreIngesting(String systemStoreName, long heartbeatTimestamp) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(systemStoreName);
    String userStoreName = systemStoreType.extractRegularStoreName(systemStoreName);
    try {
      return RetryUtils.executeWithMaxRetriesAndFixedAttemptDuration(() -> {
        long retrievedTimestamp;
        if (systemStoreType == VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE) {
          retrievedTimestamp = getPushStatusStoreReader()
              .getHeartbeat(userStoreName, PushStatusStoreUtils.CONTROLLER_HEARTBEAT_INSTANCE_NAME);
        } else {
          retrievedTimestamp = getMetaStoreReader().getHeartbeat(userStoreName);
        }
        if (retrievedTimestamp < heartbeatTimestamp) {
          throw new VeniceException("Heartbeat not refreshed.");
        }
        return true;
      }, 3, Duration.ofSeconds(1), Collections.singletonList(VeniceException.class));
    } catch (VeniceException e) {
      return false;
    }
  }

  MetaStoreReader getMetaStoreReader() {
    return metaStoreReader;
  }

  PushStatusStoreReader getPushStatusStoreReader() {
    return pushStatusStoreReader;
  }

  MetaStoreWriter getMetaStoreWriter() {
    return metaStoreWriter;
  }

  PushStatusStoreWriter getPushStatusStoreWriter() {
    return pushStatusStoreWriter;
  }

  AtomicBoolean getIsRunning() {
    return isRunning;
  }

  ReadWriteStoreRepository getStoreRepository() {
    return storeRepository;
  }
}
