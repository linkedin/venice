package com.linkedin.venice.controller.systemstore;

import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private final int checkPeriodInSeconds;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private ScheduledExecutorService checkServiceExecutor;

  private Set<String> unhealthySystemStoreSet = new HashSet<>();

  public SystemStoreHealthCheckService(
      ReadWriteStoreRepository storeRepository,
      PushStatusStoreReader pushStatusStoreReader,
      PushStatusStoreWriter pushStatusStoreWriter,
      int systemStoreCheckPeriodInSeconds) {
    this.storeRepository = storeRepository;
    this.pushStatusStoreWriter = pushStatusStoreWriter;
    this.pushStatusStoreReader = pushStatusStoreReader;
    this.checkPeriodInSeconds = systemStoreCheckPeriodInSeconds;
  }

  /**
   * Return unhealthy system store name set.
   */
  public Set<String> getUnhealthySystemStoreSet() {
    return unhealthySystemStoreSet;
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
    return true;
  }

  @Override
  public void stopInner() {
    isRunning.set(false);
    checkServiceExecutor.shutdownNow();
  }

  private class SystemStoreHealthCheckTask implements Runnable {
    @Override
    public void run() {
      if (!isRunning.get()) {
        return;
      }

      Set<String> newUnhealthySystemStoreSet = new HashSet<>();
      Map<String, Long> pushStatusStoreToHeartbeatTimestampMap = new VeniceConcurrentHashMap<>();
      for (Store store: storeRepository.getAllStores()) {
        if (!isRunning.get()) {
          return;
        }
        if (!VeniceSystemStoreUtils.isUserSystemStore(store.getName())) {
          continue;
        }
        // It is also possible that the store is a new store and is being empty pushed.
        if (store.getCurrentVersion() == 0) {
          newUnhealthySystemStoreSet.add(store.getName());
          continue;
        }
        VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(store.getName());
        String userStoreName = systemStoreType.extractRegularStoreName(store.getName());
        long currentTimestamp = System.currentTimeMillis();
        if (VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.equals(systemStoreType)) {
          pushStatusStoreWriter.writeHeartbeat(userStoreName, currentTimestamp);
          pushStatusStoreToHeartbeatTimestampMap.put(userStoreName, currentTimestamp);
        } else {
          // TODO: Add message for meta store.
        }
      }

      for (Map.Entry<String, Long> entry: pushStatusStoreToHeartbeatTimestampMap.entrySet()) {
        if (!isRunning.get()) {
          return;
        }
        if (!isSystemStoreIngesting(
            VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE,
            entry.getKey(),
            entry.getValue())) {
          newUnhealthySystemStoreSet
              .add(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(entry.getKey()));
        }
      }

      // Update the unhealthy system store set.
      unhealthySystemStoreSet = newUnhealthySystemStoreSet;
    }
  }

  private boolean isSystemStoreIngesting(
      VeniceSystemStoreType systemStoreType,
      String userStoreName,
      long heartbeatTimestamp) {
    try {
      return RetryUtils.executeWithMaxRetriesAndFixedAttemptDuration(() -> {
        if (systemStoreType == VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE) {
          long retrievedTimestamp = pushStatusStoreReader
              .getHeartbeat(userStoreName, PushStatusStoreUtils.CONTROLLER_HEARTBEAT_INSTANCE_NAME);
          if (retrievedTimestamp < heartbeatTimestamp) {
            throw new VeniceException("Heartbeat not refreshed.");
          }
        }
        // TODO: Add same check for meta store.
        return true;
      }, 3, Duration.ofSeconds(10), Collections.singletonList(VeniceException.class));
    } catch (VeniceException e) {
      return false;
    }
  }
}
