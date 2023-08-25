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
import com.linkedin.venice.system.store.MetaStoreReader;
import com.linkedin.venice.system.store.MetaStoreWriter;
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
  private final MetaStoreReader metaStoreReader;
  private final MetaStoreWriter metaStoreWriter;

  private final int checkPeriodInSeconds;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private ScheduledExecutorService checkServiceExecutor;

  private Set<String> unhealthySystemStoreSet = new HashSet<>();

  public SystemStoreHealthCheckService(
      ReadWriteStoreRepository storeRepository,
      MetaStoreReader metaStoreReader,
      MetaStoreWriter metaStoreWriter,
      PushStatusStoreReader pushStatusStoreReader,
      PushStatusStoreWriter pushStatusStoreWriter,
      int systemStoreCheckPeriodInSeconds) {
    this.storeRepository = storeRepository;
    this.metaStoreWriter = metaStoreWriter;
    this.metaStoreReader = metaStoreReader;
    this.pushStatusStoreWriter = pushStatusStoreWriter;
    this.pushStatusStoreReader = pushStatusStoreReader;
    this.checkPeriodInSeconds = systemStoreCheckPeriodInSeconds;
  }

  /**
   * Return unhealthy system store name set. This API is expected to be called by parent controller.
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
      Map<String, Long> systemStoreToHeartbeatTimestampMap = new VeniceConcurrentHashMap<>();
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
        } else {
          metaStoreWriter.writeHeartbeat(userStoreName, currentTimestamp);
        }
        systemStoreToHeartbeatTimestampMap.put(store.getName(), currentTimestamp);
      }

      for (Map.Entry<String, Long> entry: systemStoreToHeartbeatTimestampMap.entrySet()) {
        if (!isRunning.get()) {
          return;
        }
        if (!isSystemStoreIngesting(entry.getKey(), entry.getValue())) {
          newUnhealthySystemStoreSet.add(entry.getKey());
        }
      }

      // Update the unhealthy system store set.
      unhealthySystemStoreSet = newUnhealthySystemStoreSet;
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
}
