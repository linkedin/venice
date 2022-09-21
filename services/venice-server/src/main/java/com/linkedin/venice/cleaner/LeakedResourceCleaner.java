package com.linkedin.venice.cleaner;

import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.LeakedResourceCleanerStats;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * LeakedResourceCleaner is a background thread which wakes up regularly
 * to release leaked resources from local disk.
 */
public class LeakedResourceCleaner extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(LeakedResourceCleaner.class);

  private final long pollIntervalMs;
  private LeakedResourceCleanerRunnable cleaner;
  private Thread runner;
  private final StorageEngineRepository storageEngineRepository;
  private final ReadOnlyStoreRepository storeRepository;
  private final StoreIngestionService ingestionService;
  private final StorageService storageService;
  private final LeakedResourceCleanerStats stats;
  private long nonExistentStoreCleanupInterval = Time.MS_PER_DAY;

  public LeakedResourceCleaner(
      StorageEngineRepository storageEngineRepository,
      long pollIntervalMs,
      ReadOnlyStoreRepository storeRepository,
      StoreIngestionService ingestionService,
      StorageService storageService,
      MetricsRepository metricsRepository) {
    this.storageEngineRepository = storageEngineRepository;
    this.pollIntervalMs = pollIntervalMs;
    this.storeRepository = storeRepository;
    this.ingestionService = ingestionService;
    this.storageService = storageService;
    this.stats = new LeakedResourceCleanerStats(metricsRepository);
  }

  @Override
  public boolean startInner() {
    cleaner = new LeakedResourceCleanerRunnable(storageEngineRepository);
    runner = new Thread(cleaner);
    runner.setName("Storage Leaked Resource cleaner");
    runner.setDaemon(true);
    runner.start();

    return true;
  }

  @Override
  public void stopInner() {
    cleaner.setStop();
    runner.interrupt();
  }

  private class LeakedResourceCleanerRunnable implements Runnable {
    private StorageEngineRepository storageEngineRepository;
    private volatile boolean stop = false;
    Map<String, Long> nonExistentStoreToCheckedTimestamp = new HashMap<>();

    public LeakedResourceCleanerRunnable(StorageEngineRepository storageEngineRepository) {
      this.storageEngineRepository = storageEngineRepository;
    }

    protected void setStop() {
      stop = true;
    }

    @Override
    public void run() {
      while (!stop) {
        try {
          Thread.sleep(pollIntervalMs);
        } catch (InterruptedException e) {
          LOGGER.info("Received interruptedException while running LeakedResourceCleanerRunnable, will exit");
          break;
        }

        List<AbstractStorageEngine> storageEngines = storageEngineRepository.getAllLocalStorageEngines();
        for (AbstractStorageEngine storageEngine: storageEngines) {
          String resourceName = storageEngine.getStoreName();
          try {
            Store store;
            String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
            int version = Version.parseVersionFromKafkaTopicName(resourceName);
            try {
              store = storeRepository.getStoreOrThrow(storeName);
            } catch (VeniceNoStoreException e) {
              long currentTime = System.currentTimeMillis();
              if (nonExistentStoreToCheckedTimestamp.containsKey(storeName)) {
                long timestamp = nonExistentStoreToCheckedTimestamp.get(storeName);
                // If store is reported missing for more than a day by the store repo, delete the resources.
                if (timestamp + nonExistentStoreCleanupInterval < currentTime) {
                  LOGGER.info("Store: {} is not hosted by this host, it's resources will be cleaned up.", storeName);
                  storageService.removeStorageEngine(resourceName);
                  LOGGER.info("Resource: {} has been cleaned up.", resourceName);
                  stats.recordLeakedVersion();
                  nonExistentStoreToCheckedTimestamp.remove(storeName);
                }
              } else {
                nonExistentStoreToCheckedTimestamp.put(storeName, currentTime);
              }
              continue;
            }

            nonExistentStoreToCheckedTimestamp.remove(storeName);

            if (store.getVersions().isEmpty()) {
              /**
               * Defensive code, and if the version list in this store is empty, but we found a resource to clean up,
               * and it might be caused by Zookeeper issue in the extreme scenario, and we will skip the resource cleanup
               * for this store.
               */
              LOGGER.warn(
                  "Found no version for store: {}, but a lingering resource: {}, which is suspicious, so here will skip the cleanup.",
                  storeName,
                  resourceName);
              continue;
            }
            // The version has already been deleted.
            if (!store.getVersion(version).isPresent() &&
            /**
             * This is to avoid the race condition since Version will be deleted in Controller first and the
             * actual cleanup in storage node is being handled in {@link ParticipantStoreConsumptionTask}.
             *
             * If there is no version existed in ZK and no running ingestion task for current resource, it
             * is safe to be deleted.
             */
                !ingestionService.containsRunningConsumption(resourceName)) {
              LOGGER.info(
                  "Resource: {} doesn't have either the corresponding version stored in ZK, or a running ingestion task, so it will be cleaned up.",
                  resourceName);
              storageService.removeStorageEngine(resourceName);
              LOGGER.info("Resource: {} has been cleaned up.", resourceName);
              stats.recordLeakedVersion();
            }
          } catch (Exception e) {
            LOGGER.error("Received exception while verifying/cleaning up resource: {}", resourceName, e);
          }
        } // ~for
      } // ~while
    }
  }

  /* test-only */
  void setNonExistentStoreCleanupInterval(long timeInterval) {
    nonExistentStoreCleanupInterval = timeInterval;
  }
}
