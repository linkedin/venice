package com.linkedin.venice.cleaner;

import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.server.StorageEngineRepository;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.LeakedResourceCleanerStats;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.store.AbstractStorageEngine;
import io.tehuti.metrics.MetricsRepository;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * LeakedResourceCleaner is a background thread which wakes up regularly
 * to release leaked resources from local disk.
 */
public class LeakedResourceCleaner extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(LeakedResourceCleaner.class);

  private final long pollIntervalMs;
  private LeakedResourceCleanerRunnable cleaner;
  private Thread runner;
  private final StorageEngineRepository storageEngineRepository;
  private final ReadOnlyStoreRepository storeRepository;
  private final StoreIngestionService ingestionService;
  private final StorageService storageService;
  private final LeakedResourceCleanerStats stats;

  public LeakedResourceCleaner(StorageEngineRepository storageEngineRepository, long pollIntervalMs,
      ReadOnlyStoreRepository storeRepository, StoreIngestionService ingestionService, StorageService storageService,
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
    public LeakedResourceCleanerRunnable(StorageEngineRepository storageEngineRepository) {
      this.storageEngineRepository = storageEngineRepository;
    }

    protected void setStop(){
      stop = true;
    }

    @Override
    public void run() {
      while (!stop) {
        try {
          Thread.sleep(pollIntervalMs);

          List<AbstractStorageEngine> storageEngines = storageEngineRepository.getAllLocalStorageEngines();
          for (AbstractStorageEngine storageEngine : storageEngines) {
            String resourceName = storageEngine.getName();
            try {
              String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
              int version = Version.parseVersionFromKafkaTopicName(resourceName);
              Store store = storeRepository.getStore(storeName);
              if (store.getVersions().isEmpty()) {
                /**
                 * Defensive code, and if the version list in this store is empty, but we found a resource to clean up,
                 * and it might be caused by Zookeeper issue in the extreme scenario, and we will skip the resource cleanup
                 * for this store.
                 */
                logger.warn("Found no version for store: " + storeName + ", but a lingering resource: " + resourceName +
                    ", which is suspicious, so here will skip the cleanup.");
                continue;
              }
              if (!store.getVersion(version).isPresent() &&  // The version has already been deleted
                  /**
                   * This is to avoid the race condition since Version will be deleted in Controller first and the
                   * actual cleanup in storage node is being handled in {@link com.linkedin.venice.kafka.consumer.ParticipantStoreConsumptionTask}.
                   *
                   * If there is no version existed in ZK and no running ingestion task for current resource, it
                   * is safe to be deleted.
                   */
                  !ingestionService.containsRunningConsumption(resourceName)) {
                logger.info("Resource: " + resourceName + " doesn't have either the corresponding version stored in ZK, or a running ingestion task, so it will be cleaned up.");
                storageService.removeStorageEngine(resourceName);
                logger.info("Resource: " + resourceName + " has been cleaned up.");
                stats.recordLeakedVersion();
              }
            } catch (Exception e) {
              logger.error("Received exception while verifying/cleaning up resource: " + resourceName);
            }
          } //~for
        } catch (InterruptedException e)  {
          logger.info("Received interruptedException, will exit");
          break;
        } catch (Exception e) {
          logger.error("Received exception while cleaning up resources", e);
        }
      } //~while
    }
  }
}
