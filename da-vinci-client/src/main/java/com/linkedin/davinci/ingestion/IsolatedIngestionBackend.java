package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.helix.LeaderFollowerParticipantModel;
import com.linkedin.davinci.ingestion.isolated.IsolatedIngestionServer;
import com.linkedin.davinci.ingestion.regular.NativeIngestionMonitorService;
import com.linkedin.davinci.ingestion.regular.NativeIngestionRequestClient;
import com.linkedin.davinci.ingestion.regular.NativeIngestionStorageMetadataService;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.RelayNotifier;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.enums.IngestionComponentType;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.log4j.Logger;


/**
 * IsolatedIngestionBackend is the implementation of ingestion backend designed for ingestion isolation.
 * It contains references to local ingestion components - including storage metadata service, storage service and store
 * ingestion service that serves local ingestion, as well as ingestion request client that sends commands to isolated
 * ingestion service process and ingestion listener that listens to ingestion reports from child process.
 * Since RocksDB storage can only be owned by a single process, we have decided to keep metadata partition storage opened
 * in child process and in the main process, we rely on {@link NativeIngestionStorageMetadataService} to serve as the in-memory
 * metadata cache and persist the metadata updates from main process to metadata partition in child process.
 * Topic partition ingestion requests will first be sent to child process and after COMPLETED is reported, they will be
 * re-subscribed in main process to serve read traffics for user application and receive future updates.
 * The implementation of APIs in this class should consider the states in both main process and child process, as we need
 * to make sure we send the command to the correct process which holds the target storage engine.
 */
public class IsolatedIngestionBackend implements DaVinciIngestionBackend, VeniceIngestionBackend {
  private static final Logger logger = Logger.getLogger(IsolatedIngestionBackend.class);
  private final NativeIngestionStorageMetadataService storageMetadataService;
  private final StorageService storageService;
  private final KafkaStoreIngestionService kafkaStoreIngestionService;
  private final NativeIngestionRequestClient nativeIngestionRequestClient;
  private final NativeIngestionMonitorService nativeIngestionMonitorService;
  private final VeniceConfigLoader configLoader;
  private final Map<String, AtomicReference<AbstractStorageEngine>> topicStorageEngineReferenceMap = new VeniceConcurrentHashMap<>();

  private Process isolatedIngestionServiceProcess;

  public IsolatedIngestionBackend(VeniceConfigLoader configLoader, MetricsRepository metricsRepository,
      StorageMetadataService storageMetadataService, KafkaStoreIngestionService storeIngestionService, StorageService storageService) {
    int servicePort = configLoader.getVeniceServerConfig().getIngestionServicePort();
    int listenerPort = configLoader.getVeniceServerConfig().getIngestionApplicationPort();
    this.kafkaStoreIngestionService = storeIngestionService;
    this.storageMetadataService = (NativeIngestionStorageMetadataService) storageMetadataService;
    this.storageService = storageService;
    this.configLoader = configLoader;

    // Create the ingestion request client.
    nativeIngestionRequestClient = new NativeIngestionRequestClient(servicePort);
    // Create the forked isolated ingestion process.
    isolatedIngestionServiceProcess = nativeIngestionRequestClient.startForkedIngestionProcess(configLoader);
    // Create and start the ingestion report listener.
    try {
      nativeIngestionMonitorService = new NativeIngestionMonitorService(this, listenerPort, servicePort);
      nativeIngestionMonitorService.setMetricsRepository(metricsRepository);
      nativeIngestionMonitorService.setStoreIngestionService(storeIngestionService);
      nativeIngestionMonitorService.setStorageMetadataService((NativeIngestionStorageMetadataService) storageMetadataService);
      nativeIngestionMonitorService.setConfigLoader(configLoader);
      nativeIngestionMonitorService.startInner();
      logger.info("Ingestion Report Listener started.");
    } catch (Exception e) {
      throw new VeniceException("Unable to start ingestion report listener.", e);
    }
    logger.info("Created isolated ingestion backend with service port: " + servicePort + ", listener port: " + listenerPort);
  }

  @Override
  public void startConsumption(VeniceStoreConfig storeConfig, int partition) {
    if (isTopicPartitionInLocal(storeConfig.getStoreName(), partition)) {
      AbstractStorageEngine storageEngine = getStorageService().openStoreForNewPartition(storeConfig, partition);
      if (topicStorageEngineReferenceMap.containsKey(storeConfig.getStoreName())) {
        topicStorageEngineReferenceMap.get(storeConfig.getStoreName()).set(storageEngine);
      }
      getStoreIngestionService().startConsumption(storeConfig, partition);
    } else {
      nativeIngestionMonitorService.addVersionPartitionToIngestionMap(storeConfig.getStoreName(), partition);
      nativeIngestionRequestClient.startConsumption(storeConfig.getStoreName(), partition);
    }
  }

  @Override
  public void stopConsumption(VeniceStoreConfig storeConfig, int partition) {
    getStoreIngestionService().stopConsumption(storeConfig, partition);
    nativeIngestionRequestClient.stopConsumption(storeConfig.getStoreName(), partition);
  }

  @Override
  public void killConsumptionTask(String topicName) {
    getStoreIngestionService().killConsumptionTask(topicName);
    nativeIngestionRequestClient.killConsumptionTask(topicName);
  }

  @Override
  public void removeStorageEngine(String topicName) {
    getStorageService().removeStorageEngine(topicName);
    nativeIngestionMonitorService.removedSubscribedTopicName(topicName);
    nativeIngestionRequestClient.removeStorageEngine(topicName);
  }

  @Override
  public void dropStoragePartitionGracefully(VeniceStoreConfig storeConfig, int partition, int timeoutInSeconds) {
    getStoreIngestionService().stopConsumptionAndWait(storeConfig, partition, 1, timeoutInSeconds);
    getStorageService().dropStorePartition(storeConfig, partition);
    nativeIngestionRequestClient.unsubscribeTopicPartition(storeConfig.getStoreName(), partition);
  }

  @Override
  public void promoteToLeader(VeniceStoreConfig storeConfig, int partition,
      LeaderFollowerParticipantModel.LeaderSessionIdChecker leaderSessionIdChecker) {

    try {
      boolean messageCompleted = false;
      while (!messageCompleted) {
        /**
         * The reason to add this check is to avoid an edge case that - when a promote message is sent to isolated
         * ingestion process, the partition is completed but not reported to ingestion report listener. However,
         * unsubscribe() might have been kicking in, and delete the partitionConsumptionState before promote/demote
         * action is processed. This will result in NPE on PCS during action process stage.
         * In the isolated ingestion process, we add a data structure to capture if unsubscribe command is being added
         * to this specific topic partition, if so, we should reject the message and wait for a while. Ideally it will
         * end up reporting completion the ingestion in isolatedIngestionBackend, and then we will add this command
         * to local queue.
         */
        if (isTopicPartitionInLocal(storeConfig.getStoreName(), partition)) {
          getStoreIngestionService().promoteToLeader(storeConfig, partition, leaderSessionIdChecker);
          messageCompleted = true;
        } else {
          /**
           * LeaderSessionIdChecker logic for ingestion isolation is included in {@link IsolatedIngestionServer}.
           */
          messageCompleted = nativeIngestionRequestClient.promoteToLeader(storeConfig.getStoreName(), partition);
        }
        if (!messageCompleted) {
          Thread.sleep(100);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void demoteToStandby(VeniceStoreConfig storeConfig, int partition,
      LeaderFollowerParticipantModel.LeaderSessionIdChecker leaderSessionIdChecker) {
    try {
      boolean messageCompleted = false;
      while (!messageCompleted) {
        if (isTopicPartitionInLocal(storeConfig.getStoreName(), partition)) {
          getStoreIngestionService().demoteToStandby(storeConfig, partition, leaderSessionIdChecker);
          messageCompleted = true;
        } else {
          messageCompleted = nativeIngestionRequestClient.demoteToStandby(storeConfig.getStoreName(), partition);
        }
        if (!messageCompleted) {
          Thread.sleep(100);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void addIngestionNotifier(VeniceNotifier ingestionListener) {
    if (ingestionListener != null) {
      getStoreIngestionService().addCommonNotifier(ingestionListener);
      nativeIngestionMonitorService.addIngestionNotifier(getIsolatedIngestionNotifier(ingestionListener));
    }
  }

  @Override
  public void addOnlineOfflineIngestionNotifier(VeniceNotifier ingestionListener) {
    if (ingestionListener != null) {
      getStoreIngestionService().addOnlineOfflineModelNotifier(ingestionListener);
      nativeIngestionMonitorService.addIngestionNotifier(getIsolatedIngestionNotifier(ingestionListener));
    }
  }

  @Override
  public void addLeaderFollowerIngestionNotifier(VeniceNotifier ingestionListener) {
    if (ingestionListener != null) {
      getStoreIngestionService().addLeaderFollowerModelNotifier(ingestionListener);
      nativeIngestionMonitorService.addIngestionNotifier(getIsolatedIngestionNotifier(ingestionListener));
    }
  }

  @Override
  public void addPushStatusNotifier(VeniceNotifier pushStatusNotifier) {
    if (pushStatusNotifier != null) {
      getStoreIngestionService().addCommonNotifier(pushStatusNotifier);
      VeniceNotifier localPushStatusNotifier = new RelayNotifier(pushStatusNotifier) {
        @Override
        public void restarted(String kafkaTopic, int partition, long offset, String message) {
          /**
           * For push status notifier in main process, we should not report STARTED to push monitor, as END_OF_PUSH_RECEIVED
           * has been reported by child process, and it will violates push status state machine transition checks.
           */
        }
      };
      getStoreIngestionService().addCommonNotifier(localPushStatusNotifier);

      VeniceNotifier isolatedPushStatusNotifier = new RelayNotifier(pushStatusNotifier) {
        @Override
        public void completed(String kafkaTopic, int partition, long offset, String message) {
          // No-op. We should only report COMPLETED once when the partition is ready to serve in main process.
        }
      };
      nativeIngestionMonitorService.addPushStatusNotifier(isolatedPushStatusNotifier);
    }
  }

  @Override
  public void setStorageEngineReference(String topicName, AtomicReference<AbstractStorageEngine> storageEngineReference) {
    topicStorageEngineReferenceMap.put(topicName, storageEngineReference);
  }

  @Override
  public StorageMetadataService getStorageMetadataService() {
    return storageMetadataService;
  }

  @Override
  public KafkaStoreIngestionService getStoreIngestionService() {
    return kafkaStoreIngestionService;
  }

  @Override
  public StorageService getStorageService() {
    return storageService;
  }

  public void setIsolatedIngestionServiceProcess(Process process) {
    if (isolatedIngestionServiceProcess != null) {
      isolatedIngestionServiceProcess.destroy();
    }
    isolatedIngestionServiceProcess = process;
  }

  public void close() {
    try {
      nativeIngestionMonitorService.stopInner();
      nativeIngestionRequestClient.shutdownForkedProcessComponent(IngestionComponentType.KAFKA_INGESTION_SERVICE);
      nativeIngestionRequestClient.shutdownForkedProcessComponent(IngestionComponentType.STORAGE_SERVICE);
      isolatedIngestionServiceProcess.destroy();
      nativeIngestionRequestClient.close();
    } catch (Exception e) {
      logger.info("Unable to close IsolatedIngestionBackend", e);
    }
  }

  private boolean isTopicPartitionInLocal(String topicName, int partition) {
    return nativeIngestionMonitorService.isTopicPartitionIngestedInIsolatedProcess(topicName, partition);
  }

  private VeniceNotifier getIsolatedIngestionNotifier(VeniceNotifier notifier) {
    return new RelayNotifier(notifier) {
      @Override
      public void completed(String kafkaTopic, int partition, long offset, String message) {
        VeniceStoreConfig config = configLoader.getStoreConfig(kafkaTopic);
        config.setRestoreDataPartitions(false);
        config.setRestoreMetadataPartition(false);
        // Start partition consumption locally.
        startConsumption(config, partition);
      }
    };
  }
}
