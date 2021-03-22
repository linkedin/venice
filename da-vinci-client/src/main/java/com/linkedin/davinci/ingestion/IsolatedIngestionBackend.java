package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.helix.LeaderFollowerParticipantModel;
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
 * IsolatedIngestionBackend is the implementation of ingestion backend designed for ingestion isolation. Ingestion will be done
 * in the isolated ingestion process with different JVM before COMPLETED state. After COMPLETED state, ingestion will continue
 * in application JVM.
 */
public class IsolatedIngestionBackend implements IngestionBackend {
  private static final Logger logger = Logger.getLogger(IsolatedIngestionBackend.class);
  private final IngestionStorageMetadataService storageMetadataService;
  private final StorageService storageService;
  private final KafkaStoreIngestionService kafkaStoreIngestionService;
  private final IngestionRequestClient ingestionRequestClient;
  private final IngestionReportListener ingestionReportListener;
  private final VeniceConfigLoader configLoader;
  private final Map<String, AtomicReference<AbstractStorageEngine>> topicStorageEngineReferenceMap = new VeniceConcurrentHashMap<>();

  private Process isolatedIngestionServiceProcess;

  public IsolatedIngestionBackend(VeniceConfigLoader configLoader, MetricsRepository metricsRepository,
      StorageMetadataService storageMetadataService, KafkaStoreIngestionService storeIngestionService, StorageService storageService) {
    int servicePort = configLoader.getVeniceServerConfig().getIngestionServicePort();
    int listenerPort = configLoader.getVeniceServerConfig().getIngestionApplicationPort();
    this.kafkaStoreIngestionService = storeIngestionService;
    this.storageMetadataService = (IngestionStorageMetadataService) storageMetadataService;
    this.storageService = storageService;
    this.configLoader = configLoader;

    // Create the ingestion request client.
    ingestionRequestClient = new IngestionRequestClient(servicePort);
    // Create the forked Ingestion process.
    isolatedIngestionServiceProcess = ingestionRequestClient.startForkedIngestionProcess(configLoader);
    // Create and start the ingestion report listener.
    try {
      ingestionReportListener = new IngestionReportListener(this, listenerPort, servicePort);
      ingestionReportListener.setMetricsRepository(metricsRepository);
      ingestionReportListener.setStoreIngestionService(storeIngestionService);
      ingestionReportListener.setStorageMetadataService((IngestionStorageMetadataService) storageMetadataService);
      ingestionReportListener.setConfigLoader(configLoader);
      ingestionReportListener.startInner();
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
      ingestionReportListener.addVersionPartitionToIngestionMap(storeConfig.getStoreName(), partition);
      ingestionRequestClient.startConsumption(storeConfig.getStoreName(), partition);
    }
  }

  @Override
  public void stopConsumption(VeniceStoreConfig storeConfig, int partition) {
    getStoreIngestionService().stopConsumption(storeConfig, partition);
    ingestionRequestClient.stopConsumption(storeConfig.getStoreName(), partition);
  }

  @Override
  public void killConsumptionTask(String topicName) {
    getStoreIngestionService().killConsumptionTask(topicName);
    ingestionRequestClient.killConsumptionTask(topicName);
  }

  @Override
  public void removeStorageEngine(String topicName) {
    getStorageService().removeStorageEngine(topicName);
    ingestionReportListener.removedSubscribedTopicName(topicName);
    ingestionRequestClient.removeStorageEngine(topicName);
  }

  @Override
  public void unsubscribeTopicPartition(VeniceStoreConfig storeConfig, int partition, int timeoutInSeconds) {
    getStoreIngestionService().stopConsumptionAndWait(storeConfig, partition, 1, timeoutInSeconds);
    getStorageService().dropStorePartition(storeConfig, partition);
    ingestionRequestClient.unsubscribeTopicPartition(storeConfig.getStoreName(), partition);
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
           * LeaderSessionIdChecker logic for ingestion isolation is included in {@link IngestionService}.
           */
          messageCompleted = ingestionRequestClient.promoteToLeader(storeConfig.getStoreName(), partition);
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
          messageCompleted = ingestionRequestClient.demoteToStandby(storeConfig.getStoreName(), partition);
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
      ingestionReportListener.addIngestionNotifier(getIsolatedIngestionNotifier(ingestionListener));
    }
  }

  @Override
  public void addOnlineOfflineIngestionNotifier(VeniceNotifier ingestionListener) {
    if (ingestionListener != null) {
      getStoreIngestionService().addOnlineOfflineModelNotifier(ingestionListener);
      ingestionReportListener.addIngestionNotifier(getIsolatedIngestionNotifier(ingestionListener));
    }
  }

  @Override
  public void addLeaderFollowerIngestionNotifier(VeniceNotifier ingestionListener) {
    if (ingestionListener != null) {
      getStoreIngestionService().addLeaderFollowerModelNotifier(ingestionListener);
      ingestionReportListener.addIngestionNotifier(getIsolatedIngestionNotifier(ingestionListener));
    }
  }

  @Override
  public void addPushStatusNotifier(VeniceNotifier pushStatusNotifier) {
    if (pushStatusNotifier != null) {
      getStoreIngestionService().addCommonNotifier(pushStatusNotifier);
      VeniceNotifier isolatedPushStatusNotifier = new RelayNotifier(pushStatusNotifier) {
        @Override
        public void completed(String kafkaTopic, int partition, long offset, String message) {
          // No-op. We should not report completed status to monitor before we subscribe topic partition in main process.
        }
      };
      ingestionReportListener.addPushStatusNotifier(isolatedPushStatusNotifier);
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
      ingestionReportListener.stopInner();
      ingestionRequestClient.shutdownForkedProcessComponent(IngestionComponentType.KAFKA_INGESTION_SERVICE);
      ingestionRequestClient.shutdownForkedProcessComponent(IngestionComponentType.STORAGE_SERVICE);
      isolatedIngestionServiceProcess.destroy();
      ingestionRequestClient.close();
    } catch (Exception e) {
      logger.info("Unable to close IsolatedIngestionBackend", e);
    }
  }

  private boolean isTopicPartitionInLocal(String topicName, int partition) {
    return ingestionReportListener.isTopicPartitionIngestedInIsolatedProcess(topicName, partition);
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
