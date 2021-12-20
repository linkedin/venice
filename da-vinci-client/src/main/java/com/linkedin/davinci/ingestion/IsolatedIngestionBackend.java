package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.ingestion.isolated.IsolatedIngestionServer;
import com.linkedin.davinci.ingestion.main.MainIngestionMonitorService;
import com.linkedin.davinci.ingestion.main.MainIngestionRequestClient;
import com.linkedin.davinci.ingestion.main.MainIngestionStorageMetadataService;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.notifier.RelayNotifier;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.enums.IngestionComponentType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * IsolatedIngestionBackend is the implementation of ingestion backend designed for ingestion isolation.
 * It contains references to local ingestion components - including storage metadata service, storage service and store
 * ingestion service that serves local ingestion, as well as ingestion request client that sends commands to isolated
 * ingestion service process and ingestion listener that listens to ingestion reports from child process.
 * Since RocksDB storage can only be owned by a single process, we have decided to keep metadata partition storage opened
 * in child process and in the main process, we rely on {@link MainIngestionStorageMetadataService} to serve as the in-memory
 * metadata cache and persist the metadata updates from main process to metadata partition in child process.
 * Topic partition ingestion requests will first be sent to child process and after COMPLETED is reported, they will be
 * re-subscribed in main process to serve read traffics for user application and receive future updates.
 * The implementation of APIs in this class should consider the states in both main process and child process, as we need
 * to make sure we send the command to the correct process which holds the target storage engine.
 */
public class IsolatedIngestionBackend extends DefaultIngestionBackend implements DaVinciIngestionBackend, VeniceIngestionBackend {
  private static final Logger logger = LogManager.getLogger(IsolatedIngestionBackend.class);
  private static final int RETRY_WAIT_TIME_IN_MS = 10 * Time.MS_PER_SECOND;

  private final MainIngestionRequestClient mainIngestionRequestClient;
  private final MainIngestionMonitorService mainIngestionMonitorService;
  private final VeniceConfigLoader configLoader;
  private final Optional<SSLEngineComponentFactory> sslFactory;

  private Process isolatedIngestionServiceProcess;

  public IsolatedIngestionBackend(VeniceConfigLoader configLoader, ReadOnlyStoreRepository storeRepository, MetricsRepository metricsRepository,
      StorageMetadataService storageMetadataService, KafkaStoreIngestionService storeIngestionService, StorageService storageService) {
    super(storageMetadataService, storeIngestionService, storageService);
    int servicePort = configLoader.getVeniceServerConfig().getIngestionServicePort();
    int listenerPort = configLoader.getVeniceServerConfig().getIngestionApplicationPort();
    this.configLoader = configLoader;
    this.sslFactory = IsolatedIngestionUtils.getSSLEngineComponentFactory(configLoader);

    // Create the ingestion request client.
    mainIngestionRequestClient = new MainIngestionRequestClient(sslFactory, servicePort);
    // Create the forked isolated ingestion process.
    isolatedIngestionServiceProcess = mainIngestionRequestClient.startForkedIngestionProcess(configLoader);
    // Create and start the ingestion report listener.
    try {
      mainIngestionMonitorService = new MainIngestionMonitorService(this, configLoader, sslFactory);
      mainIngestionMonitorService.setStoreRepository(storeRepository);
      mainIngestionMonitorService.setMetricsRepository(metricsRepository);
      mainIngestionMonitorService.setStoreIngestionService(storeIngestionService);
      mainIngestionMonitorService.setStorageMetadataService((MainIngestionStorageMetadataService) storageMetadataService);

      mainIngestionMonitorService.startInner();
      logger.info("Ingestion Report Listener started.");
    } catch (Exception e) {
      throw new VeniceException("Unable to start ingestion report listener.", e);
    }
    logger.info("Created isolated ingestion backend with service port: " + servicePort + ", listener port: " + listenerPort);
  }

  @Override
  public void startConsumption(VeniceStoreVersionConfig storeConfig, int partition, Optional<LeaderFollowerStateType> leaderState) {
    if (isTopicPartitionInLocal(storeConfig.getStoreVersionName(), partition)) {
      logger.info("Start consumption of topic: " + storeConfig.getStoreVersionName() + ", partition: " + partition + " in main process.");
      super.startConsumption(storeConfig, partition, leaderState);
    } else {
      logger.info("Sending consumption request of topic: " + storeConfig.getStoreVersionName() + ", partition: " + partition + " to fork process.");
      mainIngestionMonitorService.addVersionPartitionToIngestionMap(storeConfig.getStoreVersionName(), partition);
      mainIngestionRequestClient.startConsumption(storeConfig.getStoreVersionName(), partition);
    }
  }

  @Override
  public void stopConsumption(VeniceStoreVersionConfig storeConfig, int partition) {
    mainIngestionRequestClient.stopConsumption(storeConfig.getStoreVersionName(), partition);
    super.stopConsumption(storeConfig, partition);
  }

  @Override
  public void killConsumptionTask(String topicName) {
    mainIngestionRequestClient.killConsumptionTask(topicName);
    super.killConsumptionTask(topicName);
  }

  @Override
  public void removeStorageEngine(String topicName) {
    mainIngestionRequestClient.removeStorageEngine(topicName);
    super.removeStorageEngine(topicName);
    mainIngestionMonitorService.cleanupTopicState(topicName);
  }

  @Override
  public void dropStoragePartitionGracefully(VeniceStoreVersionConfig storeConfig, int partition, int timeoutInSeconds) {
    mainIngestionRequestClient.unsubscribeTopicPartition(storeConfig.getStoreVersionName(), partition);
    super.dropStoragePartitionGracefully(storeConfig, partition, timeoutInSeconds);
    mainIngestionMonitorService.cleanupTopicPartitionState(storeConfig.getStoreVersionName(), partition);
  }

  @Override
  public void promoteToLeader(VeniceStoreVersionConfig storeConfig, int partition,
    LeaderFollowerPartitionStateModel.LeaderSessionIdChecker leaderSessionIdChecker) {
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
      if (isTopicPartitionInLocal(storeConfig.getStoreVersionName(), partition)) {
        super.promoteToLeader(storeConfig, partition, leaderSessionIdChecker);
        messageCompleted = true;
      } else {
        /**
         * LeaderSessionIdChecker logic for ingestion isolation is included in {@link IsolatedIngestionServer}.
         */
        messageCompleted = mainIngestionRequestClient.promoteToLeader(storeConfig.getStoreVersionName(), partition);
      }
      if (!messageCompleted) {
        logger.info("Leader promotion message rejected by remote ingestion server, will retry in " + RETRY_WAIT_TIME_IN_MS + " ms.");
        try {
          Thread.sleep(RETRY_WAIT_TIME_IN_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.info("Retry in leader promotion is interrupted.");
          break;
        }
      }
    }
  }

  @Override
  public void demoteToStandby(VeniceStoreVersionConfig storeConfig, int partition,
    LeaderFollowerPartitionStateModel.LeaderSessionIdChecker leaderSessionIdChecker) {
    boolean messageCompleted = false;
    while (!messageCompleted) {
      if (isTopicPartitionInLocal(storeConfig.getStoreVersionName(), partition)) {
        super.demoteToStandby(storeConfig, partition, leaderSessionIdChecker);
        messageCompleted = true;
      } else {
        messageCompleted = mainIngestionRequestClient.demoteToStandby(storeConfig.getStoreVersionName(), partition);
      }
      if (!messageCompleted) {
        logger.info("Leader demotion message rejected by remote ingestion server, will retry in " + RETRY_WAIT_TIME_IN_MS + " ms.");
        try {
          Thread.sleep(RETRY_WAIT_TIME_IN_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.info("Retry in leader demotion is interrupted.");
          break;
        }
      }
    }
  }

  @Override
  public void addIngestionNotifier(VeniceNotifier ingestionListener) {
    if (ingestionListener != null) {
      super.addIngestionNotifier(ingestionListener);
      mainIngestionMonitorService.addOnlineOfflineIngestionNotifier(getIsolatedIngestionNotifier(ingestionListener));
      mainIngestionMonitorService.addLeaderFollowerIngestionNotifier(getIsolatedIngestionNotifier(ingestionListener));
    }
  }

  @Override
  public void addOnlineOfflineIngestionNotifier(VeniceNotifier ingestionListener) {
    if (ingestionListener != null) {
      super.addOnlineOfflineIngestionNotifier(ingestionListener);
      mainIngestionMonitorService.addOnlineOfflineIngestionNotifier(getIsolatedIngestionNotifier(ingestionListener));
    }
  }

  @Override
  public void addLeaderFollowerIngestionNotifier(VeniceNotifier ingestionListener) {
    if (ingestionListener != null) {
      super.addLeaderFollowerIngestionNotifier(ingestionListener);
      mainIngestionMonitorService.addLeaderFollowerIngestionNotifier(getIsolatedIngestionNotifier(ingestionListener));
    }
  }

  @Override
  public void addPushStatusNotifier(VeniceNotifier pushStatusNotifier) {
    if (pushStatusNotifier != null) {
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
      mainIngestionMonitorService.addPushStatusNotifier(isolatedPushStatusNotifier);
    }
  }

  public void setIsolatedIngestionServiceProcess(Process process) {
    isolatedIngestionServiceProcess = process;
  }

  public Process getIsolatedIngestionServiceProcess() {
    return isolatedIngestionServiceProcess;
  }

  public void close() {
    try {
      mainIngestionMonitorService.stopInner();
      mainIngestionRequestClient.shutdownForkedProcessComponent(IngestionComponentType.KAFKA_INGESTION_SERVICE);
      mainIngestionRequestClient.shutdownForkedProcessComponent(IngestionComponentType.STORAGE_SERVICE);
      isolatedIngestionServiceProcess.destroy();
      mainIngestionRequestClient.close();
      super.close();
    } catch (Exception e) {
      logger.info("Unable to close " + getClass().getSimpleName(), e);
    }
  }

  private boolean isTopicPartitionInLocal(String topicName, int partition) {
    return mainIngestionMonitorService.isTopicPartitionIngestedInIsolatedProcess(topicName, partition);
  }

  private VeniceNotifier getIsolatedIngestionNotifier(VeniceNotifier notifier) {
    return new RelayNotifier(notifier) {
      @Override
      public void completed(String kafkaTopic, int partition, long offset, String message, Optional<LeaderFollowerStateType> leaderState) {
        VeniceStoreVersionConfig config = configLoader.getStoreConfig(kafkaTopic);
        config.setRestoreDataPartitions(false);
        config.setRestoreMetadataPartition(false);
        // Start partition consumption locally.
        startConsumption(config, partition, leaderState);
      }
    };
  }
}
