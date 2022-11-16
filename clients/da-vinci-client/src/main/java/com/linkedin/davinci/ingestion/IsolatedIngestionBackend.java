package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.ingestion.main.MainIngestionMonitorService;
import com.linkedin.davinci.ingestion.main.MainIngestionRequestClient;
import com.linkedin.davinci.ingestion.main.MainIngestionStorageMetadataService;
import com.linkedin.davinci.ingestion.main.MainPartitionIngestionStatus;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.notifier.RelayNotifier;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.enums.IngestionComponentType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is the implementation of ingestion backend designed for ingestion isolation.
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
public class IsolatedIngestionBackend extends DefaultIngestionBackend
    implements DaVinciIngestionBackend, VeniceIngestionBackend {
  private static final Logger LOGGER = LogManager.getLogger(IsolatedIngestionBackend.class);
  private static final int RETRY_WAIT_TIME_IN_MS = Time.MS_PER_SECOND;
  private final MainIngestionRequestClient mainIngestionRequestClient;
  private final MainIngestionMonitorService mainIngestionMonitorService;
  private final VeniceConfigLoader configLoader;
  private Process isolatedIngestionServiceProcess;

  public IsolatedIngestionBackend(
      VeniceConfigLoader configLoader,
      ReadOnlyStoreRepository storeRepository,
      MetricsRepository metricsRepository,
      StorageMetadataService storageMetadataService,
      KafkaStoreIngestionService storeIngestionService,
      StorageService storageService) {
    super(storageMetadataService, storeIngestionService, storageService);
    int servicePort = configLoader.getVeniceServerConfig().getIngestionServicePort();
    int listenerPort = configLoader.getVeniceServerConfig().getIngestionApplicationPort();
    this.configLoader = configLoader;
    Optional<SSLFactory> sslFactory = IsolatedIngestionUtils.getSSLFactory(configLoader);
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
      mainIngestionMonitorService
          .setStorageMetadataService((MainIngestionStorageMetadataService) storageMetadataService);

      mainIngestionMonitorService.startInner();
      LOGGER.info("Ingestion Report Listener started.");
    } catch (Exception e) {
      throw new VeniceException("Unable to start ingestion report listener.", e);
    }
    LOGGER
        .info("Created isolated ingestion backend with service port: {}, listener port: {}", servicePort, listenerPort);
  }

  @Override
  public void startConsumption(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      Optional<LeaderFollowerStateType> leaderState) {
    String topicName = storeConfig.getStoreVersionName();
    executeCommandWithRetry(topicName, partition, () -> {
      mainIngestionMonitorService.setVersionPartitionToIsolatedIngestion(storeConfig.getStoreVersionName(), partition);
      return mainIngestionRequestClient.startConsumption(storeConfig.getStoreVersionName(), partition);
    }, () -> {
      // TODO: I believe this is not needed. Check back.
      mainIngestionMonitorService.setVersionPartitionToLocalIngestion(storeConfig.getStoreVersionName(), partition);
      super.startConsumption(storeConfig, partition, leaderState);
    });
  }

  @Override
  public void stopConsumption(VeniceStoreVersionConfig storeConfig, int partition) {
    String topicName = storeConfig.getStoreVersionName();
    executeCommandWithRetry(
        topicName,
        partition,
        () -> mainIngestionRequestClient.stopConsumption(storeConfig.getStoreVersionName(), partition),
        () -> super.stopConsumption(storeConfig, partition));
  }

  @Override
  public void promoteToLeader(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker leaderSessionIdChecker) {
    String topicName = storeConfig.getStoreVersionName();
    executeCommandWithRetry(
        topicName,
        partition,
        () -> mainIngestionRequestClient.promoteToLeader(topicName, partition),
        () -> super.promoteToLeader(storeConfig, partition, leaderSessionIdChecker));
  }

  @Override
  public void demoteToStandby(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker leaderSessionIdChecker) {
    String topicName = storeConfig.getStoreVersionName();
    executeCommandWithRetry(
        topicName,
        partition,
        () -> mainIngestionRequestClient.demoteToStandby(topicName, partition),
        () -> super.demoteToStandby(storeConfig, partition, leaderSessionIdChecker));
  }

  @Override
  public void dropStoragePartitionGracefully(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      int timeoutInSeconds,
      boolean removeEmptyStorageEngine) {
    String topicName = storeConfig.getStoreVersionName();
    mainIngestionMonitorService.cleanupTopicPartitionState(topicName, partition);
    executeCommandWithRetry(
        topicName,
        partition,
        () -> mainIngestionRequestClient.unsubscribeTopicPartition(topicName, partition),
        () -> {
          super.dropStoragePartitionGracefully(storeConfig, partition, timeoutInSeconds, removeEmptyStorageEngine);
          // Clean up the topic partition ingestion status.
          mainIngestionRequestClient.resetTopicPartition(topicName, partition);
        });
    if (mainIngestionMonitorService.getTopicPartitionCount(topicName) == 0) {
      LOGGER.info("No serving partitions exist for topic: {}, dropping the topic storage.", topicName);
      mainIngestionMonitorService.cleanupTopicState(topicName);
      mainIngestionRequestClient.removeStorageEngine(topicName);
    }
  }

  @Override
  public void removeStorageEngine(String topicName) {
    mainIngestionRequestClient.removeStorageEngine(topicName);
    super.removeStorageEngine(topicName);
    mainIngestionMonitorService.cleanupTopicState(topicName);
  }

  @Override
  public void killConsumptionTask(String topicName) {
    mainIngestionRequestClient.killConsumptionTask(topicName);
    super.killConsumptionTask(topicName);
    mainIngestionMonitorService.cleanupTopicState(topicName);
  }

  @Override
  public void addIngestionNotifier(VeniceNotifier ingestionListener) {
    if (ingestionListener != null) {
      super.addIngestionNotifier(ingestionListener);
      mainIngestionMonitorService.addIngestionNotifier(getIsolatedIngestionNotifier(ingestionListener));
    }
  }

  @Override
  public void addLeaderFollowerIngestionNotifier(VeniceNotifier ingestionListener) {
    if (ingestionListener != null) {
      super.addLeaderFollowerIngestionNotifier(ingestionListener);
      mainIngestionMonitorService.addIngestionNotifier(getIsolatedIngestionNotifier(ingestionListener));
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
      getStoreIngestionService().addIngestionNotifier(localPushStatusNotifier);

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
      LOGGER.info("Unable to close {}", getClass().getSimpleName(), e);
    }
  }

  private boolean isTopicPartitionHostedInMainProcess(String topicName, int partition) {
    return mainIngestionMonitorService.getTopicPartitionIngestionStatus(topicName, partition)
        .equals(MainPartitionIngestionStatus.MAIN);
  }

  private boolean isTopicPartitionIngesting(String topicName, int partition) {
    return !mainIngestionMonitorService.getTopicPartitionIngestionStatus(topicName, partition)
        .equals(MainPartitionIngestionStatus.NOT_EXIST);
  }

  private VeniceNotifier getIsolatedIngestionNotifier(VeniceNotifier notifier) {
    return new RelayNotifier(notifier) {
      @Override
      public void completed(
          String kafkaTopic,
          int partition,
          long offset,
          String message,
          Optional<LeaderFollowerStateType> leaderState) {
        if (isTopicPartitionIngesting(kafkaTopic, partition)) {
          VeniceStoreVersionConfig config = configLoader.getStoreConfig(kafkaTopic);
          config.setRestoreDataPartitions(false);
          config.setRestoreMetadataPartition(false);
          // Start partition consumption locally.
          startConsumption(config, partition, leaderState);
        } else {
          LOGGER.error(
              "Partition: {} of topic: {} is not assigned to this host, will not resume the ingestion on main process.",
              partition,
              kafkaTopic);
        }
      }
    };
  }

  private void executeCommandWithRetry(
      String topicName,
      int partition,
      Supplier<Boolean> remoteCommandSupplier,
      Runnable localCommandRunnable) {
    boolean messageCompleted = false;
    while (!messageCompleted) {
      if (isTopicPartitionHostedInMainProcess(topicName, partition)) {
        LOGGER.info("Executing command of topic: {}, partition: {} in main process process.", topicName, partition);
        localCommandRunnable.run();
        messageCompleted = true;
      } else {
        LOGGER.info("Sending command of topic: {}, partition: {} to fork process.", topicName, partition);
        messageCompleted = remoteCommandSupplier.get();
      }
      if (!messageCompleted) {
        LOGGER.info("Command not executed by remote ingestion process, will retry in {} ms.", RETRY_WAIT_TIME_IN_MS);
        try {
          Thread.sleep(RETRY_WAIT_TIME_IN_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.info("Retry of the command execution is interrupted.");
          break;
        }
      }
    }
  }
}
