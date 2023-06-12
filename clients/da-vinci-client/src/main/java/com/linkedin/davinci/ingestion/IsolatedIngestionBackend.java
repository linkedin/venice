package com.linkedin.davinci.ingestion;

import static com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType.DEMOTE_TO_STANDBY;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType.PROMOTE_TO_LEADER;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType.REMOVE_PARTITION;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType.START_CONSUMPTION;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType.STOP_CONSUMPTION;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.ingestion.main.MainIngestionMonitorService;
import com.linkedin.davinci.ingestion.main.MainIngestionRequestClient;
import com.linkedin.davinci.ingestion.main.MainIngestionStorageMetadataService;
import com.linkedin.davinci.ingestion.main.MainPartitionIngestionStatus;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.notifier.RelayNotifier;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType;
import com.linkedin.venice.ingestion.protocol.enums.IngestionComponentType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.AutoCloseableSingleLock;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private final ExecutorService completionReportHandlingExecutor = Executors.newFixedThreadPool(10);
  private Process isolatedIngestionServiceProcess;
  private AtomicBoolean isShuttingDown = new AtomicBoolean(false);

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
    // Create the ingestion request client.
    mainIngestionRequestClient = new MainIngestionRequestClient(configLoader);
    // Create the forked isolated ingestion process.
    isolatedIngestionServiceProcess = mainIngestionRequestClient.startForkedIngestionProcess(configLoader);
    // Create and start the ingestion report listener.
    try {
      mainIngestionMonitorService = new MainIngestionMonitorService(this, configLoader);
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
    executeCommandWithRetry(
        topicName,
        partition,
        START_CONSUMPTION,
        () -> mainIngestionRequestClient.startConsumption(storeConfig.getStoreVersionName(), partition),
        () -> super.startConsumption(storeConfig, partition, leaderState));
  }

  @Override
  public void stopConsumption(VeniceStoreVersionConfig storeConfig, int partition) {
    String topicName = storeConfig.getStoreVersionName();
    executeCommandWithRetry(
        topicName,
        partition,
        STOP_CONSUMPTION,
        () -> getMainIngestionRequestClient().stopConsumption(storeConfig.getStoreVersionName(), partition),
        () -> super.stopConsumption(storeConfig, partition));
  }

  @Override
  public void promoteToLeader(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker leaderSessionIdChecker) {
    String topicName = storeConfig.getStoreVersionName();
    executeCommandWithRetry(topicName, partition, PROMOTE_TO_LEADER, () -> {
      boolean result = mainIngestionRequestClient.promoteToLeader(topicName, partition);
      if (result) {
        getMainIngestionMonitorService().setTopicPartitionToLeaderState(topicName, partition);
      }
      return result;
    }, () -> super.promoteToLeader(storeConfig, partition, leaderSessionIdChecker));
  }

  @Override
  public void demoteToStandby(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker leaderSessionIdChecker) {
    String topicName = storeConfig.getStoreVersionName();
    executeCommandWithRetry(topicName, partition, DEMOTE_TO_STANDBY, () -> {
      boolean result = mainIngestionRequestClient.demoteToStandby(topicName, partition);
      if (result) {
        getMainIngestionMonitorService().setTopicIngestionToFollowerState(topicName, partition);
      }
      return result;
    }, () -> super.demoteToStandby(storeConfig, partition, leaderSessionIdChecker));
  }

  @Override
  public void dropStoragePartitionGracefully(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      int timeoutInSeconds,
      boolean removeEmptyStorageEngine) {
    String topicName = storeConfig.getStoreVersionName();
    executeCommandWithRetry(topicName, partition, REMOVE_PARTITION, () -> {
      boolean result = getMainIngestionRequestClient().removeTopicPartition(topicName, partition);
      // We will only clean up topic partition status if the remote execution is successful.
      if (result) {
        getMainIngestionMonitorService().cleanupTopicPartitionState(topicName, partition);
        getMainIngestionRequestClient().resetTopicPartition(topicName, partition);
      }
      return result;
    }, () -> {
      removeTopicPartitionLocally(storeConfig, partition, timeoutInSeconds, removeEmptyStorageEngine);
      // We will only clean up topic partition status if the local execution is successful.
      getMainIngestionMonitorService().cleanupTopicPartitionState(topicName, partition);
      getMainIngestionRequestClient().resetTopicPartition(topicName, partition);
    });
    if (getMainIngestionMonitorService().getTopicPartitionCount(topicName) == 0) {
      LOGGER.info("No serving partitions exist for topic: {}, dropping the topic storage.", topicName);
      getMainIngestionMonitorService().cleanupTopicState(topicName);
      getMainIngestionRequestClient().removeStorageEngine(topicName);
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
  public void shutdownIngestionTask(String topicName) {
    mainIngestionRequestClient.shutdownIngestionTask(topicName);
    super.shutdownIngestionTask(topicName);
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

  @Override
  public void prepareForShutdown() {
    isShuttingDown.set(true);
  }

  public void setIsolatedIngestionServiceProcess(Process process) {
    isolatedIngestionServiceProcess = process;
  }

  public Process getIsolatedIngestionServiceProcess() {
    return isolatedIngestionServiceProcess;
  }

  public MainIngestionMonitorService getMainIngestionMonitorService() {
    return mainIngestionMonitorService;
  }

  public MainIngestionRequestClient getMainIngestionRequestClient() {
    return mainIngestionRequestClient;
  }

  public boolean isShuttingDown() {
    return isShuttingDown.get();
  }

  void removeTopicPartitionLocally(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      int timeoutInSeconds,
      boolean removeEmptyStorageEngine) {
    super.dropStoragePartitionGracefully(storeConfig, partition, timeoutInSeconds, removeEmptyStorageEngine);
  }

  public void close() {
    try {
      completionReportHandlingExecutor.shutdownNow();
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

  boolean isTopicPartitionHostedInMainProcess(String topicName, int partition) {
    return getMainIngestionMonitorService().getTopicPartitionIngestionStatus(topicName, partition)
        .equals(MainPartitionIngestionStatus.MAIN);
  }

  boolean isTopicPartitionHosted(String topicName, int partition) {
    return !getMainIngestionMonitorService().getTopicPartitionIngestionStatus(topicName, partition)
        .equals(MainPartitionIngestionStatus.NOT_EXIST);
  }

  ExecutorService getCompletionHandlingExecutor() {
    return completionReportHandlingExecutor;
  }

  VeniceConfigLoader getConfigLoader() {
    return configLoader;
  }

  void startConsumptionLocally(
      VeniceStoreVersionConfig storeVersionConfig,
      int partition,
      Optional<LeaderFollowerStateType> leaderState) {
    super.startConsumption(storeVersionConfig, partition, leaderState);
  }

  VeniceNotifier getIsolatedIngestionNotifier(VeniceNotifier notifier) {
    return new RelayNotifier(notifier) {
      @Override
      public void completed(
          String kafkaTopic,
          int partition,
          long offset,
          String message,
          Optional<LeaderFollowerStateType> leaderState) {
        // Use thread pool to handle the completion reporting to make sure it is not blocking the report.
        if (isTopicPartitionHosted(kafkaTopic, partition)) {
          getCompletionHandlingExecutor().submit(() -> {
            /**
             * Start partition consumption locally.
             * If any error happens when starting the consumption, error will be reported.
             */
            try {
              VeniceStoreVersionConfig config = getConfigLoader().getStoreConfig(kafkaTopic);
              config.setRestoreDataPartitions(false);
              config.setRestoreMetadataPartition(false);
              startConsumptionLocally(config, partition, leaderState);
            } catch (Exception e) {
              notifier.error(
                  kafkaTopic,
                  partition,
                  "Failed to resume the ingestion in main process for topic: " + kafkaTopic,
                  e);
            } finally {
              getMainIngestionMonitorService().setVersionPartitionToLocalIngestion(kafkaTopic, partition);
            }
          });
        } else {
          LOGGER.error(
              "Partition: {} of topic: {} is not assigned to this host, will not resume the ingestion on main process.",
              partition,
              kafkaTopic);
        }
      }
    };
  }

  void executeCommandWithRetry(
      String topicName,
      int partition,
      IngestionCommandType command,
      Supplier<Boolean> remoteCommandSupplier,
      Runnable localCommandRunnable) {
    do {
      if (isTopicPartitionHostedInMainProcess(topicName, partition)
          || (!isTopicPartitionHosted(topicName, partition) && command != START_CONSUMPTION)) {
        LOGGER.info(
            "Executing command {} of topic: {}, partition: {} in main process process.",
            command,
            topicName,
            partition);
        localCommandRunnable.run();
        return;
      }
      LOGGER.info("Sending command {} of topic: {}, partition: {} to fork process.", command, topicName, partition);
      if (command.equals(START_CONSUMPTION)) {
        /**
         * StartConsumption operation may take long time to wait for non-existence store/version until it times out.
         * Add version check here so that the long waiting period won't impact forked ingestion process Netty server
         * performance.
         */
        Utils.waitStoreVersionOrThrow(topicName, getStoreIngestionService().getMetadataRepo());
        // Start consumption should set up resource ingestion status for tracking purpose.
        getMainIngestionMonitorService().setVersionPartitionToIsolatedIngestion(topicName, partition);
      }
      // Acquire read lock here to guard against race condition between Helix state transition and restart of forked
      // process.
      try (AutoCloseableLock ignored =
          AutoCloseableSingleLock.of(getMainIngestionMonitorService().getForkProcessActionLock().readLock())) {
        try {
          if (remoteCommandSupplier.get()) {
            return;
          }
        } catch (Exception e) {
          if (command.equals(START_CONSUMPTION)) {
            // Failure in start consumption request should reset the resource ingestion status.
            LOGGER.warn("Clean up ingestion status for topic: {}, partition: {}.", topicName, partition);
            getMainIngestionMonitorService().cleanupTopicPartitionState(topicName, partition);
          }
          throw e;
        }
      }

      /**
       * The idea of this check below is to add resiliency to isolated ingestion metadata management.
       * Although in most of the case the resource ingestion status managed by main / forked process should be in sync,
       * but in event of regression or unexpected error metadata could be out of sync.
       * This extra check covers the case where resource is maintained locally but main process think it is in forked
       * process and thus keeps failing. If the check indicates that resource is managed locally, it will execute the
       * command locally and break the loop, thus the request won't be stuck forever.
       */
      if (command.equals(STOP_CONSUMPTION) && getStoreIngestionService().isPartitionConsuming(topicName, partition)) {
        LOGGER.warn(
            "Expect topic: {}, partition: {} in forked process but found in main process, will execute command {} locally.",
            topicName,
            partition,
            command);
        localCommandRunnable.run();
        return;
      }
      /**
       * This is an extra safeguard during server shutdown to make sure we will not stuck in any state transition when
       * resource metadata is out of sync between main and child process. We will log and skip the stopConsumption action.
       * In fact, the resources will actually be stopped when {@link KafkaStoreIngestionService} is stopped in both
       * processed.
       */
      if (isShuttingDown()) {
        LOGGER.warn(
            "Command {} rejected by remote ingestion process, but will not retry since server is shutting down.",
            command);
        return;
      }

      LOGGER.info(
          "Command {} rejected by remote ingestion process, will retry in {} ms.",
          command,
          RETRY_WAIT_TIME_IN_MS);
    } while (Utils.sleep(RETRY_WAIT_TIME_IN_MS));
  }
}
