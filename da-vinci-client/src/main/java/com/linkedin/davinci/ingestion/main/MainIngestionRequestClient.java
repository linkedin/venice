package com.linkedin.davinci.ingestion.main;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.HttpClientTransport;
import com.linkedin.davinci.ingestion.IsolatedIngestionProcessStats;
import com.linkedin.davinci.ingestion.isolated.IsolatedIngestionServer;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionMetricsReport;
import com.linkedin.venice.ingestion.protocol.IngestionStorageMetadata;
import com.linkedin.venice.ingestion.protocol.IngestionTaskCommand;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.ProcessShutdownCommand;
import com.linkedin.venice.ingestion.protocol.enums.IngestionAction;
import com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType;
import com.linkedin.venice.ingestion.protocol.enums.IngestionComponentType;
import com.linkedin.venice.meta.IngestionMetadataUpdateType;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.log4j.Logger;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType.*;


/**
 * MainIngestionRequestClient sends requests to isolated ingestion process and retrieves responses.
 */
public class MainIngestionRequestClient implements Closeable {
  private static final Logger logger = Logger.getLogger(MainIngestionRequestClient.class);
  private static final int REQUEST_MAX_ATTEMPT = 10;
  private static final int HEARTBEAT_REQUEST_TIMEOUT_MS = 10 * Time.MS_PER_SECOND;
  private final HttpClientTransport httpClientTransport;

  public MainIngestionRequestClient(int port) {
    httpClientTransport = new HttpClientTransport(port);
  }

  public synchronized Process startForkedIngestionProcess(VeniceConfigLoader configLoader) {
    int ingestionServicePort = configLoader.getVeniceServerConfig().getIngestionServicePort();
    int currentAttempt = 0;
    int totalAttempts = 3;
    Process forkedIngestionProcess = null;

    List<String> jvmArgs = new ArrayList<>();
    for (String jvmArg : configLoader.getCombinedProperties()
        .getString(ConfigKeys.SERVER_FORKED_PROCESS_JVM_ARGUMENT_LIST, "")
        .split(",")) {
      if (jvmArg.length() != 0) {
        jvmArgs.add(jvmArg);
      }
    }

    // Prepare initialization config
    String configFilePath = buildAndSaveConfigsForForkedIngestionProcess(configLoader);

    while (currentAttempt < totalAttempts) {
      try {
        // Add blocking call to release target port binding.
        IsolatedIngestionUtils.releaseTargetPortBinding(ingestionServicePort);

        /**
         * Do not register shutdown hook for forked ingestion process, as it will be taken care of by graceful shutdown of
         * Da Vinci client and server.
         * In the worst case that above graceful shutdown does not happen, forked ingestion process should also shut itself
         * down after specified timeout SERVER_INGESTION_ISOLATION_HEARTBEAT_TIMEOUT_MS (By default 1 min.)
         */
        forkedIngestionProcess = ForkedJavaProcess.exec(
            IsolatedIngestionServer.class,
            Arrays.asList(String.valueOf(ingestionServicePort), configFilePath),
            jvmArgs,
            Optional.empty(),
            false
        );
        // Wait for server in forked child process to bind the listening port.
        IsolatedIngestionUtils.waitPortBinding(ingestionServicePort, 100);
        // Wait for server in forked child process to pass health check.
        waitHealthCheck();
      } catch (Exception e) {
        currentAttempt++;
        if (currentAttempt == totalAttempts) {
          throw new VeniceException("Exception caught during initialization of ingestion service:", e);
        } else {
          logger.warn("Caught exception when initializing forked process in attempt " + currentAttempt + "/" + totalAttempts, e);
          // Kill failed process created in previous attempt.
          IsolatedIngestionUtils.destroyPreviousIsolatedIngestionProcess(forkedIngestionProcess);
          continue;
        }
      }
      logger.info("Isolated ingestion service initialization finished.");
      break;
    }
    return forkedIngestionProcess;
  }

  public void startConsumption(String topicName, int partitionId) {
    // Send ingestion request to ingestion service.
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = START_CONSUMPTION.getValue();
    ingestionTaskCommand.topicName = topicName;
    ingestionTaskCommand.partitionId = partitionId;
    logger.info("Sending request: " + START_CONSUMPTION + " to forked process for topic: " + topicName + ", partition: " + partitionId);
    try {
      httpClientTransport.sendRequestWithRetry(IngestionAction.COMMAND, ingestionTaskCommand, REQUEST_MAX_ATTEMPT);
    } catch (Exception e) {
      throw new VeniceException("Exception caught during startConsumption of topic: " + topicName + ", partition: " + partitionId, e);
    }
  }

  public void stopConsumption(String topicName, int partitionId) {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.STOP_CONSUMPTION.getValue();
    ingestionTaskCommand.topicName = topicName;
    ingestionTaskCommand.partitionId = partitionId;
    logger.info("Sending request: " + STOP_CONSUMPTION + " to forked process for topic: " + topicName + ", partition: " + partitionId);
    try {
      httpClientTransport.sendRequestWithRetry(IngestionAction.COMMAND, ingestionTaskCommand, REQUEST_MAX_ATTEMPT);
    } catch (Exception e) {
      throw new VeniceException("Exception caught during stopConsumption of topic: " + topicName + ", partition: " + partitionId, e);
    }
  }

  public void killConsumptionTask(String topicName) {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.KILL_CONSUMPTION.getValue();
    ingestionTaskCommand.topicName = topicName;
    logger.info("Sending request: " + KILL_CONSUMPTION + " to forked process for topic: " + topicName);
    try {
      // We do not need retry here. Retry will slow down DaVinciBackend's shutdown speed severely.
      httpClientTransport.sendRequest(IngestionAction.COMMAND, ingestionTaskCommand);
    } catch (Exception e) {
      throw new VeniceException("Exception caught during killConsumptionTask of topic: " + topicName, e);
    }
  }

  public void removeStorageEngine(String topicName) {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.REMOVE_STORAGE_ENGINE.getValue();
    ingestionTaskCommand.topicName = topicName;
    logger.info("Sending request: " + REMOVE_STORAGE_ENGINE + " to forked process for topic: " + topicName);
    try {
      httpClientTransport.sendRequestWithRetry(IngestionAction.COMMAND, ingestionTaskCommand, REQUEST_MAX_ATTEMPT);
    } catch (Exception e) {
      throw new VeniceException("Encounter exception during removeStorageEngine of topic: " + topicName, e);
    }
  }

  public void openStorageEngine(String topicName) {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.OPEN_STORAGE_ENGINE.getValue();
    ingestionTaskCommand.topicName = topicName;
    logger.info("Sending request: " + START_CONSUMPTION + " to forked process for topic: " + topicName);
    try {
      httpClientTransport.sendRequestWithRetry(IngestionAction.COMMAND, ingestionTaskCommand, REQUEST_MAX_ATTEMPT);
    } catch (Exception e) {
      throw new VeniceException("Encounter exception during openStorageEngine of topic: " + topicName, e);
    }
  }

  public void unsubscribeTopicPartition(String topicName, int partitionId) {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.REMOVE_PARTITION.getValue();
    ingestionTaskCommand.topicName = topicName;
    ingestionTaskCommand.partitionId = partitionId;
    logger.info("Sending request: " + REMOVE_PARTITION + " to forked process for topic: " + topicName + ", partition: " + partitionId);
    try {
      httpClientTransport.sendRequestWithRetry(IngestionAction.COMMAND, ingestionTaskCommand, REQUEST_MAX_ATTEMPT);
    } catch (Exception e) {
      throw new VeniceException("Encounter exception during unsubscribeTopicPartition of topic: " + topicName + ", partition: " + partitionId, e);
    }
  }

  public boolean updateMetadata(IngestionStorageMetadata ingestionStorageMetadata) {
    try {
      logger.info("Sending UPDATE_METADATA request to child process: " +
          IngestionMetadataUpdateType.valueOf(ingestionStorageMetadata.metadataUpdateType) + " for topic: " + ingestionStorageMetadata.topicName
          + " partition: " + ingestionStorageMetadata.partitionId);
      IngestionTaskReport report = httpClientTransport.sendRequest(IngestionAction.UPDATE_METADATA, ingestionStorageMetadata);
      return report.isPositive;
    } catch (Exception e) {
      /**
       * We only log the exception when failing to persist metadata updates into child process.
       * Child process might crashed, but it will be respawned and will be able to receive future updates.
       */
      logger.warn("Encounter exception when sending metadata updates to child process for topic: "
          + ingestionStorageMetadata.topicName + ", partition: " + ingestionStorageMetadata.partitionId);
      return false;
    }
  }

  public void shutdownForkedProcessComponent(IngestionComponentType ingestionComponentType) {
    // Send ingestion request to ingestion service.
    ProcessShutdownCommand processShutdownCommand = new ProcessShutdownCommand();
    processShutdownCommand.componentType = ingestionComponentType.getValue();
    logger.info("Sending shutdown component request to forked process for component: " + ingestionComponentType.name());
    try {
      httpClientTransport.sendRequest(IngestionAction.SHUTDOWN_COMPONENT, processShutdownCommand);
    } catch (Exception e) {
      logger.warn("Encounter exception when shutting down component: " + ingestionComponentType.name());
    }
  }

  public boolean promoteToLeader(String topicName, int partition) {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.PROMOTE_TO_LEADER.getValue();
    ingestionTaskCommand.topicName = topicName;
    ingestionTaskCommand.partitionId = partition;
    logger.info("Sending request: " + PROMOTE_TO_LEADER + " to forked process for topic: " + topicName);
    try {
      IngestionTaskReport report = httpClientTransport.sendRequestWithRetry(IngestionAction.COMMAND, ingestionTaskCommand, REQUEST_MAX_ATTEMPT);
      return report.isPositive;
    } catch (Exception e) {
      throw new VeniceException("Exception caught during promoteToLeader of topic: " + topicName, e);
    }
  }

  public boolean demoteToStandby(String topicName, int partition) {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.DEMOTE_TO_STANDBY.getValue();
    ingestionTaskCommand.topicName = topicName;
    ingestionTaskCommand.partitionId = partition;
    logger.info("Sending request: " + DEMOTE_TO_STANDBY + " to forked process for topic: " + topicName);
    try {
      IngestionTaskReport report = httpClientTransport.sendRequestWithRetry(IngestionAction.COMMAND, ingestionTaskCommand, REQUEST_MAX_ATTEMPT);
      return report.isPositive;
    } catch (Exception e) {
      throw new VeniceException("Exception caught during demoteToStandby of topic: " + topicName, e);
    }
  }

  public boolean collectMetrics(IsolatedIngestionProcessStats isolatedIngestionProcessStats) {
    try {
      IngestionMetricsReport metricsReport = httpClientTransport.sendRequest(IngestionAction.METRIC, getDummyCommand());
      if (logger.isDebugEnabled()) {
        logger.debug("Collected " + metricsReport.aggregatedMetrics.size() + " metrics from isolated ingestion service.");
      }
      isolatedIngestionProcessStats.updateMetricMap(metricsReport.aggregatedMetrics);
      return true;
    } catch (Exception e) {
      // Don't spam the server logging.
      logger.warn("Unable to collect metrics from ingestion service");
      return false;
    }
  }

  public boolean sendHeartbeatRequest() {
    try {
      httpClientTransport.sendRequest(IngestionAction.HEARTBEAT, getDummyCommand(), HEARTBEAT_REQUEST_TIMEOUT_MS);
      return true;
    } catch (Exception e) {
      // Don't spam the server logging.
      logger.warn("Unable to get heartbeat from ingestion service");
      return false;
    }
  }

  @Override
  public void close() {
    httpClientTransport.close();
  }

  private void waitHealthCheck() {
    long waitTime = 1000;
    int maxAttempt = 100;
    int retryCount = 0;
    long startTimeInMs = System.currentTimeMillis();
    while (true) {
      try {
        if (sendHeartbeatRequest()) {
          logger.info("Ingestion service server health check passed in " + (System.currentTimeMillis() - startTimeInMs) + " ms.");
          break;
        } else {
          throw new VeniceException("Got non-OK response from ingestion service.");
        }
      } catch (Exception e) {
        retryCount++;
        if (retryCount > maxAttempt) {
          logger.info("Fail to pass health-check for ingestion service after " + maxAttempt + " retries.");
          throw e;
        }
        Utils.sleep(waitTime);
      }
    }
  }
}
