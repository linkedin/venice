package com.linkedin.davinci.ingestion.main;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.buildAndSaveConfigsForForkedIngestionProcess;
import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.getDummyCommand;
import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.saveForkedIngestionKafkaClusterMapConfig;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_HEARTBEAT_REQUEST_TIMEOUT_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_REQUEST_TIMEOUT_SECONDS;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType.START_CONSUMPTION;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.HttpClientTransport;
import com.linkedin.davinci.ingestion.isolated.IsolatedIngestionServer;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionStorageMetadata;
import com.linkedin.venice.ingestion.protocol.IngestionTaskCommand;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.ProcessShutdownCommand;
import com.linkedin.venice.ingestion.protocol.enums.IngestionAction;
import com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType;
import com.linkedin.venice.ingestion.protocol.enums.IngestionComponentType;
import com.linkedin.venice.meta.IngestionMetadataUpdateType;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class sends requests to isolated ingestion process and retrieves responses.
 */
public class MainIngestionRequestClient implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(MainIngestionRequestClient.class);

  private static final int REQUEST_MAX_ATTEMPT = 10;
  private HttpClientTransport httpClientTransport;
  private final int heartbeatRequestTimeoutSeconds;

  public MainIngestionRequestClient(VeniceConfigLoader configLoader) {
    heartbeatRequestTimeoutSeconds =
        configLoader.getCombinedProperties().getInt(SERVER_INGESTION_ISOLATION_HEARTBEAT_REQUEST_TIMEOUT_SECONDS, 5);
    Optional<SSLFactory> sslFactory = IsolatedIngestionUtils.getSSLFactory(configLoader);
    int port = configLoader.getVeniceServerConfig().getIngestionServicePort();
    int requestTimeoutInSeconds =
        configLoader.getCombinedProperties().getInt(SERVER_INGESTION_ISOLATION_REQUEST_TIMEOUT_SECONDS, 120);
    httpClientTransport = new HttpClientTransport(sslFactory, port, requestTimeoutInSeconds);
  }

  public synchronized Process startForkedIngestionProcess(VeniceConfigLoader configLoader) {
    int ingestionServicePort = configLoader.getVeniceServerConfig().getIngestionServicePort();
    int currentAttempt = 0;
    int totalAttempts = 3;
    ForkedJavaProcess forkedIngestionProcess = null;

    List<String> jvmArgs = configLoader.getVeniceServerConfig().getForkedProcessJvmArgList();

    // Prepare initialization config
    String configFilePath = buildAndSaveConfigsForForkedIngestionProcess(configLoader);
    saveForkedIngestionKafkaClusterMapConfig(configLoader);

    while (currentAttempt < totalAttempts) {
      try {
        // Destroy lingering isolated forked process.
        IsolatedIngestionUtils.destroyLingeringIsolatedIngestionProcess(configLoader);

        /**
         * Do not register shutdown hook for forked ingestion process, as it will be taken care of by graceful shutdown of
         * Da Vinci client and server.
         * In the worst case that above graceful shutdown does not happen, forked ingestion process should also shut itself
         * down after specified timeout SERVER_INGESTION_ISOLATION_HEARTBEAT_TIMEOUT_MS (By default 1 min.)
         */
        forkedIngestionProcess = ForkedJavaProcess
            .exec(IsolatedIngestionServer.class, Collections.singletonList(configFilePath), jvmArgs, false);
        LOGGER.info("Forked new isolated ingestion process at PID: " + forkedIngestionProcess.pid());
        IsolatedIngestionUtils.saveForkedIngestionProcessMetadata(configLoader, forkedIngestionProcess);
        // Wait for server in forked child process to bind the listening port.
        IsolatedIngestionUtils.waitPortBinding(ingestionServicePort, 100);
        // Wait for server in forked child process to pass health check.
        waitHealthCheck();
      } catch (Exception e) {
        currentAttempt++;
        if (currentAttempt == totalAttempts) {
          throw new VeniceException("Exception caught during initialization of ingestion service:", e);
        } else {
          LOGGER.warn(
              "Caught exception when initializing forked process in attempt " + currentAttempt + "/" + totalAttempts,
              e);
          continue;
        }
      }
      LOGGER.info("Isolated ingestion service initialization finished.");
      break;
    }
    return forkedIngestionProcess;
  }

  public boolean startConsumption(String topicName, int partitionId) {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = START_CONSUMPTION.getValue();
    ingestionTaskCommand.topicName = topicName;
    ingestionTaskCommand.partitionId = partitionId;
    return sendIngestionCommandWithRetry(
        ingestionTaskCommand,
        topicName,
        Optional.of(partitionId),
        REQUEST_MAX_ATTEMPT);
  }

  public boolean stopConsumption(String topicName, int partitionId) {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.STOP_CONSUMPTION.getValue();
    ingestionTaskCommand.topicName = topicName;
    ingestionTaskCommand.partitionId = partitionId;
    return sendIngestionCommandWithRetry(
        ingestionTaskCommand,
        topicName,
        Optional.of(partitionId),
        REQUEST_MAX_ATTEMPT);
  }

  public void killConsumptionTask(String topicName) {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.KILL_CONSUMPTION.getValue();
    ingestionTaskCommand.topicName = topicName;

    // We do not need to retry here. Retry will slow down DaVinciBackend's shutdown speed severely.
    sendIngestionCommandWithRetry(ingestionTaskCommand, topicName, Optional.empty(), 1);
  }

  public void shutdownIngestionTask(String topicName) {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.SHUTDOWN_INGESTION_TASK.getValue();
    ingestionTaskCommand.topicName = topicName;

    // We do not need to retry here. Retry will slow down DaVinciBackend's shutdown speed severely.
    sendIngestionCommandWithRetry(ingestionTaskCommand, topicName, Optional.empty(), 1);
  }

  public void removeStorageEngine(String topicName) {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.REMOVE_STORAGE_ENGINE.getValue();
    ingestionTaskCommand.topicName = topicName;
    sendIngestionCommandWithRetry(ingestionTaskCommand, topicName, Optional.empty(), REQUEST_MAX_ATTEMPT);
  }

  public void openStorageEngine(String topicName) {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.OPEN_STORAGE_ENGINE.getValue();
    ingestionTaskCommand.topicName = topicName;
    sendIngestionCommandWithRetry(ingestionTaskCommand, topicName, Optional.empty(), REQUEST_MAX_ATTEMPT);
  }

  public boolean removeTopicPartition(String topicName, int partitionId) {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.REMOVE_PARTITION.getValue();
    ingestionTaskCommand.topicName = topicName;
    ingestionTaskCommand.partitionId = partitionId;
    return sendIngestionCommandWithRetry(ingestionTaskCommand, topicName, Optional.empty(), REQUEST_MAX_ATTEMPT);
  }

  public boolean promoteToLeader(String topicName, int partitionId) {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.PROMOTE_TO_LEADER.getValue();
    ingestionTaskCommand.topicName = topicName;
    ingestionTaskCommand.partitionId = partitionId;
    return sendIngestionCommandWithRetry(
        ingestionTaskCommand,
        topicName,
        Optional.of(partitionId),
        REQUEST_MAX_ATTEMPT);
  }

  public boolean demoteToStandby(String topicName, int partitionId) {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.DEMOTE_TO_STANDBY.getValue();
    ingestionTaskCommand.topicName = topicName;
    ingestionTaskCommand.partitionId = partitionId;
    return sendIngestionCommandWithRetry(
        ingestionTaskCommand,
        topicName,
        Optional.of(partitionId),
        REQUEST_MAX_ATTEMPT);
  }

  public void resetTopicPartition(String topicName, int partitionId) {
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.RESET_PARTITION.getValue();
    ingestionTaskCommand.topicName = topicName;
    ingestionTaskCommand.partitionId = partitionId;
    sendIngestionCommandWithRetry(ingestionTaskCommand, topicName, Optional.of(partitionId), REQUEST_MAX_ATTEMPT);
  }

  public boolean updateMetadata(IngestionStorageMetadata ingestionStorageMetadata) {
    try {
      LOGGER.info(
          "Sending UPDATE_METADATA request to child process: "
              + IngestionMetadataUpdateType.valueOf(ingestionStorageMetadata.metadataUpdateType) + " for topic: "
              + ingestionStorageMetadata.topicName + " partition: " + ingestionStorageMetadata.partitionId);
      IngestionTaskReport report =
          httpClientTransport.sendRequest(IngestionAction.UPDATE_METADATA, ingestionStorageMetadata);
      return report.isPositive;
    } catch (Exception e) {
      /**
       * We only log the exception when failing to persist metadata updates into child process.
       * Child process might crash, but it will be respawned and will be able to receive future updates.
       */
      LOGGER.warn(
          "Encounter exception when sending metadata updates to child process for topic: "
              + ingestionStorageMetadata.topicName + ", partition: " + ingestionStorageMetadata.partitionId);
      return false;
    }
  }

  public void shutdownForkedProcessComponent(IngestionComponentType ingestionComponentType) {
    // Send ingestion request to ingestion service.
    ProcessShutdownCommand processShutdownCommand = new ProcessShutdownCommand();
    processShutdownCommand.componentType = ingestionComponentType.getValue();
    LOGGER.info("Sending shutdown component request to forked process for component: " + ingestionComponentType.name());
    try {
      httpClientTransport.sendRequest(IngestionAction.SHUTDOWN_COMPONENT, processShutdownCommand);
    } catch (Exception e) {
      LOGGER.warn("Encounter exception when shutting down component: " + ingestionComponentType.name());
    }
  }

  public boolean sendHeartbeatRequest() {
    try {
      httpClientTransport.sendRequest(IngestionAction.HEARTBEAT, getDummyCommand(), heartbeatRequestTimeoutSeconds);
      return true;
    } catch (Exception e) {
      // Don't spam the server logging.
      LOGGER.warn("Unable to get heartbeat from ingestion service");
      return false;
    }
  }

  @Override
  public void close() {
    httpClientTransport.close();
  }

  // Visible for testing
  protected void setHttpClientTransport(HttpClientTransport clientTransport) {
    this.httpClientTransport = clientTransport;
  }

  private boolean sendIngestionCommandWithRetry(
      IngestionTaskCommand command,
      String topicName,
      Optional<Integer> partitionId,
      int requestMaxAttempt) {
    String commandType = IngestionCommandType.valueOf(command.commandType).toString();
    String commandInfo =
        " for topic: " + topicName + (partitionId.map(integer -> (", partition: " + integer)).orElse(""));
    LOGGER.info("Sending request: " + commandType + " to forked process" + commandInfo);
    IngestionTaskReport report;
    try {
      report = httpClientTransport.sendRequestWithRetry(IngestionAction.COMMAND, command, requestMaxAttempt);
    } catch (Exception e) {
      throw new VeniceException("Caught exception when sending command: " + commandType + commandInfo, e);
    }
    if (report != null && report.exceptionThrown) {
      throw new VeniceException(
          "Caught exception when executing command in isolated process: " + commandType + commandInfo + " "
              + report.message);
    }
    return report != null && report.isPositive;
  }

  private void waitHealthCheck() {
    long waitTime = 1000;
    int maxAttempt = 100;
    int retryCount = 0;
    long startTimeInMs = System.currentTimeMillis();
    while (true) {
      try {
        if (sendHeartbeatRequest()) {
          LOGGER.info(
              "Ingestion service server health check passed in " + (System.currentTimeMillis() - startTimeInMs)
                  + " ms.");
          break;
        } else {
          throw new VeniceException("Got non-OK response from ingestion service.");
        }
      } catch (Exception e) {
        retryCount++;
        if (retryCount > maxAttempt) {
          LOGGER.info("Fail to pass health-check for ingestion service after " + maxAttempt + " retries.");
          throw e;
        }
        Utils.sleep(waitTime);
      }
    }
  }
}
