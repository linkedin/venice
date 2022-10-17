package com.linkedin.davinci.ingestion.isolated;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.buildHttpResponse;
import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.createIngestionTaskReport;
import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.deserializeIngestionActionRequest;
import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.getDummyContent;
import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.readHttpRequestContent;
import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.serializeIngestionActionResponse;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.main.MainIngestionStorageMetadataService;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
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
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.ExceptionUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.net.URI;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * IsolatedIngestionServerHandler is the handler class for {@link IsolatedIngestionServer}. This handler will be spawn to handle
 * the following {@link IngestionAction} request from main process:
 * (1) INIT: Initialization request that pass all the configs to initialize the {@link IsolatedIngestionServer} components.
 * (2) COMMAND: Different kinds of ingestion commands to control the ingestion of a given topic (partition)
 * (3) METRIC: Request to collect metrics from child process and report to InGraph service.
 * (4) HEARTBEAT: Request to check the health of child process for monitoring purpose.
 * (5) UPDATE_METADATA: A special kind of request to update metadata of topic partitions opened in main process. As
 * of current ingestion isolation design, metadata partition of a topic will always be opened in child process.
 * {@link MainIngestionStorageMetadataService} maintains in-memory cache of metadata in main
 * process, and it will persist metadata updates via UPDATE_METADATA requests.
 * (6) SHUTDOWN_COMPONENT: Request to shutdown a specific ingestion component gracefully.
 *
 * This class contains all the logic details to handle above requests. Also, it registers ingestion listener which relays
 * status reporting to main process.
 */
public class IsolatedIngestionServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger LOGGER = LogManager.getLogger(IsolatedIngestionServerHandler.class);

  private final IsolatedIngestionServer isolatedIngestionServer;

  public IsolatedIngestionServerHandler(IsolatedIngestionServer isolatedIngestionServer) {
    super();
    this.isolatedIngestionServer = isolatedIngestionServer;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("IsolatedIngestionServerHandler created for listener service.");
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
    try {
      IngestionAction action = getIngestionActionFromRequest(msg);
      byte[] result = getDummyContent();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Received {} message: {}", action.name(), msg);
      }
      if (!isolatedIngestionServer.isInitiated()) {
        throw new VeniceException("Isolated ingestion server is not initialized yet!");
      }
      switch (action) {
        case COMMAND:
          IngestionTaskCommand ingestionTaskCommand =
              deserializeIngestionActionRequest(action, readHttpRequestContent(msg));
          IngestionTaskReport report = handleIngestionTaskCommand(ingestionTaskCommand);
          result = serializeIngestionActionResponse(action, report);
          break;
        case METRIC:
          IngestionMetricsReport metricsReport = handleMetricsRequest();
          result = serializeIngestionActionResponse(action, metricsReport);
          break;
        case HEARTBEAT:
          isolatedIngestionServer.updateHeartbeatTime();
          break;
        case UPDATE_METADATA:
          IngestionStorageMetadata ingestionStorageMetadata =
              deserializeIngestionActionRequest(action, readHttpRequestContent(msg));
          IngestionTaskReport metadataUpdateReport = handleIngestionStorageMetadataUpdate(ingestionStorageMetadata);
          result = serializeIngestionActionResponse(action, metadataUpdateReport);
          break;
        case SHUTDOWN_COMPONENT:
          ProcessShutdownCommand processShutdownCommand =
              deserializeIngestionActionRequest(action, readHttpRequestContent(msg));
          IngestionTaskReport shutdownTaskReport = handleProcessShutdownCommand(processShutdownCommand);
          result = serializeIngestionActionResponse(action, shutdownTaskReport);
          break;
        default:
          throw new UnsupportedOperationException("Unrecognized ingestion action: " + action);
      }
      ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.OK, result));
    } catch (UnsupportedOperationException e) {
      // Here we only handles the bad requests exception. Other errors are handled in exceptionCaught() method.
      LOGGER.error("Caught unrecognized request action:", e);
      ctx.writeAndFlush(
          buildHttpResponse(
              HttpResponseStatus.BAD_REQUEST,
              ExceptionUtils.compactExceptionDescription(e, "channelRead0")));
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOGGER.error("Encounter exception -  message: {}, cause: {}", cause.getMessage(), cause);
    ctx.writeAndFlush(
        buildHttpResponse(
            HttpResponseStatus.INTERNAL_SERVER_ERROR,
            ExceptionUtils.compactExceptionDescription(cause, "exceptionCaught")));
    ctx.close();
  }

  private IngestionTaskReport handleIngestionTaskCommand(IngestionTaskCommand ingestionTaskCommand) {
    long startTimeInMs = System.currentTimeMillis();
    String topicName = ingestionTaskCommand.topicName.toString();
    int partitionId = ingestionTaskCommand.partitionId;
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);

    IngestionTaskReport report = createIngestionTaskReport(topicName, partitionId);
    IngestionCommandType ingestionCommandType = IngestionCommandType.valueOf(ingestionTaskCommand.commandType);
    try {
      if (!isolatedIngestionServer.isInitiated()) {
        throw new VeniceException("IsolatedIngestionServer has not been initiated.");
      }
      KafkaStoreIngestionService storeIngestionService = isolatedIngestionServer.getStoreIngestionService();
      VeniceStoreVersionConfig storeConfig = isolatedIngestionServer.getConfigLoader().getStoreConfig(topicName);
      // Explicitly disable the behavior to restore local data partitions as it might already been opened by main
      // process.
      storeConfig.setRestoreDataPartitions(false);
      switch (ingestionCommandType) {
        case START_CONSUMPTION:
          ReadOnlyStoreRepository storeRepository = isolatedIngestionServer.getStoreRepository();
          // For subscription based store repository, we will need to subscribe to the store explicitly.
          if (storeRepository instanceof SubscriptionBasedReadOnlyStoreRepository) {
            LOGGER.info("Ingestion Service subscribing to store: {}", storeName);
            ((SubscriptionBasedReadOnlyStoreRepository) storeRepository).subscribe(storeName);
          }
          LOGGER.info("Start ingesting partition: {} of topic: {}", partitionId, topicName);
          isolatedIngestionServer.setPartitionToBeSubscribed(topicName, partitionId);
          isolatedIngestionServer.getIngestionBackend().startConsumption(storeConfig, partitionId);
          break;
        case STOP_CONSUMPTION:
          isolatedIngestionServer.getIngestionBackend().stopConsumption(storeConfig, partitionId);
          break;
        case KILL_CONSUMPTION:
          isolatedIngestionServer.getIngestionBackend().killConsumptionTask(topicName);
          isolatedIngestionServer.cleanupTopicState(topicName);
          break;
        case IS_PARTITION_CONSUMING:
          report.isPositive = storeIngestionService.isPartitionConsuming(storeConfig, partitionId);
          break;
        case REMOVE_STORAGE_ENGINE:
          isolatedIngestionServer.getIngestionBackend().removeStorageEngine(topicName);
          isolatedIngestionServer.cleanupTopicState(topicName);
          break;
        case REMOVE_PARTITION:
          /**
           * Here we do not allow storage service to clean up "empty" storage engine. When ingestion isolation is turned on,
           * storage partition will be re-opened in main process after COMPLETED is announced by StoreIngestionTask. Although
           * it might indicate there is no remaining data partitions in the forked process storage engine, it still holds the
           * metadata partition. Cleaning up the "empty storage engine" will (1) delete metadata partition (2) remove storage
           * engine from the map. When a new ingestion request comes in, it will create another metadata partition, but all
           * the metadata stored previously is gone forever...
           */
          isolatedIngestionServer.getIngestionBackend()
              .dropStoragePartitionGracefully(
                  storeConfig,
                  partitionId,
                  isolatedIngestionServer.getStopConsumptionWaitRetriesNum(),
                  false);
          isolatedIngestionServer.cleanupTopicPartitionState(topicName, partitionId);
          break;
        case OPEN_STORAGE_ENGINE:
          // Open metadata partition of the store engine.
          storeConfig.setRestoreDataPartitions(false);
          storeConfig.setRestoreMetadataPartition(true);
          isolatedIngestionServer.getStorageService().openStore(storeConfig, () -> null);
          LOGGER.info("Metadata partition of topic: {} restored.", ingestionTaskCommand.topicName);
          break;
        case PROMOTE_TO_LEADER:
          // This is to avoid the race condition. When partition is being unsubscribed, we should not add it to the
          // action queue, but instead fail the command fast.
          if (isolatedIngestionServer.isPartitionSubscribed(topicName, partitionId)) {
            isolatedIngestionServer.getIngestionBackend()
                .promoteToLeader(
                    storeConfig,
                    partitionId,
                    isolatedIngestionServer.getLeaderSectionIdChecker(topicName, partitionId));
          } else {
            report.isPositive = false;
            LOGGER.info(
                "Partition {} of topic: {} is being unsubscribed, reject leader promotion request",
                partitionId,
                topicName);
          }
          break;
        case DEMOTE_TO_STANDBY:
          if (isolatedIngestionServer.isPartitionSubscribed(topicName, partitionId)) {
            isolatedIngestionServer.getIngestionBackend()
                .demoteToStandby(
                    storeConfig,
                    partitionId,
                    isolatedIngestionServer.getLeaderSectionIdChecker(topicName, partitionId));
          } else {
            report.isPositive = false;
            LOGGER.info(
                "Partition {} of topic: {} is being unsubscribed, reject leader demotion request",
                partitionId,
                topicName);
          }
          break;
        default:
          break;
      }
    } catch (Exception e) {
      LOGGER.error("Encounter exception while handling ingestion command", e);
      report.isPositive = false;
      report.message = e.getClass().getSimpleName() + "_"
          + ExceptionUtils.compactExceptionDescription(e, "handleIngestionTaskCommand");
    }
    long executionTimeInMs = System.currentTimeMillis() - startTimeInMs;
    LOGGER.info(
        "Completed ingestion command {} for topic: {}, partition: {} in ms: {}",
        ingestionCommandType,
        topicName,
        partitionId,
        executionTimeInMs);
    return report;
  }

  private IngestionMetricsReport handleMetricsRequest() {
    IngestionMetricsReport report = new IngestionMetricsReport();
    report.aggregatedMetrics = new HashMap<>();
    if (isolatedIngestionServer.getMetricsRepository() != null) {
      isolatedIngestionServer.getMetricsRepository().metrics().forEach((name, metric) -> {
        if (metric != null) {
          try {
            // Best-effort to reduce metrics delta size sent from child process to main process.
            Double originalValue = isolatedIngestionServer.getMetricsMap().get(name);
            if (originalValue == null || !originalValue.equals(metric.value())) {
              report.aggregatedMetrics.put(name, metric.value());
            }
            isolatedIngestionServer.getMetricsMap().put(name, metric.value());
          } catch (Exception e) {
            String exceptionLogMessage = "Encounter exception when retrieving value of metric: " + name;
            if (!isolatedIngestionServer.getRedundantExceptionFilter().isRedundantException(exceptionLogMessage)) {
              LOGGER.error(exceptionLogMessage, e);
            }
          }
        }
      });
    }
    return report;
  }

  private IngestionTaskReport handleIngestionStorageMetadataUpdate(IngestionStorageMetadata ingestionStorageMetadata) {
    String topicName = ingestionStorageMetadata.topicName.toString();
    int partitionId = ingestionStorageMetadata.partitionId;

    IngestionTaskReport report = createIngestionTaskReport(topicName, partitionId);
    try {
      if (!isolatedIngestionServer.isInitiated()) {
        // Short circuit here when ingestion service is not initiated.
        String errorMessage = "IsolatedIngestionServer has not been initiated.";
        LOGGER.error(errorMessage);
        report.isPositive = false;
        report.message = errorMessage;
        return report;
      }
      switch (IngestionMetadataUpdateType.valueOf(ingestionStorageMetadata.metadataUpdateType)) {
        case PUT_OFFSET_RECORD:
          isolatedIngestionServer.getStorageMetadataService()
              .put(
                  topicName,
                  partitionId,
                  new OffsetRecord(
                      ingestionStorageMetadata.payload.array(),
                      isolatedIngestionServer.getPartitionStateSerializer()));
          break;
        case CLEAR_OFFSET_RECORD:
          isolatedIngestionServer.getStorageMetadataService().clearOffset(topicName, partitionId);
          break;
        case PUT_STORE_VERSION_STATE:
          isolatedIngestionServer.getStorageMetadataService()
              .computeStoreVersionState(
                  topicName,
                  ignored -> IsolatedIngestionUtils
                      .deserializeStoreVersionState(topicName, ingestionStorageMetadata.payload.array()));
          break;
        case CLEAR_STORE_VERSION_STATE:
          isolatedIngestionServer.getStorageMetadataService().clearStoreVersionState(topicName);
          break;
        default:
          break;
      }
    } catch (VeniceException e) {
      LOGGER.error("Encounter exception while updating storage metadata", e);
      // Will not retry the message as the VeniceException indicates topic not found in storage engine repository.
      report.isPositive = true;
      report.message = e.getClass().getSimpleName() + "_"
          + ExceptionUtils.compactExceptionDescription(e, "handleIngestionStorageMetadataUpdate");
    }
    return report;
  }

  private IngestionTaskReport handleProcessShutdownCommand(ProcessShutdownCommand processShutdownCommand) {
    IngestionTaskReport report = createIngestionTaskReport();
    try {
      if (!isolatedIngestionServer.isInitiated()) {
        throw new VeniceException("IsolatedIngestionServer has not been initiated.");
      }
      switch (IngestionComponentType.valueOf(processShutdownCommand.componentType)) {
        case KAFKA_INGESTION_SERVICE:
          isolatedIngestionServer.getStoreIngestionService().stop();
          break;
        case STORAGE_SERVICE:
          isolatedIngestionServer.getStorageService().stop();
          break;
        default:
          break;
      }
    } catch (Exception e) {
      LOGGER.error("Encounter exception while shutting down ingestion components in forked process", e);
      report.isPositive = false;
      report.message = e.getClass().getSimpleName() + "_"
          + ExceptionUtils.compactExceptionDescription(e, "handleProcessShutdownCommand");
    }
    return report;
  }

  private IngestionAction getIngestionActionFromRequest(HttpRequest req) {
    // Sometimes req.uri() gives a full uri (eg https://host:port/path) and sometimes it only gives a path
    // Generating a URI lets us always take just the path.
    String[] requestParts = URI.create(req.uri()).getPath().split("/");
    HttpMethod reqMethod = req.method();
    if (!reqMethod.equals(HttpMethod.POST) || requestParts.length < 2) {
      throw new VeniceException(
          "Only able to parse POST requests for actions: init, command, report.  Cannot parse request for: "
              + req.uri());
    }

    try {
      return IngestionAction.valueOf(requestParts[1].toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new VeniceException(
          "Only able to parse POST requests for actions: init, command, report.  Cannot support action: "
              + requestParts[1],
          e);
    }
  }
}
