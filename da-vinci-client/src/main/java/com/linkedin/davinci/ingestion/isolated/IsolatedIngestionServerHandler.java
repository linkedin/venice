package com.linkedin.davinci.ingestion.isolated;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.ingestion.DefaultIngestionBackend;
import com.linkedin.davinci.ingestion.main.MainIngestionStorageMetadataService;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.repository.VeniceMetadataRepositoryBuilder;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.storage.StorageEngineMetadataService;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.CommonConfigKeys;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.ingestion.protocol.IngestionMetricsReport;
import com.linkedin.venice.ingestion.protocol.IngestionStorageMetadata;
import com.linkedin.venice.ingestion.protocol.IngestionTaskCommand;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.InitializationConfigs;
import com.linkedin.venice.ingestion.protocol.ProcessShutdownCommand;
import com.linkedin.venice.ingestion.protocol.enums.IngestionAction;
import com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType;
import com.linkedin.venice.ingestion.protocol.enums.IngestionComponentType;
import com.linkedin.venice.ingestion.protocol.enums.IngestionReportType;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.IngestionMetadataUpdateType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.security.DefaultSSLFactory;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.VeniceProperties;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.tehuti.metrics.MetricsRepository;
import java.net.URI;
import java.util.HashMap;
import java.util.Optional;
import org.apache.log4j.Logger;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.client.store.ClientFactory.*;


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
  private static final Logger logger = Logger.getLogger(IsolatedIngestionServerHandler.class);

  private final IsolatedIngestionServer isolatedIngestionServer;

  public IsolatedIngestionServerHandler(IsolatedIngestionServer isolatedIngestionServer) {
    super();
    this.isolatedIngestionServer = isolatedIngestionServer;
    if (logger.isDebugEnabled()) {
      logger.debug("IsolatedIngestionServerHandler created for listener service.");
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
      if (logger.isDebugEnabled()) {
        logger.debug("Received " + action.name() + " message: " + msg);
      }
      switch (action) {
        case INIT:
          InitializationConfigs initializationConfigs = deserializeIngestionActionRequest(action, readHttpRequestContent(msg));
          handleIngestionInitialization(initializationConfigs);
          break;
        case COMMAND:
          IngestionTaskCommand ingestionTaskCommand = deserializeIngestionActionRequest(action, readHttpRequestContent(msg));
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
          IngestionStorageMetadata ingestionStorageMetadata = deserializeIngestionActionRequest(action, readHttpRequestContent(msg));
          IngestionTaskReport metadataUpdateReport = handleIngestionStorageMetadataUpdate(ingestionStorageMetadata);
          result = serializeIngestionActionResponse(action, metadataUpdateReport);
          break;
        case SHUTDOWN_COMPONENT:
          ProcessShutdownCommand processShutdownCommand = deserializeIngestionActionRequest(action, readHttpRequestContent(msg));
          IngestionTaskReport shutdownTaskReport = handleProcessShutdownCommand(processShutdownCommand);
          result = serializeIngestionActionResponse(action, shutdownTaskReport);
          break;
        default:
          throw new UnsupportedOperationException("Unrecognized ingestion action: " + action);
      }
      ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.OK, result));
    } catch (UnsupportedOperationException e) {
      // Here we only handles the bad requests exception. Other errors are handled in exceptionCaught() method.
      logger.error("Caught unrecognized request action:", e);
      ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.BAD_REQUEST, e.getMessage()));
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    logger.error("Encounter exception " + cause.getMessage(), cause);
    ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, cause.getMessage()));
    ctx.close();
  }

  private void handleIngestionInitialization(InitializationConfigs initializationConfigs) {
    logger.info("Received aggregated configs: " + initializationConfigs.aggregatedConfigs);

    // Put all configs in aggregated configs into the VeniceConfigLoader.
    PropertyBuilder propertyBuilder = new PropertyBuilder();
    initializationConfigs.aggregatedConfigs.forEach((key, value) -> propertyBuilder.put(key.toString(), value));
    VeniceProperties veniceProperties = propertyBuilder.build();
    VeniceConfigLoader configLoader = new VeniceConfigLoader(veniceProperties, veniceProperties);
    isolatedIngestionServer.setConfigLoader(configLoader);
    isolatedIngestionServer.setStopConsumptionWaitRetriesNum(configLoader.getCombinedProperties().getInt(
        SERVER_STOP_CONSUMPTION_WAIT_RETRIES_NUM, 180));

    // Initialize D2Client.
    SSLFactory sslFactory;
    D2Client d2Client;
    String d2ZkHosts = veniceProperties.getString(D2_CLIENT_ZK_HOSTS_ADDRESS);
    if (veniceProperties.getBoolean(CommonConfigKeys.SSL_ENABLED, false)) {
      try {
        /**
         * TODO: DefaultSSLFactory is a copy of the ssl factory implementation in a version of container lib,
         * we should construct the same SSL Factory being used in the main process with help of ReflectionUtils.
         */
        sslFactory = new DefaultSSLFactory(veniceProperties.toProperties());
      } catch (Exception e) {
        throw new VeniceException("Encounter exception in constructing DefaultSSLFactory", e);
      }
      d2Client = new D2ClientBuilder()
          .setZkHosts(d2ZkHosts)
          .setIsSSLEnabled(true)
          .setSSLParameters(sslFactory.getSSLParameters())
          .setSSLContext(sslFactory.getSSLContext())
          .build();
    } else {
      d2Client = new D2ClientBuilder().setZkHosts(d2ZkHosts).build();
    }
    startD2Client(d2Client);

    // Create the client config.
    ClientConfig clientConfig = new ClientConfig()
        .setD2Client(d2Client)
        .setD2ServiceName(ClientConfig.DEFAULT_D2_SERVICE_NAME);
    String clusterName = configLoader.getVeniceClusterConfig().getClusterName();

    // Create MetricsRepository
    MetricsRepository metricsRepository = new MetricsRepository();
    isolatedIngestionServer.setMetricsRepository(metricsRepository);

    // Initialize store/schema repositories.
    VeniceMetadataRepositoryBuilder veniceMetadataRepositoryBuilder = new VeniceMetadataRepositoryBuilder(
        configLoader,
        clientConfig,
        metricsRepository,
        null,
        true);
    ReadOnlyStoreRepository storeRepository = veniceMetadataRepositoryBuilder.getStoreRepo();
    ReadOnlySchemaRepository schemaRepository = veniceMetadataRepositoryBuilder.getSchemaRepo();
    Optional<HelixReadOnlyZKSharedSchemaRepository> helixReadOnlyZKSharedSchemaRepository = veniceMetadataRepositoryBuilder.getReadOnlyZKSharedSchemaRepository();
    ClusterInfoProvider clusterInfoProvider = veniceMetadataRepositoryBuilder.getClusterInfoProvider();
    isolatedIngestionServer.setStoreRepository(storeRepository);

    SchemaReader partitionStateSchemaReader = ClientFactory.getSchemaReader(
        ClientConfig.cloneConfig(clientConfig).setStoreName(AvroProtocolDefinition.PARTITION_STATE.getSystemStoreName()));
    SchemaReader storeVersionStateSchemaReader = ClientFactory.getSchemaReader(
        ClientConfig.cloneConfig(clientConfig).setStoreName(AvroProtocolDefinition.STORE_VERSION_STATE.getSystemStoreName()));
    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer = AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    partitionStateSerializer.setSchemaReader(partitionStateSchemaReader);
    isolatedIngestionServer.setPartitionStateSerializer(partitionStateSerializer);
    InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer = AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
    storeVersionStateSerializer.setSchemaReader(storeVersionStateSchemaReader);
    isolatedIngestionServer.setStoreVersionStateSerializer(storeVersionStateSerializer);

    // Create RocksDBMemoryStats. For now RocksDBMemoryStats cannot work with SharedConsumerPool.
    RocksDBMemoryStats rocksDBMemoryStats = ((configLoader.getVeniceServerConfig().isDatabaseMemoryStatsEnabled())
        && (!configLoader.getVeniceServerConfig().isSharedConsumerPoolEnabled()))?
        new RocksDBMemoryStats(metricsRepository, "RocksDBMemoryStats", configLoader.getVeniceServerConfig().getRocksDBServerConfig().isRocksDBPlainTableFormatEnabled()) : null;

    /**
     * Using reflection to create all the stats classes related to ingestion isolation. All these classes extends
     * {@link AbstractVeniceStats} class and takes {@link MetricsRepository} as the only parameter in its constructor.
     */
    for (String ingestionIsolationStatsClassName : veniceProperties.getString(SERVER_INGESTION_ISOLATION_STATS_CLASS_LIST, "").split(",")) {
      if (ingestionIsolationStatsClassName.length() != 0) {
        Class<? extends AbstractVeniceStats> ingestionIsolationStatsClass = ReflectUtils.loadClass(ingestionIsolationStatsClassName);
        if (!AbstractVeniceStats.class.isAssignableFrom(ingestionIsolationStatsClass)) {
          throw new VeniceException("Class: " + ingestionIsolationStatsClassName + " does not extends AbstractVeniceStats");
        }
        AbstractVeniceStats ingestionIsolationStats =
            ReflectUtils.callConstructor(ingestionIsolationStatsClass, new Class<?>[]{MetricsRepository.class}, new Object[]{metricsRepository});
        logger.info("Created Ingestion Isolation stats: " + ingestionIsolationStats.getName());
      } else {
        logger.info("Ingestion isolation stats class name is empty, will skip it.");
      }
    }

    // Create StorageService
    AggVersionedStorageEngineStats storageEngineStats = new AggVersionedStorageEngineStats(metricsRepository, storeRepository);
    /**
     * The reason of not to restore the data partitions during initialization of storage service is:
     * 1. During first fresh start up with no data on disk, we don't need to restore anything
     * 2. During fresh start up with data on disk (aka bootstrap), we will receive messages to subscribe to the partition
     * and it will re-open the partition on demand.
     * 3. During crash recovery restart, partitions that are already ingestion will be opened by parent process and we
     * should not try to open it. The remaining ingestion tasks will open the storage engines.
     */
    StorageService storageService = new StorageService(configLoader, storageEngineStats, rocksDBMemoryStats, storeVersionStateSerializer, partitionStateSerializer, storeRepository, false, true);
    storageService.start();
    isolatedIngestionServer.setStorageService(storageService);

    // Create SchemaReader
    SchemaReader kafkaMessageEnvelopeSchemaReader = getSchemaReader(
        ClientConfig.cloneConfig(clientConfig).setStoreName(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName())
    );

    StorageMetadataService storageMetadataService = new StorageEngineMetadataService(storageService.getStorageEngineRepository(), partitionStateSerializer);
    isolatedIngestionServer.setStorageMetadataService(storageMetadataService);

    StorageEngineBackedCompressorFactory compressorFactory = new StorageEngineBackedCompressorFactory(storageMetadataService);

    // Create KafkaStoreIngestionService
    KafkaStoreIngestionService storeIngestionService = new KafkaStoreIngestionService(
        storageService.getStorageEngineRepository(),
        configLoader,
        storageMetadataService,
        clusterInfoProvider,
        storeRepository,
        schemaRepository,
        metricsRepository,
        rocksDBMemoryStats,
        Optional.of(kafkaMessageEnvelopeSchemaReader),
        veniceMetadataRepositoryBuilder.isDaVinciClient() ? Optional.empty() : Optional.of(clientConfig),
        partitionStateSerializer,
        helixReadOnlyZKSharedSchemaRepository,
        null,
        true,
        compressorFactory,
        Optional.empty());
    storeIngestionService.start();
    storeIngestionService.addCommonNotifier(ingestionListener);
    isolatedIngestionServer.setStoreIngestionService(storeIngestionService);
    isolatedIngestionServer.setIngestionBackend(new DefaultIngestionBackend(storageMetadataService, storeIngestionService, storageService));

    logger.info("Starting report client with target application port: " + configLoader.getVeniceServerConfig().getIngestionApplicationPort());
    // Create Netty client to report status back to application.
    IsolatedIngestionRequestClient reportClient = new IsolatedIngestionRequestClient(configLoader.getVeniceServerConfig().getIngestionApplicationPort());
    isolatedIngestionServer.setReportClient(reportClient);

    // Mark the IsolatedIngestionServer as initiated.
    isolatedIngestionServer.setInitiated(true);
  }

  private IngestionTaskReport handleIngestionTaskCommand(IngestionTaskCommand ingestionTaskCommand) {
    long startTimeInMs = System.currentTimeMillis();
    String topicName = ingestionTaskCommand.topicName.toString();
    int partitionId = ingestionTaskCommand.partitionId;
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);

    IngestionTaskReport report = createIngestionTaskReport(topicName, partitionId);
    IngestionCommandType ingestionCommandType =  IngestionCommandType.valueOf(ingestionTaskCommand.commandType);
    try {
      if (!isolatedIngestionServer.isInitiated()) {
        throw new VeniceException("IsolatedIngestionServer has not been initiated.");
      }
      VeniceStoreConfig storeConfig = isolatedIngestionServer.getConfigLoader().getStoreConfig(topicName);
      KafkaStoreIngestionService storeIngestionService = isolatedIngestionServer.getStoreIngestionService();

      switch (ingestionCommandType) {
        case START_CONSUMPTION:
          ReadOnlyStoreRepository storeRepository = isolatedIngestionServer.getStoreRepository();
          // For subscription based store repository, we will need to subscribe to the store explicitly.
          if (storeRepository instanceof SubscriptionBasedReadOnlyStoreRepository) {
            logger.info("Ingestion Service subscribing to store: " + storeName);
            ((SubscriptionBasedReadOnlyStoreRepository)storeRepository).subscribe(storeName);
          }
          logger.info("Start ingesting partition: " + partitionId + " of topic: " + topicName);
          isolatedIngestionServer.setPartitionToBeSubscribed(topicName, partitionId);
          isolatedIngestionServer.getIngestionBackend().startConsumption(storeConfig, partitionId);
          break;
        case STOP_CONSUMPTION:
          isolatedIngestionServer.getIngestionBackend().stopConsumption(storeConfig, partitionId);
          break;
        case KILL_CONSUMPTION:
          isolatedIngestionServer.getIngestionBackend().killConsumptionTask(topicName);
          break;
        case RESET_CONSUMPTION:
          storeIngestionService.resetConsumptionOffset(storeConfig, partitionId);
          break;
        case IS_PARTITION_CONSUMING:
          report.isPositive = storeIngestionService.isPartitionConsuming(storeConfig, partitionId);
          break;
        case REMOVE_STORAGE_ENGINE:
          isolatedIngestionServer.getIngestionBackend().removeStorageEngine(topicName);
          break;
        case REMOVE_PARTITION:
          isolatedIngestionServer.getIngestionBackend().dropStoragePartitionGracefully(storeConfig, partitionId, isolatedIngestionServer.getStopConsumptionWaitRetriesNum());
          break;
        case OPEN_STORAGE_ENGINE:
          // Open metadata partition of the store engine.
          storeConfig.setRestoreDataPartitions(false);
          storeConfig.setRestoreMetadataPartition(true);
          isolatedIngestionServer.getStorageService().openStore(storeConfig);
          logger.info("Metadata partition of topic: " + ingestionTaskCommand.topicName.toString() + " restored.");
          break;
        case PROMOTE_TO_LEADER:
          // This is to avoid the race condition. When partition is being unsubscribed, we should not add it to the action queue, but instead fail the command fast.
          if (isolatedIngestionServer.isPartitionSubscribed(topicName, partitionId)) {
            isolatedIngestionServer.getIngestionBackend().promoteToLeader(storeConfig, partitionId, isolatedIngestionServer.getLeaderSectionIdChecker(topicName, partitionId));
          } else {
            report.isPositive = false;
            logger.info("Partition " + partitionId + " of topic: " + topicName + " is being unsubscribed, reject leader promotion request");
          }
          break;
        case DEMOTE_TO_STANDBY:
          if (isolatedIngestionServer.isPartitionSubscribed(topicName, partitionId)) {
            isolatedIngestionServer.getIngestionBackend().demoteToStandby(storeConfig, partitionId, isolatedIngestionServer.getLeaderSectionIdChecker(topicName, partitionId));
          } else {
            report.isPositive = false;
            logger.info("Partition " + partitionId + " of topic: " + topicName + " is being unsubscribed, reject leader demotion request");
          }
          break;
        default:
          break;
      }
    } catch (Exception e) {
      logger.error("Encounter exception while handling ingestion command", e);
      report.isPositive = false;
      report.message = e.getClass().getSimpleName() + "_" + e.getMessage();
    }
    long executionTimeInMs = System.currentTimeMillis() - startTimeInMs;
    logger.info("Completed ingestion command " + ingestionCommandType +  " for topic: " + topicName + ", partition: " + partitionId + " in millis: " + executionTimeInMs);
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
            if (originalValue == null || originalValue.equals(metric.value())) {
              report.aggregatedMetrics.put(name, metric.value());
            }
            isolatedIngestionServer.getMetricsMap().put(name, metric.value());
          } catch (Exception e) {
            String exceptionLogMessage = "Encounter exception when retrieving value of metric: " + name;
            if (!isolatedIngestionServer.getRedundantExceptionFilter().isRedundantException(exceptionLogMessage)) {
              logger.error(exceptionLogMessage, e);
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
        logger.error(errorMessage);
        report.isPositive = false;
        report.message = errorMessage;
        return report;
      }
      switch (IngestionMetadataUpdateType.valueOf(ingestionStorageMetadata.metadataUpdateType)) {
        case PUT_OFFSET_RECORD:
          isolatedIngestionServer.getStorageMetadataService().put(topicName, partitionId, new OffsetRecord(ingestionStorageMetadata.payload.array(), isolatedIngestionServer
              .getPartitionStateSerializer()));
          break;
        case CLEAR_OFFSET_RECORD:
          isolatedIngestionServer.getStorageMetadataService().clearOffset(topicName, partitionId);
          break;
        case PUT_STORE_VERSION_STATE:
          isolatedIngestionServer.getStorageMetadataService().put(topicName, IsolatedIngestionUtils.deserializeStoreVersionState(topicName, ingestionStorageMetadata.payload.array()));
          break;
        case CLEAR_STORE_VERSION_STATE:
          isolatedIngestionServer.getStorageMetadataService().clearStoreVersionState(topicName);
          break;
        default:
          break;
      }
    } catch (Exception e) {
      logger.error("Encounter exception while updating storage metadata", e);
      report.isPositive = false;
      report.message = e.getClass().getSimpleName() + "_" + e.getMessage();
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
      logger.error("Encounter exception while shutting down ingestion components in forked process", e);
      report.isPositive = false;
      report.message = e.getClass().getSimpleName() + "_" + e.getMessage();
    }
    return report;
  }

  private IngestionAction getIngestionActionFromRequest(HttpRequest req){
    // Sometimes req.uri() gives a full uri (eg https://host:port/path) and sometimes it only gives a path
    // Generating a URI lets us always take just the path.
    String[] requestParts = URI.create(req.uri()).getPath().split("/");
    HttpMethod reqMethod = req.method();
    if (!reqMethod.equals(HttpMethod.POST) || requestParts.length < 2) {
      throw new VeniceException("Only able to parse POST requests for actions: init, command, report.  Cannot parse request for: " + req.uri());
    }

    try {
      return IngestionAction.valueOf(requestParts[1].toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new VeniceException("Only able to parse POST requests for actions: init, command, report.  Cannot support action: " + requestParts[1], e);
    }
  }

  private final VeniceNotifier ingestionListener = new VeniceNotifier() {
    @Override
    public void completed(String kafkaTopic, int partitionId, long offset, String message) {
      IngestionTaskReport report = createIngestionTaskReport(IngestionReportType.COMPLETED, kafkaTopic, partitionId, offset, message);
      isolatedIngestionServer.reportIngestionStatus(report);
    }

    @Override
    public void error(String kafkaTopic, int partitionId, String message, Exception e) {
      IngestionTaskReport report = createIngestionTaskReport(IngestionReportType.ERROR, kafkaTopic, partitionId, e.getClass().getSimpleName() + "_" + e.getMessage());
      isolatedIngestionServer.reportIngestionStatus(report);
    }

    @Override
    public void started(String kafkaTopic, int partitionId, String message) {
      IngestionTaskReport report = createIngestionTaskReport(IngestionReportType.STARTED, kafkaTopic, partitionId, message);
      isolatedIngestionServer.reportIngestionStatus(report);
    }

    @Override
    public void restarted(String kafkaTopic, int partitionId, long offset, String message) {
      IngestionTaskReport report = createIngestionTaskReport(IngestionReportType.RESTARTED, kafkaTopic, partitionId, offset, message);
      isolatedIngestionServer.reportIngestionStatus(report);
    }

    @Override
    public void endOfPushReceived(String kafkaTopic, int partitionId, long offset, String message) {
      IngestionTaskReport report = createIngestionTaskReport(IngestionReportType.END_OF_PUSH_RECEIVED, kafkaTopic, partitionId, offset, message);
      isolatedIngestionServer.reportIngestionStatus(report);
    }

    @Override
    public void startOfBufferReplayReceived(String kafkaTopic, int partitionId, long offset, String message) {
      IngestionTaskReport report = createIngestionTaskReport(IngestionReportType.START_OF_BUFFER_REPLAY_RECEIVED, kafkaTopic, partitionId, offset, message);
      isolatedIngestionServer.reportIngestionStatus(report);
    }

    @Override
    public void startOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset, String incrementalPushVersion) {
      IngestionTaskReport report = createIngestionTaskReport(IngestionReportType.START_OF_INCREMENTAL_PUSH_RECEIVED, kafkaTopic, partitionId, offset, incrementalPushVersion);
      isolatedIngestionServer.reportIngestionStatus(report);
    }

    @Override
    public void endOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset, String incrementalPushVersion) {
      IngestionTaskReport report = createIngestionTaskReport(IngestionReportType.END_OF_INCREMENTAL_PUSH_RECEIVED, kafkaTopic, partitionId, offset, incrementalPushVersion);
      isolatedIngestionServer.reportIngestionStatus(report);
    }

    @Override
    public void topicSwitchReceived(String kafkaTopic, int partitionId, long offset, String message) {
      IngestionTaskReport report = createIngestionTaskReport(IngestionReportType.TOPIC_SWITCH_RECEIVED, kafkaTopic, partitionId, offset, message);
      isolatedIngestionServer.reportIngestionStatus(report);
    }

    @Override
    public void progress(String kafkaTopic, int partitionId, long offset, String message) {
      IngestionTaskReport report = createIngestionTaskReport(IngestionReportType.PROGRESS, kafkaTopic, partitionId, offset, message);
      isolatedIngestionServer.reportIngestionStatus(report);
    }
  };
}
