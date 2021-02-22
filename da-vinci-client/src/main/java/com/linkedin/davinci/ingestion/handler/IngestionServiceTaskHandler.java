package com.linkedin.davinci.ingestion.handler;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.ingestion.IngestionRequestClient;
import com.linkedin.davinci.ingestion.IngestionService;
import com.linkedin.davinci.ingestion.IngestionUtils;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.repository.NativeMetadataRepository;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.storage.StorageEngineMetadataService;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.CommonConfigKeys;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.SubscriptionBasedStoreRepository;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.ingestion.protocol.IngestionMetricsReport;
import com.linkedin.venice.ingestion.protocol.IngestionStorageMetadata;
import com.linkedin.venice.ingestion.protocol.IngestionTaskCommand;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.InitializationConfigs;
import com.linkedin.venice.ingestion.protocol.ProcessShutdownCommand;
import com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType;
import com.linkedin.venice.ingestion.protocol.enums.IngestionComponentType;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.ingestion.protocol.enums.IngestionAction;
import com.linkedin.venice.meta.IngestionMetadataUpdateType;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.StaticClusterInfoProvider;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.security.DefaultSSLFactory;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.RedundantExceptionFilter;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.log4j.Logger;

import static com.linkedin.davinci.ingestion.IngestionUtils.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.client.store.ClientFactory.*;


public class IngestionServiceTaskHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger logger = Logger.getLogger(IngestionServiceTaskHandler.class);
  private static final RedundantExceptionFilter redundantExceptionFilter = RedundantExceptionFilter.getRedundantExceptionFilter(RedundantExceptionFilter.DEFAULT_BITSET_SIZE, TimeUnit.MINUTES.toMillis(10));
  private final IngestionService ingestionService;

  public IngestionServiceTaskHandler(IngestionService ingestionService) {
    super();
    this.ingestionService = ingestionService;
    if (logger.isDebugEnabled()) {
      logger.debug("IngestionServiceTaskHandler created for listener service.");
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
      switch (action) {
        case INIT:
          if (logger.isDebugEnabled()) {
            logger.debug("Received INIT message: " + msg.toString());
          }
          InitializationConfigs initializationConfigs = deserializeIngestionActionRequest(action, readHttpRequestContent(msg));
          handleIngestionInitialization(initializationConfigs);
          ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.OK, getDummyContent()));
          break;
        case COMMAND:
          if (logger.isDebugEnabled()) {
            logger.debug("Received COMMAND message " + msg.toString());
          }
          IngestionTaskCommand ingestionTaskCommand = deserializeIngestionActionRequest(action, readHttpRequestContent(msg));
          IngestionTaskReport report = handleIngestionTaskCommand(ingestionTaskCommand);
          byte[] serializedReport = serializeIngestionActionResponse(action, report);
          ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.OK, serializedReport));
          break;
        case METRIC:
          if (logger.isDebugEnabled()) {
            logger.debug("Received METRIC message.");
          }
          IngestionMetricsReport metricsReport = handleMetricsRequest();
          byte[] serializedMetricsReport = serializeIngestionActionResponse(action, metricsReport);
          ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.OK, serializedMetricsReport));
          break;
        case HEARTBEAT:
          if (logger.isDebugEnabled()) {
            logger.debug("Received HEARTBEAT message.");
          }
          ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.OK, getDummyContent()));
          break;
        case UPDATE_METADATA:
          if (logger.isDebugEnabled()) {
            logger.debug("Received UPDATE_METADATA message.");
          }
          IngestionStorageMetadata ingestionStorageMetadata = deserializeIngestionActionRequest(action, readHttpRequestContent(msg));
          IngestionTaskReport metadataUpdateReport = handleIngestionStorageMetadataUpdate(ingestionStorageMetadata);
          byte[] serializedMetadataUpdateReport = serializeIngestionActionResponse(action, metadataUpdateReport);
          ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.OK, serializedMetadataUpdateReport));
          break;
        case SHUTDOWN_COMPONENT:
          logger.info("Received SHUTDOWN_COMPONENT message.");
          ProcessShutdownCommand processShutdownCommand = deserializeIngestionActionRequest(action, readHttpRequestContent(msg));
          IngestionTaskReport shutdownTaskReport = handleProcessShutdownCommand(processShutdownCommand);
          byte[] serializedShutdownTaskReport = serializeIngestionActionResponse(action, shutdownTaskReport);
          ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.OK, serializedShutdownTaskReport));
          break;
        default:
          throw new UnsupportedOperationException("Unrecognized ingestion action: " + action);
      }
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
    /**
     * The reason of not to restore the data partitions during initialization of storage service is:
     * 1. During first fresh start up with no data on disk, we don't need to restore anything
     * 2. During fresh start up with data on disk (aka bootstrap), we will receive messages to subscribe to the partition
     * and it will re-open the partition on demand.
     * 3. During crash recovery restart, partitions that are already ingestion will be opened by parent process and we
     * should not try to open it. The remaining ingestion tasks will open the storage engines.
     */
    propertyBuilder.put(ConfigKeys.SERVER_RESTORE_DATA_PARTITIONS_ENABLED, "false");
    VeniceProperties veniceProperties = propertyBuilder.build();
    VeniceConfigLoader configLoader = new VeniceConfigLoader(veniceProperties, veniceProperties);
    ingestionService.setConfigLoader(configLoader);

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
    ingestionService.setMetricsRepository(metricsRepository);

    // Create ZkClient
    HelixAdapterSerializer adapter = new HelixAdapterSerializer();
    ZkClient zkClient = ZkClientFactory.newZkClient(configLoader.getVeniceClusterConfig().getZookeeperAddress());
    zkClient.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, ".ingestion-service-zk-client"));

    // Create StoreRepository and SchemaRepository
    SubscriptionBasedReadOnlyStoreRepository storeRepository;
    ReadOnlySchemaRepository schemaRepository;
    boolean useSystemStore = veniceProperties.getBoolean(ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY, false);
    logger.info("Isolated ingestion service uses system store repository: " + useSystemStore);
    ClusterInfoProvider clusterInfoProvider;
    if (useSystemStore) {
      logger.info("Initializing IngestionServiceTaskHandler with " + NativeMetadataRepository.class.getSimpleName());
      NativeMetadataRepository
          systemStoreBasedRepository = NativeMetadataRepository.getInstance(clientConfig, veniceProperties);
      systemStoreBasedRepository.refresh();
      clusterInfoProvider = systemStoreBasedRepository;
      storeRepository = systemStoreBasedRepository;
      schemaRepository = systemStoreBasedRepository;
    } else {
      clusterInfoProvider = new StaticClusterInfoProvider(Collections.singleton(clusterName));
      storeRepository = new SubscriptionBasedStoreRepository(zkClient, adapter, clusterName);
      storeRepository.refresh();
      schemaRepository = new HelixReadOnlySchemaRepository(storeRepository, zkClient, adapter, clusterName, 3, 1000);
      schemaRepository.refresh();
    }
    ingestionService.setStoreRepository(storeRepository);

    SchemaReader partitionStateSchemaReader = ClientFactory.getSchemaReader(
        ClientConfig.cloneConfig(clientConfig).setStoreName(AvroProtocolDefinition.PARTITION_STATE.getSystemStoreName()));
    SchemaReader storeVersionStateSchemaReader = ClientFactory.getSchemaReader(
        ClientConfig.cloneConfig(clientConfig).setStoreName(AvroProtocolDefinition.STORE_VERSION_STATE.getSystemStoreName()));
    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer = AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    partitionStateSerializer.setSchemaReader(partitionStateSchemaReader);
    ingestionService.setPartitionStateSerializer(partitionStateSerializer);
    InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer = AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
    storeVersionStateSerializer.setSchemaReader(storeVersionStateSchemaReader);
    ingestionService.setStoreVersionStateSerializer(storeVersionStateSerializer);

    // Create RocksDBMemoryStats
    RocksDBMemoryStats rocksDBMemoryStats = configLoader.getVeniceServerConfig().isDatabaseMemoryStatsEnabled() ?
        new RocksDBMemoryStats(metricsRepository, "RocksDBMemoryStats", configLoader.getVeniceServerConfig().getRocksDBServerConfig().isRocksDBPlainTableFormatEnabled()) : null;

    /**
     * Using reflection to create all the stats classes related to ingestion isolation. All these classes extends
     * {@link AbstractVeniceStats} class and takes {@link MetricsRepository} as the only parameter in its constructor.
     */
    for (String ingestionIsolationStatsClassName : veniceProperties.getString(SERVER_INGESTION_ISOLATION_STATS_CLASS_LIST, "").split(",")) {
      if (ingestionIsolationStatsClassName.length() != 0) {
        Class<? extends AbstractVeniceStats> ingestionIsolationStatsClass = ReflectUtils.loadClass(ingestionIsolationStatsClassName);
        if (!ingestionIsolationStatsClass.isAssignableFrom(AbstractVeniceStats.class)) {
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
    StorageService storageService = new StorageService(configLoader, storageEngineStats, rocksDBMemoryStats, storeVersionStateSerializer, partitionStateSerializer);
    storageService.start();
    ingestionService.setStorageService(storageService);

    // Create SchemaReader
    SchemaReader kafkaMessageEnvelopeSchemaReader = getSchemaReader(
        ClientConfig.cloneConfig(clientConfig).setStoreName(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName())
    );

    StorageMetadataService storageMetadataService = new StorageEngineMetadataService(storageService.getStorageEngineRepository(), partitionStateSerializer);
    ingestionService.setStorageMetadataService(storageMetadataService);

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
        Optional.empty(),
        partitionStateSerializer);
    storeIngestionService.start();
    storeIngestionService.addCommonNotifier(ingestionListener);
    ingestionService.setStoreIngestionService(storeIngestionService);

    logger.info("Starting report client with target application port: " + configLoader.getVeniceServerConfig().getIngestionApplicationPort());
    // Create Netty client to report status back to application.
    IngestionRequestClient reportClient = new IngestionRequestClient(configLoader.getVeniceServerConfig().getIngestionApplicationPort());
    ingestionService.setReportClient(reportClient);

    // Mark the IngestionService as initiated.
    ingestionService.setInitiated(true);
  }

  private IngestionTaskReport handleIngestionTaskCommand(IngestionTaskCommand ingestionTaskCommand) {
    String topicName = ingestionTaskCommand.topicName.toString();
    int partitionId = ingestionTaskCommand.partitionId;

    IngestionTaskReport report = new IngestionTaskReport();
    report.isPositive = true;
    report.message = "";
    report.topicName = topicName;
    report.partitionId = partitionId;
    try {
      if (!ingestionService.isInitiated()) {
        throw new VeniceException("IngestionService has not been initiated.");
      }
      VeniceStoreConfig storeConfig = ingestionService.getConfigLoader().getStoreConfig(topicName);
      StorageService storageService = ingestionService.getStorageService();
      KafkaStoreIngestionService storeIngestionService = ingestionService.getStoreIngestionService();

      switch (IngestionCommandType.valueOf(ingestionTaskCommand.commandType)) {
        case START_CONSUMPTION:
          IngestionMode ingestionMode = ingestionService.getConfigLoader().getVeniceServerConfig().getIngestionMode();
          if (!ingestionMode.equals(IngestionMode.ISOLATED)) {
            throw new VeniceException("Ingestion isolation is not enabled.");
          }
          // Subscribe to the store in store repository.
          String storeName = Version.parseStoreFromKafkaTopicName(topicName);
          // Ingestion Service needs store repository to subscribe to the store.
          ingestionService.getStoreRepository().subscribe(storeName);
          logger.info("Start ingesting partition: " + partitionId + " of topic: " + topicName);

          storageService.openStoreForNewPartition(storeConfig, partitionId);
          storeIngestionService.startConsumption(storeConfig, partitionId);
          break;
        case STOP_CONSUMPTION:
          storeIngestionService.stopConsumption(storeConfig, partitionId);
          break;
        case KILL_CONSUMPTION:
          storeIngestionService.killConsumptionTask(topicName);
          break;
        case RESET_CONSUMPTION:
          storeIngestionService.resetConsumptionOffset(storeConfig, partitionId);
          break;
        case IS_PARTITION_CONSUMING:
          report.isPositive = storeIngestionService.isPartitionConsuming(storeConfig, partitionId);
          break;
        case REMOVE_STORAGE_ENGINE:
          ingestionService.getStorageService().removeStorageEngine(ingestionTaskCommand.topicName.toString());
          logger.info("Remaining storage engines after dropping: " + ingestionService.getStorageService().getStorageEngineRepository().getAllLocalStorageEngines().toString());
          break;
        case REMOVE_PARTITION:
          if (storeIngestionService.isPartitionConsuming(storeConfig, partitionId)) {
            storeIngestionService.stopConsumptionAndWait(storeConfig, partitionId, 1, 30);
            logger.info("Partition: " + partitionId + " of topic: " + topicName + " has stopped consumption.");
          }
          ingestionService.getStorageService().dropStorePartition(storeConfig, partitionId);
          logger.info("Partition: " + partitionId + " of topic: " + topicName + " has been removed.");
        default:
          break;
      }
    } catch (Exception e) {
      logger.error("Encounter exception while handling ingestion command", e);
      report.isPositive = false;
      report.isError = true;
      report.message = e.getClass().getSimpleName() + "_" + e.getMessage();
    }
    return report;
  }

  private IngestionMetricsReport handleMetricsRequest() {
    IngestionMetricsReport report = new IngestionMetricsReport();
    report.aggregatedMetrics = new HashMap<>();
    if (ingestionService.getMetricsRepository() != null) {
      ingestionService.getMetricsRepository().metrics().forEach((name, metric) -> {
        if (metric != null) {
          try {
            report.aggregatedMetrics.put(name, metric.value());
          } catch (Exception e) {
            String exceptionLogMessage = "Encounter exception when retrieving value of metric: " + name;
            if (!redundantExceptionFilter.isRedundantException(exceptionLogMessage)) {
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

    IngestionTaskReport report = new IngestionTaskReport();
    report.isPositive = true;
    report.message = "";
    report.topicName = topicName;
    report.partitionId = partitionId;
    try {
      if (!ingestionService.isInitiated()) {
        throw new VeniceException("IngestionService has not been initiated.");
      }
      switch (IngestionMetadataUpdateType.valueOf(ingestionStorageMetadata.metadataUpdateType)) {
        case PUT_OFFSET_RECORD:
          if (logger.isDebugEnabled()) {
            logger.debug("Put OffsetRecord");
          }
          ingestionService.getStorageMetadataService().put(topicName, partitionId, new OffsetRecord(ingestionStorageMetadata.payload.array(), ingestionService.getPartitionStateSerializer()));
          break;
        case CLEAR_OFFSET_RECORD:
          if (logger.isDebugEnabled()) {
            logger.debug("Clear OffsetRecord");
          }
          ingestionService.getStorageMetadataService().clearOffset(topicName, partitionId);
          break;
        case PUT_STORE_VERSION_STATE:
          if (logger.isDebugEnabled()) {
            logger.debug("Put StoreVersionState");
          }
          ingestionService.getStorageMetadataService().put(topicName, IngestionUtils.deserializeStoreVersionState(topicName, ingestionStorageMetadata.payload.array()));
          break;
        case CLEAR_STORE_VERSION_STATE:
          if (logger.isDebugEnabled()) {
            logger.debug("Clear StoreVersionState");
          }
          ingestionService.getStorageMetadataService().clearStoreVersionState(topicName);
          break;
        default:
          break;
      }
    } catch (Exception e) {
      logger.error("Encounter exception while updating storage metadata", e);
      report.isPositive = false;
      report.isError = true;
      report.message = e.getClass().getSimpleName() + "_" + e.getMessage();
    }
    return report;
  }

  private IngestionTaskReport handleProcessShutdownCommand(ProcessShutdownCommand processShutdownCommand) {
    IngestionTaskReport report = new IngestionTaskReport();
    report.isPositive = true;
    report.message = "";
    report.topicName = "";
    try {
      if (!ingestionService.isInitiated()) {
        throw new VeniceException("IngestionService has not been initiated.");
      }
      switch (IngestionComponentType.valueOf(processShutdownCommand.componentType)) {
        case KAFKA_INGESTION_SERVICE:
          ingestionService.getStoreIngestionService().stop();
          break;
        case STORAGE_SERVICE:
          ingestionService.getStorageService().stop();
          break;
        default:
          break;
      }
    } catch (Exception e) {
      logger.error("Encounter exception while shutting down ingestion components in forked process", e);
      report.isPositive = false;
      report.isError = true;
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
      IngestionTaskReport report = new IngestionTaskReport();
      report.isCompleted = true;
      report.message = message;
      report.topicName = kafkaTopic;
      report.partitionId = partitionId;
      report.offset = offset;
      ingestionService.reportIngestionStatus(report);

    }

    @Override
    public void error(String kafkaTopic, int partitionId, String message, Exception e) {
      IngestionTaskReport report = new IngestionTaskReport();
      report.isError = true;
      report.message = e.getClass().getSimpleName() + "_" + e.getMessage();
      report.topicName = kafkaTopic;
      report.partitionId = partitionId;
      ingestionService.reportIngestionStatus(report);
    }

    @Override
    public void started(String kafkaTopic, int partitionId, String message) {
      IngestionTaskReport report = new IngestionTaskReport();
      report.isStarted = true;
      report.message = message;
      report.topicName = kafkaTopic;
      report.partitionId = partitionId;
      ingestionService.reportIngestionStatus(report);
    }

    @Override
    public void restarted(String kafkaTopic, int partitionId, long offset, String message) {
      IngestionTaskReport report = new IngestionTaskReport();
      report.isRestarted = true;
      report.message = message;
      report.topicName = kafkaTopic;
      report.partitionId = partitionId;
      report.offset = offset;
      ingestionService.reportIngestionStatus(report);
    }

    @Override
    public void endOfPushReceived(String kafkaTopic, int partitionId, long offset, String message) {
      IngestionTaskReport report = new IngestionTaskReport();
      report.isRestarted = true;
      report.message = message;
      report.topicName = kafkaTopic;
      report.partitionId = partitionId;
      report.offset = offset;
      ingestionService.reportIngestionStatus(report);
    }

    @Override
    public void startOfBufferReplayReceived(String kafkaTopic, int partitionId, long offset, String message) {
      IngestionTaskReport report = new IngestionTaskReport();
      report.isStartOfBufferReplayReceived = true;
      report.message = message;
      report.topicName = kafkaTopic;
      report.partitionId = partitionId;
      report.offset = offset;
      ingestionService.reportIngestionStatus(report);
    }

    @Override
    public void startOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset, String incrementalPushVersion) {
      IngestionTaskReport report = new IngestionTaskReport();
      report.isStartOfIncrementalPushReceived = true;
      report.message = incrementalPushVersion;
      report.topicName = kafkaTopic;
      report.partitionId = partitionId;
      report.offset = offset;
      ingestionService.reportIngestionStatus(report);
    }

    @Override
    public void endOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset, String incrementalPushVersion) {
      IngestionTaskReport report = new IngestionTaskReport();
      report.isEndOfIncrementalPushReceived = true;
      report.message = incrementalPushVersion;
      report.topicName = kafkaTopic;
      report.partitionId = partitionId;
      report.offset = offset;
      ingestionService.reportIngestionStatus(report);
    }

    @Override
    public void topicSwitchReceived(String kafkaTopic, int partitionId, long offset, String message) {
      IngestionTaskReport report = new IngestionTaskReport();
      report.isTopicSwitchReceived = true;
      report.message = message;
      report.topicName = kafkaTopic;
      report.partitionId = partitionId;
      report.offset = offset;
      ingestionService.reportIngestionStatus(report);
    }

    @Override
    public void progress(String kafkaTopic, int partitionId, long offset, String message) {
      IngestionTaskReport report = new IngestionTaskReport();
      report.isProgress = true;
      report.message = message;
      report.topicName = kafkaTopic;
      report.partitionId = partitionId;
      report.offset = offset;
      ingestionService.reportIngestionStatus(report);
    }
  };
}
