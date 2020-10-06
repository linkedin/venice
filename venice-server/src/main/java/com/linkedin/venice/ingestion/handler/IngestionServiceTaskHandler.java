package com.linkedin.venice.ingestion.handler;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.SubscriptionBasedStoreRepository;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.ingestion.IngestionRequestClient;
import com.linkedin.venice.ingestion.IngestionService;
import com.linkedin.venice.ingestion.protocol.IngestionTaskCommand;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.InitializationConfigs;
import com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType;
import com.linkedin.venice.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.venice.meta.IngestionAction;
import com.linkedin.venice.meta.IngestionIsolationMode;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.stats.AggVersionedStorageEngineStats;
import com.linkedin.venice.stats.RocksDBMemoryStats;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.storage.StorageEngineMetadataService;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.tehuti.metrics.MetricsRepository;
import java.net.URI;
import java.util.Optional;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.log4j.Logger;

import static com.linkedin.venice.client.store.ClientFactory.*;
import static com.linkedin.venice.ingestion.IngestionUtils.*;


public class IngestionServiceTaskHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger logger = Logger.getLogger(IngestionServiceTaskHandler.class);
  private final IngestionService ingestionService;

  public IngestionServiceTaskHandler(IngestionService ingestionService) {
    super();
    this.ingestionService = ingestionService;
    logger.info("IngestionServiceTaskHandler created for listener service.");
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
          logger.info("Received INIT message: " + msg.toString());
          InitializationConfigs initializationConfigs = parseIngestionInitialization(msg);
          handleIngestionInitialization(initializationConfigs);
          ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.OK, "OK!"));
          break;
        case COMMAND:
          logger.info("Received COMMAND message " + msg.toString());
          IngestionTaskCommand ingestionTaskCommand = parseIngestionTaskCommand(msg);
          IngestionTaskReport report = handleIngestionTaskCommand(ingestionTaskCommand);
          byte[] serializedReport = serializeIngestionTaskReport(report);
          ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.OK, serializedReport));
          break;
        default:
          throw new UnsupportedOperationException("Unrecognized ingestion action: " + action);
      }
    } catch (UnsupportedOperationException e) {
      // Here we only handles the bad requests exception. Other errors are handled in exceptionCaught() method.
      logger.error("Caught unrecognized request action:" + e.getMessage());
      ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.BAD_REQUEST, e.getMessage()));
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    logger.error("Encounter exception " + cause.getMessage());
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
    ingestionService.setConfigLoader(configLoader);

    D2Client d2Client = new D2ClientBuilder().setZkHosts(configLoader.getVeniceClusterConfig().getZookeeperAddress()).build();
    startD2Client(d2Client);

    // Create the client config.
    ClientConfig clientConfig = new ClientConfig();
    clientConfig.setD2Client(d2Client).setD2ServiceName("venice-discovery");
    String clusterName = configLoader.getVeniceClusterConfig().getClusterName();

    // Create MetricsRepository
    MetricsRepository metricsRepository = new MetricsRepository();

    // Create ZkClient
    HelixAdapterSerializer adapter = new HelixAdapterSerializer();
    ZkClient zkClient = ZkClientFactory.newZkClient(configLoader.getVeniceClusterConfig().getZookeeperAddress());
    zkClient.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, ".ingestion-service-zk-client"));

    /**
     * Create StoreRepository
     * TODO: Currently we are using the ZK-based store repository. It will be switched to MetadataBasedStoreRepository to make it ZK-free.
     */
    SubscriptionBasedReadOnlyStoreRepository storeRepository = new SubscriptionBasedStoreRepository(zkClient, adapter, clusterName);
    storeRepository.refresh();
    ingestionService.setStoreRepository(storeRepository);

    // Create SchemaRepository
    ReadOnlySchemaRepository schemaRepository = new HelixReadOnlySchemaRepository(storeRepository, zkClient, adapter, clusterName, 3, 1000);
    schemaRepository.refresh();

    // Create RocksDBMemoryStats
    RocksDBMemoryStats rocksDBMemoryStats = configLoader.getVeniceServerConfig().isDatabaseMemoryStatsEnabled() ?
        new RocksDBMemoryStats(metricsRepository, "RocksDBMemoryStats", configLoader.getVeniceServerConfig().getRocksDBServerConfig().isRocksDBStatisticsEnabled()) : null;

    // Create StorageService
    AggVersionedStorageEngineStats storageEngineStats = new AggVersionedStorageEngineStats(metricsRepository, storeRepository);
    StorageService storageService = new StorageService(configLoader, storageEngineStats, rocksDBMemoryStats);
    storageService.start();
    ingestionService.setStorageService(storageService);

    // Create SchemaReader
    SchemaReader schemaReader = getSchemaReader(
        ClientConfig.cloneConfig(clientConfig).setStoreName(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName())
    );

    // Create KafkaStoreIngestionService
    KafkaStoreIngestionService storeIngestionService = new KafkaStoreIngestionService(
        storageService.getStorageEngineRepository(),
        configLoader,
        new StorageEngineMetadataService(storageService.getStorageEngineRepository()),
        storeRepository,
        schemaRepository,
        metricsRepository,
        rocksDBMemoryStats,
        Optional.of(schemaReader),
        Optional.of(clientConfig));
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
    report.errorMessage = "";
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
          // TODO: Decide whether to move this check to initialization phase.
          IngestionIsolationMode isolationMode = ingestionService.getConfigLoader().getVeniceServerConfig().getIngestionIsolationMode();
          if (isolationMode.equals(IngestionIsolationMode.NO_OP)) {
            throw new VeniceException("Ingestion Isolation Mode is set as NO_OP(not enabled).");
          }

          // Subscribe to the store in store repository.
          String storeName = Version.parseStoreFromKafkaTopicName(topicName);
          // Ingestion Service needs store repository to subscribe to the store.
          ingestionService.getStoreRepository().subscribe(storeName);
          logger.info("Start ingesting partition: " + partitionId + " of topic: " + topicName);
          ingestionService.addIngestionPartition(topicName, partitionId);
          storageService.openStoreForNewPartition(storeConfig, partitionId);
          storeIngestionService.startConsumption(storeConfig, partitionId, false);
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
        default:
          break;
      }
    } catch (Exception e) {
      logger.error("Encounter exception while handling ingestion command: " + e.getMessage());
      report.isPositive = false;
      report.errorMessage = e.getMessage();
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

  private InitializationConfigs parseIngestionInitialization(FullHttpRequest httpRequest) {
    return deserializeInitializationConfigs(readHttpRequestContent(httpRequest));
  }

  private IngestionTaskCommand parseIngestionTaskCommand(FullHttpRequest httpRequest) {
    return deserializeIngestionTaskCommand(readHttpRequestContent(httpRequest));
  }

  private final VeniceNotifier ingestionListener = new VeniceNotifier() {
    @Override
    public void completed(String kafkaTopic, int partitionId, long offset) {
      logger.info("Ingestion finished for topic: " + kafkaTopic + " partition id: " + partitionId + " offset: " + offset);
      VeniceStoreConfig storeConfig = ingestionService.getConfigLoader().getStoreConfig(kafkaTopic);
      ingestionService.getStoreIngestionService().stopConsumption(storeConfig, partitionId);
      ingestionService.removeIngestionPartition(kafkaTopic, partitionId, offset);
    }
  };
}
