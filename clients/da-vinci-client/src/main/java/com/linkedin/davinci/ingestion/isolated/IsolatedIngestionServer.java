package com.linkedin.davinci.ingestion.isolated;

import static com.linkedin.venice.ConfigKeys.CLUSTER_DISCOVERY_D2_SERVICE;
import static com.linkedin.venice.ConfigKeys.D2_ZK_HOSTS_ADDRESS;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_CONNECTION_TIMEOUT_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_STATS_CLASS_LIST;
import static com.linkedin.venice.ConfigKeys.SERVER_REMOTE_INGESTION_REPAIR_SLEEP_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_STOP_CONSUMPTION_TIMEOUT_IN_SECONDS;
import static java.lang.Thread.currentThread;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.DefaultIngestionBackend;
import com.linkedin.davinci.ingestion.IsolatedIngestionBackend;
import com.linkedin.davinci.ingestion.main.MainIngestionMonitorService;
import com.linkedin.davinci.ingestion.main.MainIngestionRequestClient;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.RemoteIngestionRepairService;
import com.linkedin.davinci.repository.VeniceMetadataRepositoryBuilder;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.storage.StorageEngineMetadataService;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.cleaner.LeakedResourceCleaner;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.d2.D2ClientFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.ingestion.protocol.IngestionMetricsReport;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.enums.IngestionReportType;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.ReadOnlyLiveClusterConfigRepository;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.tehuti.metrics.MetricsRepository;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is the server service of the isolated ingestion. It is a Netty based server that listens to
 * all the requests sent from {@link IsolatedIngestionBackend} in main process and spawns {@link IsolatedIngestionServerHandler}
 * to handle the request.
 *
 * The general workflow goes as follows:
 * (1) When server instance is created in child process, it will remain idle until it receives initialization
 * request from main process, which pass in all the configs needed to initialize all ingestion components.
 * (2) Once initialization completes, it starts listening to ingestion command sent from main process.
 * (3) When ingestion notifier in child process is notified, it will use its {@link MainIngestionRequestClient} to relay
 * status back to {@link MainIngestionMonitorService} in main process. {@link MainIngestionMonitorService} will further
 * dispatch status reporting to all registered notifiers in main process.
 *  -- For COMPLETED status, it will stop ingestion and shutdown corresponding storage so main process can re-subscribe it for serving purpose.
 *  -- For ERROR status, it will also stop ingestion and shutdown storage, and it will also forward the ERROR status for main process to handle.
 * The server will not persist any ingestion status to the disk. When the child process crashes, {@link MainIngestionMonitorService}
 * will be responsible for respawning a new instance and resuming all ongoing ingestion tasks for fault tolerance purpose.
 */
public class IsolatedIngestionServer extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(IsolatedIngestionServer.class);

  private final RedundantExceptionFilter redundantExceptionFilter =
      new RedundantExceptionFilter(RedundantExceptionFilter.DEFAULT_BITSET_SIZE, TimeUnit.MINUTES.toMillis(10));
  private final ServerBootstrap bootstrap;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final ExecutorService ingestionExecutor = Executors.newFixedThreadPool(10);
  private final ScheduledExecutorService heartbeatCheckScheduler = Executors.newScheduledThreadPool(1);
  private final ScheduledExecutorService metricsCollectionScheduler = Executors.newScheduledThreadPool(1);
  private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
  private final int servicePort;
  private final ExecutorService longRunningTaskExecutor = Executors.newFixedThreadPool(10);
  private final ExecutorService statusReportingExecutor = Executors.newSingleThreadExecutor();
  /**
   * This map data structure keeps track of a specific topic-partition (resource) is being ingested in the isolated process.
   * (1) If the topic partition value does not exist, this means this resource is not being maintained in the host.
   * (2) If the topic partition value equals to true, this means the resource is being ingested in the isolated process.
   * (3) If the topic partition value equals to false, this means the resource has completed ingestion in the process and
   * is either maintained in the process or in the process of being reported back. All ingestion commands regarding this
   * resource will be rejected and retried in the main process.
   */
  private final Map<String, Map<Integer, AtomicBoolean>> topicPartitionSubscriptionMap =
      new VeniceConcurrentHashMap<>();
  private final Map<String, Double> metricsMap = new VeniceConcurrentHashMap<>();
  /**
   * Heartbeat timeout acknowledge disconnection between main and forked processes. After this timeout, forked process
   * should stop running gracefully.
   */
  private final long connectionTimeoutMs;

  private ChannelFuture serverFuture;
  private MetricsRepository metricsRepository = null;
  private final VeniceConfigLoader configLoader;
  private ReadOnlyStoreRepository storeRepository = null;
  private ReadOnlyLiveClusterConfigRepository liveConfigRepository = null;
  private StorageService storageService = null;
  private KafkaStoreIngestionService storeIngestionService = null;
  private StorageMetadataService storageMetadataService = null;
  // PartitionState and StoreVersionState serializers are lazily constructed after receiving the init configs
  private InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;
  private InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer;
  private boolean isInitiated = false;
  private IsolatedIngestionRequestClient reportClient;
  private IsolatedIngestionRequestClient metricClient;
  private volatile long heartbeatTimeInMs = System.currentTimeMillis();
  private int stopConsumptionTimeoutInSeconds;
  private DefaultIngestionBackend ingestionBackend;
  private final RemoteIngestionRepairService repairService;

  private LeakedResourceCleaner leakedResourceCleaner;

  private final VeniceServerConfig serverConfig;

  public IsolatedIngestionServer(String configPath) throws FileNotFoundException {
    VeniceProperties loadedVeniceProperties = IsolatedIngestionUtils.loadVenicePropertiesFromFile(configPath);
    String configBasePath =
        (new VeniceConfigLoader(loadedVeniceProperties, loadedVeniceProperties)).getVeniceServerConfig()
            .getDataBasePath();
    Map<String, Map<String, String>> kafkaClusterMap =
        IsolatedIngestionUtils.loadForkedIngestionKafkaClusterMapConfig(configBasePath);
    this.configLoader = new VeniceConfigLoader(loadedVeniceProperties, loadedVeniceProperties, kafkaClusterMap);
    this.serverConfig = configLoader.getVeniceServerConfig();

    this.servicePort = serverConfig.getIngestionServicePort();
    this.connectionTimeoutMs =
        configLoader.getCombinedProperties().getLong(SERVER_INGESTION_ISOLATION_CONNECTION_TIMEOUT_SECONDS, 180)
            * Time.MS_PER_SECOND;
    // Initialize Netty server.
    Class<? extends ServerChannel> serverSocketChannelClass = NioServerSocketChannel.class;
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup();
    bootstrap = new ServerBootstrap();
    repairService = new RemoteIngestionRepairService(
        configLoader.getCombinedProperties()
            .getInt(
                SERVER_REMOTE_INGESTION_REPAIR_SLEEP_INTERVAL_SECONDS,
                RemoteIngestionRepairService.DEFAULT_REPAIR_THREAD_SLEEP_INTERVAL_SECONDS));
    bootstrap.group(bossGroup, workerGroup)
        .channel(serverSocketChannelClass)
        .childHandler(new IsolatedIngestionServerChannelInitializer(this))
        .option(ChannelOption.SO_BACKLOG, 1000)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.TCP_NODELAY, true);
  }

  @Override
  public boolean startInner() {
    int maxAttempt = 100;
    long waitTime = 500;
    int retryCount = 0;
    while (true) {
      try {
        serverFuture = bootstrap.bind(servicePort).sync();
        break;
      } catch (Exception e) {
        retryCount += 1;
        if (retryCount > maxAttempt) {
          throw new VeniceException(
              "Ingestion Service is unable to bind to target port " + servicePort + " after " + maxAttempt
                  + " retries.");
        }
        Utils.sleep(waitTime);
      }
    }
    LOGGER.info("Listener service started on port: {}", servicePort);

    initializeIsolatedIngestionServer();
    LOGGER.info("All ingestion components are initialized.");

    heartbeatCheckScheduler.scheduleAtFixedRate(this::checkHeartbeatTimeout, 0, 5, TimeUnit.SECONDS);
    metricsCollectionScheduler.scheduleAtFixedRate(this::reportMetricsUpdateToMainProcess, 0, 1, TimeUnit.MINUTES);
    // There is no async process in this function, so we are completely finished with the start-up process.
    repairService.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    ChannelFuture shutdown = serverFuture.channel().closeFuture();
    workerGroup.shutdownGracefully();
    bossGroup.shutdownGracefully();
    shutdown.sync();

    // Shutdown the internal clean up executor of redundant exception filter.
    redundantExceptionFilter.shutdown();

    try {
      if (storeIngestionService != null) {
        storeIngestionService.stop();
      }
      LOGGER.info("StoreIngestionService has been shutdown.");
      if (storageService != null) {
        storageService.stop();
      }
      LOGGER.info("StorageService has been shutdown.");
    } catch (Throwable e) {
      throw new VeniceException("Unable to stop Ingestion Service", e);
    }

    if (leakedResourceCleaner != null) {
      leakedResourceCleaner.stop();
    }

    heartbeatCheckScheduler.shutdownNow();
    metricsCollectionScheduler.shutdownNow();
    ingestionExecutor.shutdown();
    longRunningTaskExecutor.shutdown();
    try {
      if (!longRunningTaskExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        longRunningTaskExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }

    statusReportingExecutor.shutdown();
    try {
      if (!statusReportingExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        statusReportingExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }
    repairService.stop();
  }

  public boolean isInitiated() {
    return isInitiated;
  }

  public StorageService getStorageService() {
    return storageService;
  }

  public VeniceConfigLoader getConfigLoader() {
    return configLoader;
  }

  public KafkaStoreIngestionService getStoreIngestionService() {
    return storeIngestionService;
  }

  public StorageMetadataService getStorageMetadataService() {
    return storageMetadataService;
  }

  public ReadOnlyStoreRepository getStoreRepository() {
    return storeRepository;
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public DefaultIngestionBackend getIngestionBackend() {
    return ingestionBackend;
  }

  public InternalAvroSpecificSerializer<PartitionState> getPartitionStateSerializer() {
    return partitionStateSerializer;
  }

  public int getStopConsumptionTimeoutInSeconds() {
    return stopConsumptionTimeoutInSeconds;
  }

  public Map<String, Double> getMetricsMap() {
    return metricsMap;
  }

  public void updateHeartbeatTime() {
    this.heartbeatTimeInMs = System.currentTimeMillis();
    LOGGER.info("Received heartbeat from main process at: {}", this.heartbeatTimeInMs);
  }

  public void cleanupTopicPartitionState(String topicName, int partitionId) {
    Map<Integer, AtomicBoolean> partitionSubscriptionMap = topicPartitionSubscriptionMap.get(topicName);
    if (partitionSubscriptionMap != null) {
      partitionSubscriptionMap.remove(partitionId);
    }
  }

  public void cleanupTopicState(String topicName) {
    topicPartitionSubscriptionMap.remove(topicName);
  }

  /**
   * Use executor to execute status reporting in async fashion, otherwise it may cause deadlock between main process
   * and child process.
   * One previous example of the deadlock situation could happen when VersionBackend is trying to unsubscribe a topic partition,
   * it will hold VersionBackend instance lock, and send a blocking call to isolated ingestion service to call
   * {@link KafkaStoreIngestionService#stopConsumptionAndWait(VeniceStoreVersionConfig, int, int, int, boolean)}, inside which it will
   * wait up to 30 seconds to drain internal messages for the partition. For SOP message, it will call
   * {@link com.linkedin.davinci.notifier.VeniceNotifier#started(String, int)} and in ingestion isolation case it will
   * send a blocking call to main process to report progress. The logic inside Da Vinci Client ingestion notifier's
   * started() will call tryStartHeartbeat() in VersionBackend which will also need the VersionBackend instance lock.
   * Thus all of them get stuck until timeout, which leads to unexpected behavior of draining to closed RocksDB storage.
   * This status reporting executor is designed to be single thread to respect the reporting order inside child process.
   * For time-consuming action like stopConsumptionAndWait, we introduced an extra multi-thread executors to improve the
   * performance.
   */
  public void reportIngestionStatus(IngestionTaskReport report) {
    IngestionReportType ingestionReportType = IngestionReportType.valueOf(report.reportType);
    if (ingestionReportType.equals(IngestionReportType.COMPLETED)
        || ingestionReportType.equals(IngestionReportType.ERROR)) {
      String topicName = report.topicName.toString();
      int partitionId = report.partitionId;

      // Collect the latest OffsetRecord ByteBuffer array and store version state before consumption stops.
      if (ingestionReportType.equals(IngestionReportType.COMPLETED)) {
        // Force sync topic partition offsets before shutting down RocksDB and reopen in main process.
        getStoreIngestionService().syncTopicPartitionOffset(topicName, partitionId);
        // Set offset record in ingestion report.
        report.offsetRecord = getStoreIngestionService().getPartitionOffsetRecords(topicName, partitionId);

        // Set store version state in ingestion report.
        StoreVersionState storeVersionState = storageMetadataService.getStoreVersionState(topicName);
        if (storeVersionState != null) {
          report.storeVersionState =
              ByteBuffer.wrap(IsolatedIngestionUtils.serializeStoreVersionState(topicName, storeVersionState));
        } else {
          throw new VeniceException("StoreVersionState does not exist for topic: " + topicName);
        }
        LOGGER.info(
            "Ingestion completed for replica: {}, offset: {}",
            Utils.getReplicaId(topicName, partitionId),
            report.offset);
      } else {
        LOGGER.error(
            "Ingestion error for replica: {}, error message: {}",
            Utils.getReplicaId(topicName, partitionId),
            report.message);
      }

      stopConsumptionAndReport(report);
    } else {
      statusReportingExecutor.execute(() -> reportClient.reportIngestionStatus(report));
    }
  }

  public RedundantExceptionFilter getRedundantExceptionFilter() {
    return redundantExceptionFilter;
  }

  // Set the topic partition state to be unsubscribed.
  public void setResourceToBeUnsubscribed(String topicName, int partition) {
    getTopicPartitionSubscriptionMap().computeIfAbsent(topicName, s -> new VeniceConcurrentHashMap<>())
        .computeIfAbsent(partition, p -> new AtomicBoolean(false))
        .set(false);
  }

  // Set the topic partition state to be subscribed.
  public void setResourceToBeSubscribed(String topicName, int partition) {
    getTopicPartitionSubscriptionMap().computeIfAbsent(topicName, s -> new VeniceConcurrentHashMap<>())
        .computeIfAbsent(partition, p -> new AtomicBoolean(true))
        .set(true);
  }

  // Check if topic partition is being subscribed.
  public boolean isResourceSubscribed(String topicName, int partition) {
    AtomicBoolean subscription =
        getTopicPartitionSubscriptionMap().getOrDefault(topicName, Collections.emptyMap()).get(partition);
    if (subscription == null) {
      return false;
    }
    return subscription.get();
  }

  public void maybeInitializeResourceHostingMetadata(String topicName, int partition) {
    getTopicPartitionSubscriptionMap().computeIfAbsent(topicName, s -> new VeniceConcurrentHashMap<>())
        .computeIfAbsent(partition, p -> new AtomicBoolean(true));
  }

  Map<String, Map<Integer, AtomicBoolean>> getTopicPartitionSubscriptionMap() {
    return topicPartitionSubscriptionMap;
  }

  ExecutorService getStatusReportingExecutor() {
    return statusReportingExecutor;
  }

  IsolatedIngestionRequestClient getReportClient() {
    return reportClient;
  }

  void stopConsumptionAndReport(IngestionTaskReport report) {
    String topicName = report.topicName.toString();
    int partitionId = report.partitionId;
    // Unsubscribe resource here so all incoming requests should be rejected and retried until handover is completed.
    setResourceToBeUnsubscribed(topicName, partitionId);

    CompletableFuture<Boolean> asyncShutdownResourceTaskFuture =
        submitStopConsumptionAndCloseStorageTask(topicName, partitionId);
    getStatusReportingExecutor().execute(() -> {
      try {
        if (!asyncShutdownResourceTaskFuture.get()) {
          return;
        }
      } catch (ExecutionException | InterruptedException e) {
        LOGGER.warn(
            "Encounter exception when waiting future execution of stop consumption and close storage for {} of topic: {}",
            partitionId,
            topicName);
      }
      /**
       * Only when we actively stop partition consumption do we report the ingestion status here. Otherwise, we will not
       * perform ingestion handover.
       */
      if (!getReportClient().reportIngestionStatus(report)) {
        LOGGER.warn("Failed to deliver ingestion report to main process");
      }
    });

  }

  /**
   * Handle the logic of COMPLETED/ERROR here since we need to stop related ingestion task and close RocksDB partition.
   * Since the logic takes time to wait for completion, we need to execute it in async fashion to prevent blocking other operations.
   */
  CompletableFuture<Boolean> submitStopConsumptionAndCloseStorageTask(String topicName, int partitionId) {
    return CompletableFuture.supplyAsync(() -> {
      /**
       * Wait for all pending ingestion action on this partition to be completed in async fashion, which is expected to
       * be fast. This check must be executed in async fashion as startConsumption may report COMPLETED directly and the
       * pending action counter will not be 0 if it is called in the same thread.
       */
      getStoreIngestionService()
          .waitIngestionTaskToCompleteAllPartitionPendingActions(topicName, partitionId, 100, 300);
      /**
       * For Da Vinci Live Update suppression, it is actively stopping ingestion so isPartitionConsuming() is not working.
       * Since Da Vinci does not have issue of receiving stopConsumption and handover at the same time, original race
       * condition won't be an issue for DVC.
       */
      boolean shouldHandoverResource = getStoreIngestionService().isPartitionConsuming(topicName, partitionId)
          || getStoreIngestionService().isLiveUpdateSuppressionEnabled();
      if (shouldHandoverResource) {
        VeniceStoreVersionConfig storeConfig = getConfigLoader().getStoreConfig(topicName);
        // Make sure partition is not consuming, so we can safely close the rocksdb partition
        long startTimeInMs = System.currentTimeMillis();
        final int stopConsumptionWaitIntervalInSeconds = 1;
        getStoreIngestionService().stopConsumptionAndWait(
            storeConfig,
            partitionId,
            stopConsumptionWaitIntervalInSeconds,
            stopConsumptionTimeoutInSeconds / stopConsumptionWaitIntervalInSeconds,
            false);
        // Close all RocksDB sub-Partitions in Ingestion Service.
        getStorageService().closeStorePartition(storeConfig, partitionId);
        LOGGER.info(
            "Partition: {} of topic: {} closed in {} ms.",
            partitionId,
            topicName,
            LatencyUtils.getElapsedTimeFromMsToMs(startTimeInMs));
        return true;
      } else {
        // If pending ingestion action stops consumption (unsubscribe), we will not handover ingestion and we should
        // restore the subscribed state.
        LOGGER.warn("Topic: {}, partition: {} is not consuming, will not handover ingestion.", topicName, partitionId);
        setResourceToBeSubscribed(topicName, partitionId);
        return false;
      }
    }, getLongRunningTaskExecutor());
  }

  ExecutorService getLongRunningTaskExecutor() {
    return longRunningTaskExecutor;
  }

  private void checkHeartbeatTimeout() {
    if (!isShuttingDown.get()) {
      long currentTimeMillis = System.currentTimeMillis();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Checking heartbeat timeout at {}, latest heartbeat on server: {}",
            currentTimeMillis,
            heartbeatTimeInMs);
      }

      if ((currentTimeMillis - heartbeatTimeInMs) > connectionTimeoutMs) {
        LOGGER.warn(
            "Lost connection to parent process after {} ms, will shutdown the ingestion backend gracefully.",
            connectionTimeoutMs);
        isShuttingDown.set(true);
        try {
          stop();
          // Force closing the JVM process as we don't want any lingering process. It is safe to exit the JVM now as all
          // necessary resources are shutdown.
          System.exit(0);
        } catch (Exception e) {
          LOGGER.info("Unable to shutdown ingestion service gracefully", e);
        }
      }
    }
  }

  void reportMetricsUpdateToMainProcess() {
    try {
      IngestionMetricsReport metricsReport = new IngestionMetricsReport();
      metricsReport.aggregatedMetrics = new VeniceConcurrentHashMap<>();
      /**
       * TODO: This approach may lead to metric loss if we fail to deliver a report of metric updates due to timeout.
       * But since server will continue to handle update even if client times out, it is considered ok for now. We should
       * try to see if we can safeguard the metric delta delivery mechanism.
       */
      if (getMetricsRepository() != null) {
        getMetricsRepository().metrics().forEach((name, metric) -> {
          if (metric != null) {
            try {
              // Best-effort to reduce metrics delta size sent from child process to main process.
              Double originalValue = getMetricsMap().get(name);
              Double newValue = metric.value();
              if (originalValue == null || !originalValue.equals(newValue)) {
                metricsReport.aggregatedMetrics.put(name, newValue);
              }
              getMetricsMap().put(name, newValue);
            } catch (Exception e) {
              String exceptionLogMessage = "Encounter exception when retrieving value of metric: " + name;
              if (!getRedundantExceptionFilter().isRedundantException(exceptionLogMessage)) {
                LOGGER.error(exceptionLogMessage, e);
              }
            }
          }
        });
      }
      getMetricClient().reportMetricUpdate(metricsReport);
    } catch (Exception e) {
      LOGGER.warn("Encounter exception when fetching latest metrics and reporting back to main process", e);
    }
  }

  IsolatedIngestionRequestClient getMetricClient() {
    return metricClient;
  }

  private void initializeIsolatedIngestionServer() {
    stopConsumptionTimeoutInSeconds =
        configLoader.getCombinedProperties().getInt(SERVER_STOP_CONSUMPTION_TIMEOUT_IN_SECONDS, 180);

    // Initialize D2Client.
    String d2ZkHosts = configLoader.getCombinedProperties().getString(D2_ZK_HOSTS_ADDRESS);
    Optional<SSLFactory> sslFactory = IsolatedIngestionUtils.getSSLFactoryForIngestion(configLoader);
    D2Client d2Client = D2ClientFactory.getD2Client(d2ZkHosts, sslFactory);

    final String clusterDiscoveryD2ServiceName = configLoader.getCombinedProperties()
        .getString(CLUSTER_DISCOVERY_D2_SERVICE, ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME);

    // Create the client config.
    ClientConfig clientConfig =
        new ClientConfig().setD2Client(d2Client).setD2ServiceName(clusterDiscoveryD2ServiceName);

    // Create MetricsRepository
    metricsRepository = MetricsRepositoryUtils.createMultiThreadedMetricsRepository();

    // Initialize store/schema repositories.
    VeniceMetadataRepositoryBuilder veniceMetadataRepositoryBuilder =
        new VeniceMetadataRepositoryBuilder(configLoader, clientConfig, metricsRepository, null, true);
    storeRepository = veniceMetadataRepositoryBuilder.getStoreRepo();
    liveConfigRepository = veniceMetadataRepositoryBuilder.getLiveClusterConfigRepo();
    ReadOnlySchemaRepository schemaRepository = veniceMetadataRepositoryBuilder.getSchemaRepo();
    Optional<HelixReadOnlyZKSharedSchemaRepository> helixReadOnlyZKSharedSchemaRepository =
        veniceMetadataRepositoryBuilder.getReadOnlyZKSharedSchemaRepository();
    ClusterInfoProvider clusterInfoProvider = veniceMetadataRepositoryBuilder.getClusterInfoProvider();

    SchemaReader partitionStateSchemaReader = ClientFactory.getSchemaReader(
        ClientConfig.cloneConfig(clientConfig)
            .setStoreName(AvroProtocolDefinition.PARTITION_STATE.getSystemStoreName()),
        null);
    SchemaReader storeVersionStateSchemaReader = ClientFactory.getSchemaReader(
        ClientConfig.cloneConfig(clientConfig)
            .setStoreName(AvroProtocolDefinition.STORE_VERSION_STATE.getSystemStoreName()),
        null);
    partitionStateSerializer = AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    partitionStateSerializer.setSchemaReader(partitionStateSchemaReader);
    storeVersionStateSerializer = AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
    storeVersionStateSerializer.setSchemaReader(storeVersionStateSchemaReader);

    // Create RocksDBMemoryStats.
    boolean plainTableEnabled = serverConfig.getRocksDBServerConfig().isRocksDBPlainTableFormatEnabled();
    RocksDBMemoryStats rocksDBMemoryStats = serverConfig.isDatabaseMemoryStatsEnabled()
        ? new RocksDBMemoryStats(metricsRepository, "RocksDBMemoryStats", plainTableEnabled)
        : null;

    /**
     * Using reflection to create all the stats classes related to ingestion isolation. All these classes extends
     * {@link AbstractVeniceStats} class and takes {@link MetricsRepository} as the only parameter in its constructor.
     */
    for (String ingestionIsolationStatsClassName: configLoader.getCombinedProperties()
        .getString(SERVER_INGESTION_ISOLATION_STATS_CLASS_LIST, "")
        .split(",")) {
      if (ingestionIsolationStatsClassName.length() != 0) {
        Class<? extends AbstractVeniceStats> ingestionIsolationStatsClass =
            ReflectUtils.loadClass(ingestionIsolationStatsClassName);
        if (!AbstractVeniceStats.class.isAssignableFrom(ingestionIsolationStatsClass)) {
          throw new VeniceException(
              "Class: " + ingestionIsolationStatsClassName + " does not extends AbstractVeniceStats");
        }
        AbstractVeniceStats ingestionIsolationStats = ReflectUtils.callConstructor(
            ingestionIsolationStatsClass,
            new Class<?>[] { MetricsRepository.class },
            new Object[] { metricsRepository });
        LOGGER.info("Created Ingestion Isolation stats: {}", ingestionIsolationStats.getName());
      } else {
        LOGGER.info("Ingestion isolation stats class name is empty, will skip it.");
      }
    }

    // Create StorageService
    AggVersionedStorageEngineStats storageEngineStats = new AggVersionedStorageEngineStats(
        metricsRepository,
        storeRepository,
        serverConfig.isUnregisterMetricForDeletedStoreEnabled());
    boolean isDaVinciClient = veniceMetadataRepositoryBuilder.isDaVinciClient();

    /**
     * For isolated ingestion server, we always restore metadata partition, but we will only restore data partition in server,
     * not Da Vinci.
     * The reason of not to restore the data partitions during initialization of storage service in DVC is:
     * 1. During fresh start up with data on disk (aka bootstrap), we will receive messages to subscribe to the partition
     * and it will re-open the partition on demand.
     * 2. During crash recovery restart, partitions that are already ingestion will be opened by parent process and we
     * should not try to open it. The remaining ingestion tasks will open the storage engines.
     *
     * The reason to restore data partitions during initialization of storage service in server is:
     * 1. Helix can issue OFFLINE->DROPPED state transition for stale partition in anytime, including server restart time.
     * If data partition is not restored here (as well as the main process), it will never be dropped so the RocksDB storage
     * will be leaking.
     */
    storageService = new StorageService(
        configLoader,
        storageEngineStats,
        rocksDBMemoryStats,
        storeVersionStateSerializer,
        partitionStateSerializer,
        storeRepository,
        !isDaVinciClient,
        true);
    storageService.start();

    // Create SchemaReader
    SchemaReader kafkaMessageEnvelopeSchemaReader = ClientFactory.getSchemaReader(
        ClientConfig.cloneConfig(clientConfig)
            .setStoreName(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName()),
        null);

    storageMetadataService =
        new StorageEngineMetadataService(storageService.getStorageEngineRepository(), partitionStateSerializer);

    StorageEngineBackedCompressorFactory compressorFactory =
        new StorageEngineBackedCompressorFactory(storageMetadataService);

    PubSubClientsFactory pubSubClientsFactory = serverConfig.getPubSubClientsFactory();

    // Create KafkaStoreIngestionService
    storeIngestionService = new KafkaStoreIngestionService(
        storageService,
        configLoader,
        storageMetadataService,
        clusterInfoProvider,
        storeRepository,
        schemaRepository,
        liveConfigRepository,
        metricsRepository,
        Optional.of(kafkaMessageEnvelopeSchemaReader),
        isDaVinciClient ? Optional.empty() : Optional.of(clientConfig),
        partitionStateSerializer,
        helixReadOnlyZKSharedSchemaRepository,
        null,
        true,
        compressorFactory,
        Optional.empty(),
        null,
        isDaVinciClient,
        repairService,
        pubSubClientsFactory,
        sslFactory,
        null,
        null);
    storeIngestionService.start();
    storeIngestionService.addIngestionNotifier(new IsolatedIngestionNotifier(this));

    ingestionBackend = new DefaultIngestionBackend(
        storageMetadataService,
        storeIngestionService,
        storageService,
        null,
        configLoader.getVeniceServerConfig());

    if (serverConfig.isLeakedResourceCleanupEnabled()) {
      this.leakedResourceCleaner = new LeakedResourceCleaner(
          storageService.getStorageEngineRepository(),
          serverConfig.getLeakedResourceCleanUpIntervalInMS(),
          storeRepository,
          storeIngestionService,
          storageService,
          metricsRepository);
      leakedResourceCleaner.start();
    }
    LOGGER.info("Starting report client with target application port: {}", serverConfig.getIngestionApplicationPort());
    // Create Netty client to report ingestion status back to main process.
    reportClient = new IsolatedIngestionRequestClient(configLoader);
    // Create Netty client to report metrics update back to main process.
    metricClient = new IsolatedIngestionRequestClient(configLoader);
    // Mark the isolated ingestion service as initiated.
    isInitiated = true;
  }

  public static void main(String[] args) throws Exception {
    LOGGER.info("Capture arguments: {}", Arrays.toString(args));
    if (args.length != 1) {
      throw new VeniceException("Expected exactly one argument for config file path. Got " + args.length);
    }
    IsolatedIngestionServer isolatedIngestionServer = new IsolatedIngestionServer(args[0]);
    isolatedIngestionServer.start();
  }
}
