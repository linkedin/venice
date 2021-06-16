package com.linkedin.davinci.ingestion.isolated;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.helix.LeaderFollowerParticipantModel;
import com.linkedin.davinci.ingestion.DefaultIngestionBackend;
import com.linkedin.davinci.ingestion.IsolatedIngestionBackend;
import com.linkedin.davinci.ingestion.main.MainIngestionMonitorService;
import com.linkedin.davinci.ingestion.main.MainIngestionRequestClient;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.enums.IngestionReportType;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.tehuti.metrics.MetricsRepository;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;

import static com.linkedin.venice.utils.Time.*;
import static java.lang.Thread.*;


/**
 * IsolatedIngestionServer is the server service of the isolated ingestion service. It is a Netty based server that listens to
 * all the requests sent from {@link IsolatedIngestionBackend} in main process and spawn {@link IsolatedIngestionServerHandler}
 * to handle the request.
 *
 * The general workflow goes as follows:
 * (1) When IsolatedIngestionServer instance is created in child process, it will remain idle until it receives initialization
 * request from main process, which pass in all the configs needed to initialize all ingestion components.
 * (2) Once initialization completes, it starts listening to ingestion command sent from main process, such as startConsumption,
 * stopConsumption, killConsumption, updateMetadata and so on.
 * (3) When ingestion notifier in child process is notified, it will use its {@link MainIngestionRequestClient} to relay status
 * back to {@link MainIngestionMonitorService} in main process. {@link MainIngestionMonitorService} will further dispatch status
 * reporting to all registered notifiers in main process.
 *  -- For COMPLETED status, it will stop ingestion and shutdown corresponding storage so main process can re-subscribe it for serving purpose.
 *  -- For ERROR status, it will also stop ingestion and shutdown storage, and it will also forward the ERROR status for main process to handle.
 * IsolatedIngestionServer itself is stateless and will not persist any ingestion status. When the child process encounters failure
 * and crash, {@link MainIngestionMonitorService} will be responsible of respawning a new instance and resume all ongoing ingestion
 * tasks for fault tolerance purpose.
 */
public class IsolatedIngestionServer extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(IsolatedIngestionServer.class);

  private final RedundantExceptionFilter redundantExceptionFilter = new RedundantExceptionFilter(RedundantExceptionFilter.DEFAULT_BITSET_SIZE, TimeUnit.MINUTES.toMillis(10));
  private final ServerBootstrap bootstrap;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final ExecutorService ingestionExecutor = Executors.newFixedThreadPool(10);
  private final ScheduledExecutorService heartbeatCheckScheduler = Executors.newScheduledThreadPool(1);
  private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
  private final int servicePort;
  private final ExecutorService longRunningTaskExecutor = Executors.newFixedThreadPool(10);
  private final ExecutorService statusReportingExecutor = Executors.newSingleThreadExecutor();
  // Leader section Id map helps to verify if the PROMOTE_TO_LEADER/DEMOTE_TO_STANDBY is valid or not when processing the message in the queue.
  private final Map<String, Map<Integer, AtomicLong>> leaderSessionIdMap = new VeniceConcurrentHashMap<>();
  /**
   * The boolean value of this map indicates whether we have added UNSUBSCRIBE message to the processing queue.
   * We should not add leader change message into the queue if we have added UNSUBSCRIBE message to the queue, otherwise
   * it won't get processed and the request may be missed. This will help leader promo/demote request from parent process
   * fail out early and avoid race condition.
   */
  private final Map<String, Map<Integer, AtomicBoolean>> topicPartitionSubscriptionMap = new VeniceConcurrentHashMap<>();
  private final Map<String, Double> metricsMap = new VeniceConcurrentHashMap<>();
  private final long heartbeatTimeoutMs;

  private ChannelFuture serverFuture;
  private MetricsRepository metricsRepository = null;
  private VeniceConfigLoader configLoader = null;
  private ReadOnlyStoreRepository storeRepository = null;
  private StorageService storageService = null;
  private KafkaStoreIngestionService storeIngestionService = null;
  private StorageMetadataService storageMetadataService = null;
  // PartitionState and StoreVersionState serializers are lazily constructed after receiving the init configs
  private InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;
  private InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer;
  private boolean isInitiated = false;
  private IsolatedIngestionRequestClient reportClient;
  private volatile long heartbeatTimeInMs = System.currentTimeMillis();
  private int stopConsumptionWaitRetriesNum;
  private DefaultIngestionBackend ingestionBackend;

  public IsolatedIngestionServer(int servicePort, long heartbeatTimeoutMs) {
    this.servicePort = servicePort;
    this.heartbeatTimeoutMs = heartbeatTimeoutMs;

    // Initialize Netty server.
    Class<? extends ServerChannel> serverSocketChannelClass = NioServerSocketChannel.class;
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup();
    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup).channel(serverSocketChannelClass)
        .childHandler(new IsolatedIngestionServerChannelInitializer(this))
        .option(ChannelOption.SO_BACKLOG, 1000)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.TCP_NODELAY, true);
    logger.info("IsolatedIngestionServer created");
  }

  public IsolatedIngestionServer(int servicePort) {
    this(servicePort, 60 * MS_PER_SECOND);
  }

  @Override
  public boolean startInner() {
    int maxAttempt = 100;
    int retryCount = 0;
    while (true) {
      try {
        serverFuture = bootstrap.bind(servicePort).sync();
        break;
      } catch (Exception e) {
        retryCount += 1;
        if (retryCount > maxAttempt) {
          throw new VeniceException("Ingestion Service is unable to bind to target port " + servicePort  + " after " + maxAttempt + " retries.");
        }
      }
      Utils.sleep(100);
    }
    logger.info("Listener service started on port: " + servicePort);
    heartbeatCheckScheduler.scheduleAtFixedRate(this::checkHeartbeatTimeout, 0, 5, TimeUnit.SECONDS);
    // There is no async process in this function, so we are completely finished with the start up process.
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
      logger.info("StoreIngestionService has been shutdown.");
      if (storageService != null) {
        storageService.stop();
      }
      logger.info("StorageService has been shutdown.");
    } catch (Throwable e) {
      throw new VeniceException("Unable to stop Ingestion Service", e);
    }

    heartbeatCheckScheduler.shutdownNow();
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
  }

  public void setConfigLoader(VeniceConfigLoader configLoader) {
    this.configLoader = configLoader;
  }

  public void setStoreRepository(ReadOnlyStoreRepository storeRepository) {
    this.storeRepository = storeRepository;
  }

  public void setStorageService(StorageService storageService) {
    this.storageService = storageService;
  }

  public void setStoreIngestionService(KafkaStoreIngestionService storeIngestionService) {
    this.storeIngestionService = storeIngestionService;
  }

  public void setStorageMetadataService(StorageMetadataService storageMetadataService) {
    this.storageMetadataService = storageMetadataService;
  }

  public void setReportClient(IsolatedIngestionRequestClient reportClient) {
    this.reportClient = reportClient;
  }

  public void setMetricsRepository(MetricsRepository metricsRepository) {
    this.metricsRepository = metricsRepository;
  }

  public void setPartitionStateSerializer(InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer) {
    this.partitionStateSerializer = partitionStateSerializer;
  }

  public void setStoreVersionStateSerializer(
      InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer) {
    this.storeVersionStateSerializer = storeVersionStateSerializer;
  }

  public void setStopConsumptionWaitRetriesNum(int numRetries) {
    this.stopConsumptionWaitRetriesNum = numRetries;
  }

  public void setIngestionBackend(DefaultIngestionBackend ingestionBackend) {
    this.ingestionBackend = ingestionBackend;
  }

  public IsolatedIngestionRequestClient getReportClient() {
    return reportClient;
  }

  public void setInitiated(boolean initiated) {
    isInitiated = initiated;
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

  public InternalAvroSpecificSerializer<StoreVersionState> getStoreVersionStateSerializer() {
    return storeVersionStateSerializer;
  }

  public int getStopConsumptionWaitRetriesNum() {
    return stopConsumptionWaitRetriesNum;
  }

  public Map<String, Double> getMetricsMap() {
    return metricsMap;
  }

  public void updateHeartbeatTime() {
    this.heartbeatTimeInMs = System.currentTimeMillis();
    logger.info("Received heartbeat from main process at: " + this.heartbeatTimeInMs);
  }

  /**
   * Use executor to execute status reporting in async fashion, otherwise it may cause deadlock between main process
   * and child process.
   * One previous example of the deadlock situation could happen when VersionBackend is trying to unsubscribe a topic partition,
   * it will hold VersionBackend instance lock, and send a blocking call to isolated ingestion service to call
   * {@link KafkaStoreIngestionService#stopConsumptionAndWait(VeniceStoreConfig, int, int, int)}, inside which it will
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
    if (ingestionReportType.equals(IngestionReportType.COMPLETED) || ingestionReportType.equals(IngestionReportType.ERROR)) {
      // Fetch LeaderState from LeaderSubPartition of the user partition.
      LeaderFollowerStateType leaderState = getStoreIngestionService().getLeaderStateFromPartitionConsumptionState(report.topicName.toString(), report.partitionId);
      logger.info("Transferring leader state " + leaderState + " for topic: " + report.topicName + ", partition: " + report.partitionId);

      setPartitionToBeUnsubscribed(report.topicName.toString(), report.partitionId);

      Future<?> executionFuture = submitStopConsumptionAndCloseStorageTask(report);
      statusReportingExecutor.execute(() -> {
        String topicName = report.topicName.toString();
        int partitionId = report.partitionId;
        long offset = report.offset;
        try {
          executionFuture.get();
        } catch (ExecutionException | InterruptedException e) {
          logger.info("Encounter exception when trying to stop consumption and close storage for " + partitionId + " of topic: " + topicName);
        }
        if (ingestionReportType.equals(IngestionReportType.COMPLETED)) {
          logger.info("Ingestion completed for topic: " + topicName + ", partition id: " + partitionId + ", offset: " + offset);
          // Set offset record in ingestion report.
          report.offsetRecordArray = getStoreIngestionService().getPartitionOffsetRecords(topicName, partitionId);

          // Set store version state in ingestion report.
          Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(topicName);
          if (storeVersionState.isPresent()) {
            report.storeVersionState = ByteBuffer.wrap(
                IsolatedIngestionUtils.serializeStoreVersionState(topicName, storeVersionState.get()));
          } else {
            throw new VeniceException("StoreVersionState does not exist for version " + topicName);
          }
          // Report leaderState for user partition.
          report.leaderFollowerState = leaderState.getValue();
        } else {
          logger.error("Ingestion error for topic: " + topicName + ", partition id: " + partitionId + " " + report.message);
        }
        reportClient.reportIngestionStatus(report);
      });
    } else {
      statusReportingExecutor.execute(() ->  reportClient.reportIngestionStatus(report));
    }
  }

  public RedundantExceptionFilter getRedundantExceptionFilter() {
    return redundantExceptionFilter;
  }

  public synchronized LeaderFollowerParticipantModel.LeaderSessionIdChecker getLeaderSectionIdChecker(String topicName, int partitionId) {
    leaderSessionIdMap.putIfAbsent(topicName, new VeniceConcurrentHashMap<>());
    Map<Integer, AtomicLong> partitionIdToLeaderSessionIdMap = leaderSessionIdMap.get(topicName);
    partitionIdToLeaderSessionIdMap.putIfAbsent(partitionId, new AtomicLong(0));
    AtomicLong leaderSessionId = partitionIdToLeaderSessionIdMap.get(partitionId);
    return new LeaderFollowerParticipantModel.LeaderSessionIdChecker(leaderSessionId.incrementAndGet(), leaderSessionId);
  }

  // Set the topic partition state to be unsubscribed(false)
  public void setPartitionToBeUnsubscribed(String topicName, int partition) {
    topicPartitionSubscriptionMap.putIfAbsent(topicName, new VeniceConcurrentHashMap<>());
    topicPartitionSubscriptionMap.get(topicName).putIfAbsent(partition, new AtomicBoolean(false));
    topicPartitionSubscriptionMap.get(topicName).get(partition).set(false);
  }

  // Set the topic partition state to be subscribed(true)
  public void setPartitionToBeSubscribed(String topicName, int partition) {
    topicPartitionSubscriptionMap.putIfAbsent(topicName, new VeniceConcurrentHashMap<>());
    topicPartitionSubscriptionMap.get(topicName).putIfAbsent(partition, new AtomicBoolean(true));
    topicPartitionSubscriptionMap.get(topicName).get(partition).set(true);
  }

  // Check if topic partition is being subscribed.
  public boolean isPartitionSubscribed(String topicName, int partition) {
    if (!topicPartitionSubscriptionMap.containsKey(topicName)) {
      return false;
    }
    Map<Integer, AtomicBoolean> partitionSubscriptionMap = topicPartitionSubscriptionMap.get(topicName);
    if (!partitionSubscriptionMap.containsKey(partition)) {
      return false;
    }
    return partitionSubscriptionMap.get(partition).get();
  }

  /**
   * Handle the logic of COMPLETED/ERROR here since we need to stop related ingestion task and close RocksDB partition.
   * Since the logic takes time to wait for completion, we need to execute it in async fashion to prevent blocking other operations.
   */
  private Future<?> submitStopConsumptionAndCloseStorageTask(IngestionTaskReport report) {
    String topicName = report.topicName.toString();
    int partitionId = report.partitionId;
    return longRunningTaskExecutor.submit(() -> {
      VeniceStoreConfig storeConfig = getConfigLoader().getStoreConfig(topicName);
      // Make sure partition is not consuming so we can safely close the rocksdb partition
      long startTimeInMs = System.currentTimeMillis();
      getStoreIngestionService().stopConsumptionAndWait(storeConfig, report.partitionId, 1, stopConsumptionWaitRetriesNum);
      // Close all RocksDB sub-Partitions in Ingestion Service.
      getStorageService().closeStorePartition(storeConfig, partitionId);
      logger.info("Partition: " + partitionId + " of topic: " + topicName + " closed in " + LatencyUtils.getElapsedTimeInMs(startTimeInMs) + " ms.");
    });
  }

  private void checkHeartbeatTimeout() {
    if (!isShuttingDown.get()) {
      long currentTimeMillis = System.currentTimeMillis();
      if (logger.isDebugEnabled()) {
        logger.debug("Checking heartbeat timeout at " + currentTimeMillis + ", latest heartbeat on server: " + heartbeatTimeInMs);
      }

      if ((currentTimeMillis - heartbeatTimeInMs) > heartbeatTimeoutMs) {
        logger.warn("Lost connection to parent process after " + heartbeatTimeoutMs + "ms, will shutdown the ingestion backend gracefully.");
        isShuttingDown.set(true);
        try {
          stop();
          // Force closing the JVM process as we don't want any lingering process. It is safe to exit the JVM now as all necessary resources are shutdown.
          System.exit(0);
        } catch (Exception e) {
          logger.info("Unable to shutdown ingestion service gracefully", e);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    logger.info("Capture arguments: " + Arrays.toString(args));
    if (args.length < 1) {
      throw new VeniceException("Expected at least one arguments: port. Got " + args.length);
    }
    int port = Integer.parseInt(args[0]);
    IsolatedIngestionServer isolatedIngestionServer;
    if (args.length == 2) {
      isolatedIngestionServer = new IsolatedIngestionServer(port, Long.parseLong(args[1]));
    } else {
      isolatedIngestionServer = new IsolatedIngestionServer(port);
    }
    isolatedIngestionServer.start();
  }
}
