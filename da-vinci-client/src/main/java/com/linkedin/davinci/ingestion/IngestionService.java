package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.helix.LeaderFollowerParticipantModel;
import com.linkedin.davinci.ingestion.channel.IngestionServiceChannelInitializer;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.enums.IngestionReportType;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
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
import java.util.HashMap;
import java.util.Map;
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

import static java.lang.Thread.*;


/**
 * IngestionService is the server service of the ingestion isolation side.
 */
public class IngestionService extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(IngestionService.class);

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
  private final Map<String, Map<Integer, AtomicLong>> leaderSessionIdMap = new HashMap<>();
  // The boolean value of this map indicates whether we have added UNSUBSCRIBE message to the processing queue.
  // We should not add leader change message into the queue if we have added UNSUBSCRIBE message to the queue, otherwise it won't get processed.
  // This will help leader promo/demote request from parent process fail out early and avoid race condition.
  private final Map<String, Map<Integer, AtomicBoolean>> topicPartitionSubscriptionMap = new HashMap<>();

  private ChannelFuture serverFuture;
  private MetricsRepository metricsRepository = null;
  private VeniceConfigLoader configLoader = null;
  private SubscriptionBasedReadOnlyStoreRepository storeRepository = null;
  private StorageService storageService = null;
  private KafkaStoreIngestionService storeIngestionService = null;
  private StorageMetadataService storageMetadataService = null;
  // PartitionState and StoreVersionState serializers are lazily constructed after receiving the init configs
  private InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;
  private InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer;
  private boolean isInitiated = false;
  private IngestionRequestClient reportClient;
  private long heartbeatTime = -1;
  private long heartbeatTimeoutMs;

  public IngestionService(int servicePort, long heartbeatTimeoutMs) {
    this.servicePort = servicePort;
    this.heartbeatTimeoutMs = heartbeatTimeoutMs;

    // Initialize Netty server.
    Class<? extends ServerChannel> serverSocketChannelClass = NioServerSocketChannel.class;
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup();
    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup).channel(serverSocketChannelClass)
        .childHandler(new IngestionServiceChannelInitializer(this))
        .option(ChannelOption.SO_BACKLOG, 1000)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.TCP_NODELAY, true);
    logger.info("IngestionService created");
  }

  public IngestionService(int servicePort) {
    this(servicePort, TimeUnit.MILLISECONDS.toMillis(60));
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
      if (storageService != null) {
        storageService.stop();
      }
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

  public void setStoreRepository(SubscriptionBasedReadOnlyStoreRepository storeRepository) {
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

  public void setReportClient(IngestionRequestClient reportClient) {
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

  public IngestionRequestClient getReportClient() {
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

  public SubscriptionBasedReadOnlyStoreRepository getStoreRepository() {
    return storeRepository;
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public InternalAvroSpecificSerializer<PartitionState> getPartitionStateSerializer() {
    return partitionStateSerializer;
  }

  public InternalAvroSpecificSerializer<StoreVersionState> getStoreVersionStateSerializer() {
    return storeVersionStateSerializer;
  }

  public void updateHeartbeatTime() {
    this.heartbeatTime = System.currentTimeMillis();
  }

  public void reportIngestionStatus(IngestionTaskReport report) {
    if (IngestionReportType.valueOf(report.reportType).equals(IngestionReportType.COMPLETED) || IngestionReportType.valueOf(report.reportType).equals(IngestionReportType.ERROR)) {
      setPartitionToBeUnsubscribed(report.topicName.toString(), report.partitionId);

      // Use async to avoid deadlock waiting in StoreBufferDrainer
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
        if (IngestionReportType.valueOf(report.reportType).equals(IngestionReportType.COMPLETED)) {
          logger.info("Ingestion completed for topic: " + topicName + ", partition id: " + partitionId + ", offset: " + offset);
          // Set offset record in ingestion report.
          OffsetRecord offsetRecord = storageMetadataService.getLastOffset(topicName, partitionId);
          report.offsetRecord = ByteBuffer.wrap(offsetRecord.toBytes());
          logger.info("OffsetRecord of topic " + topicName + " , partition " + partitionId + " " + offsetRecord.toString());

          // Set store version state in ingestion report.
          Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(topicName);
          if (storeVersionState.isPresent()) {
            report.storeVersionState = ByteBuffer.wrap(IngestionUtils.serializeStoreVersionState(topicName, storeVersionState.get()));
          } else {
            throw new VeniceException("StoreVersionState does not exist for version " + topicName);
          }
          // TODO: Transmit current L/F state to main process in following RB.
        } else {
          logger.error("Ingestion error for topic: " + topicName + ", partition id: " + partitionId + " " + report.message);
        }
        reportClient.reportIngestionTask(report);
      });
    } else {
      statusReportingExecutor.execute(() ->  reportClient.reportIngestionTask(report));
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
    topicPartitionSubscriptionMap.putIfAbsent(topicName, new HashMap<>());
    topicPartitionSubscriptionMap.get(topicName).putIfAbsent(partition, new AtomicBoolean(false));
    topicPartitionSubscriptionMap.get(topicName).get(partition).set(false);
  }

  // Check if topic partition is being unsubscribed.
  public boolean isPartitionBeingUnsubscribed(String topicName, int partition) {
    if (!topicPartitionSubscriptionMap.containsKey(topicName)) {
      return false;
    }
    return topicPartitionSubscriptionMap.get(topicName).getOrDefault(partition, new AtomicBoolean(true)).get();
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
      getStoreIngestionService().stopConsumptionAndWait(storeConfig, partitionId, 1, 60);
      // Close RocksDB partition in Ingestion Service.
      getStorageService().getStorageEngineRepository().getLocalStorageEngine(topicName).closePartition(partitionId);
      logger.info("Partition: " + partitionId + " of topic: " + topicName + " closed.");
    });
  }

  private void checkHeartbeatTimeout() {
    if (!isShuttingDown.get()) {
      long currentTimeMillis = System.currentTimeMillis();
      logger.info("Checking heartbeat timeout at " + currentTimeMillis + ", latest heartbeat: " + heartbeatTime);

      if ((heartbeatTime != -1) && ((currentTimeMillis - heartbeatTime) > heartbeatTimeoutMs)) {
        logger.warn("Lost connection to parent process, will shutdown the ingestion backend gracefully.");
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
    IngestionService ingestionService;
    if (args.length == 2) {
      ingestionService = new IngestionService(port, Long.parseLong(args[1]));
    } else {
      ingestionService = new IngestionService(port);
    }
    ingestionService.start();
  }
}
