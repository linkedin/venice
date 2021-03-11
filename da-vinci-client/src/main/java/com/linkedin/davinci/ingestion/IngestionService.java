package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.ingestion.channel.IngestionServiceChannelInitializer;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.Utils;
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
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    try {
      if (!ingestionExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        ingestionExecutor.shutdownNow();
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
    if (report.isCompleted || report.isError) {
      // Use async to avoid deadlock waiting in StoreBufferDrainer
      ingestionExecutor.execute(() -> stopIngestionAndReportIngestionStatus(report));
    } else {
      reportClient.reportIngestionTask(report);
    }
  }

  public RedundantExceptionFilter getRedundantExceptionFilter() {
    return redundantExceptionFilter;
  }

  /**
   * Handle the logic of COMPLETED/ERROR here since we need to stop related ingestion task and close RocksDB partition.
   * Since the logic takes time to wait for completion, we need to execute it in async fashion to prevent blocking other operations.
   */
  private void stopIngestionAndReportIngestionStatus(IngestionTaskReport report) {
    String topicName = report.topicName.toString();
    int partitionId = report.partitionId;
    long offset = report.offset;

    VeniceStoreConfig storeConfig = getConfigLoader().getStoreConfig(topicName);
    // Make sure partition is not consuming so we can safely close the rocksdb partition
    getStoreIngestionService().stopConsumptionAndWait(storeConfig, partitionId, 1, 60);
    // Close RocksDB partition in Ingestion Service.
    getStorageService().getStorageEngineRepository().getLocalStorageEngine(topicName).closePartition(partitionId);
    logger.info("Partition: " + partitionId + " of topic: " + topicName + " closed.");

    if (report.isCompleted) {
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
    } else {
      logger.error("Ingestion error for topic: " + topicName + ", partition id: " + partitionId + " " + report.message);
    }
    reportClient.reportIngestionTask(report);
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
