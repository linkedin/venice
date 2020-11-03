package com.linkedin.venice.ingestion;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.channel.IngestionServiceChannelInitializer;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.IngestionAction;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.storage.StorageMetadataService;
import com.linkedin.venice.storage.StorageService;
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
import org.apache.log4j.Logger;

import static com.linkedin.venice.ingestion.IngestionUtils.*;


/**
 * IngestionService is the server service of the ingestion isolation side.
 */
public class IngestionService extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(IngestionService.class);
  private final ServerBootstrap bootstrap;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private ChannelFuture serverFuture;

  private MetricsRepository metricsRepository = null;
  private VeniceConfigLoader configLoader = null;
  private SubscriptionBasedReadOnlyStoreRepository storeRepository = null;
  private StorageService storageService = null;
  private KafkaStoreIngestionService storeIngestionService = null;
  private StorageMetadataService storageMetadataService = null;
  private boolean isInitiated = false;

  private final int servicePort;
  private IngestionRequestClient reportClient;

  //TODO: move netty config to a config file
  private static int nettyBacklogSize = 1000;

  public IngestionService(int servicePort) {
    this.servicePort = servicePort;

    // Initialize Netty server.
    Class<? extends ServerChannel> serverSocketChannelClass = NioServerSocketChannel.class;
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup();
    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup).channel(serverSocketChannelClass)
        .childHandler(new IngestionServiceChannelInitializer(this))
        .option(ChannelOption.SO_BACKLOG, nettyBacklogSize)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.TCP_NODELAY, true);
    logger.info("IngestionService created");
  }

  @Override
  public boolean startInner() throws Exception {
    IngestionUtils.releaseTargetPortBinding(servicePort);
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
    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    ChannelFuture shutdown = serverFuture.channel().closeFuture();
    workerGroup.shutdownGracefully();
    bossGroup.shutdownGracefully();
    shutdown.sync();

    try {
      storeIngestionService.stop();
      storageService.stop();
    } catch (Throwable e) {
      throw new VeniceException("Unable to stop Ingestion Service", e);
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


  public void reportIngestionCompletion(String topicName, int partitionId, long offset) {
    // Send ingestion status change report to report listener.
    IngestionTaskReport report = new IngestionTaskReport();
    report.isComplete = true;
    report.isEndOfPushReceived = true;
    report.isError = false;
    report.errorMessage = "";
    report.topicName = topicName;
    report.partitionId = partitionId;
    report.offset = offset;
    OffsetRecord offsetRecord = storageMetadataService.getLastOffset(topicName, partitionId);
    logger.info("OffsetRecord of topic " + topicName + " , partition " + partitionId + " " + offsetRecord.toString());
    report.offsetRecord = ByteBuffer.wrap(offsetRecord.toBytes());
    Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(topicName);
    if (storeVersionState.isPresent()) {
      report.storeVersionState = ByteBuffer.wrap(IngestionUtils.serializeStoreVersionState(topicName, storeVersionState.get()));
    } else {
      throw new VeniceException("StoreVersionState does not exist for version " + topicName);
    }
    byte[] serializedReport = serializeIngestionTaskReport(report);
    try {
      logger.info("Sending ingestion completion report for version:  " + topicName + " partition id:" + partitionId + " offset:" + offset);
      reportClient.sendRequest(reportClient.buildHttpRequest(IngestionAction.REPORT, serializedReport));
    } catch (Exception e) {
      logger.warn("Failed to send report to application with exception: " + e.getMessage());
    }
  }

  public void reportIngestionError(String topicName, int partitionId, Exception e) {
    // Send ingestion status change report to report listener.
    IngestionTaskReport report = new IngestionTaskReport();
    report.isComplete = false;
    report.isEndOfPushReceived = false;
    report.isError = true;
    report.errorMessage = e.getClass().getSimpleName() + "_" + e.getMessage();
    report.topicName = topicName;
    report.partitionId = partitionId;
    byte[] serializedReport = serializeIngestionTaskReport(report);
    try {
      logger.info("Sending ingestion error report for version:  " + topicName + " partition id:" + partitionId);
      reportClient.sendRequest(reportClient.buildHttpRequest(IngestionAction.REPORT, serializedReport));
    } catch (Exception ex) {
      logger.warn("Failed to send report to application with exception: " + ex.getMessage());
    }
  }

  public static void main(String[] args) throws Exception {
    logger.info("Capture arguments: " + Arrays.toString(args));
    if (args.length != 1) {
      throw new VeniceException("Expected one arguments: port. Got " + args.length);
    }
    int port = Integer.parseInt(args[0]);
    IngestionService ingestionService = new IngestionService(port);
    ingestionService.startInner();
  }
}
