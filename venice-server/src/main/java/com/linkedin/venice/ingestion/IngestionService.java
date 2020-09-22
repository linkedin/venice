package com.linkedin.venice.ingestion;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.channel.IngestionServiceChannelInitializer;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.venice.meta.IngestionAction;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
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

  // TODO: Make unused variable local in IngestionServiceTaskHandler
  private VeniceConfigLoader configLoader = null;
  private SubscriptionBasedReadOnlyStoreRepository storeRepository = null;
  private StorageService storageService = null;
  private KafkaStoreIngestionService storeIngestionService = null;
  private boolean isInitiated = false;
  private final Map<String, Set<Integer>> topicNameToPartitionSetMap = new VeniceConcurrentHashMap<>();
  private final Map<String, Map<Integer, Long>> readyToServePartitionMap = new VeniceConcurrentHashMap<>();
  private final ReentrantLock updateLock = new ReentrantLock();

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
    serverFuture = bootstrap.bind(servicePort).sync();
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

  public void addIngestionPartition(String topicName, int partitionId) {
    updateLock.lock();
    Set<Integer> activePartitions = topicNameToPartitionSetMap.getOrDefault(topicName, new HashSet<>());
    activePartitions.add(partitionId);
    topicNameToPartitionSetMap.put(topicName, activePartitions);
    updateLock.unlock();
  }

  public void removeIngestionPartition(String topicName, int partitionId, long offset) {
    updateLock.lock();
    Set<Integer> activePartitions = topicNameToPartitionSetMap.getOrDefault(topicName, new HashSet<>());
    activePartitions.remove(partitionId);
    topicNameToPartitionSetMap.put(topicName, activePartitions);
    Map<Integer, Long> readyPartitions = readyToServePartitionMap.getOrDefault(topicName, new VeniceConcurrentHashMap<>());
    readyPartitions.put(partitionId, offset);
    readyToServePartitionMap.put(topicName, readyPartitions);
    updateLock.unlock();

    // All partitions related to this topic has been ingested successfully.
    if (activePartitions.isEmpty()) {
      logger.info("All partitions have done consumption for topic " + topicName + ", closing storage engine now.");
      // Close all partitions including metadata partition. This will unblock the storage engine serving in main service.
      storageService.getStorageEngineRepository().getLocalStorageEngine(topicName).close();
      readyToServePartitionMap.remove(topicName);
      readyPartitions.forEach((p, o) -> {
        reportIngestionCompletion(topicName, p, o);
      });
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

  public void setReportClient(IngestionRequestClient reportClient) {
    this.reportClient = reportClient;
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

  public SubscriptionBasedReadOnlyStoreRepository getStoreRepository() {
    return storeRepository;
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

  private void reportIngestionCompletion(String topicName, int partitionId, long offset) {
    // Send ingestion status change report to report listener.
    IngestionTaskReport report = new IngestionTaskReport();
    report.isComplete = true;
    report.isEndOfPushReceived = true;
    report.isError = false;
    report.errorMessage = "";
    report.topicName = topicName;
    report.partitionId = partitionId;
    report.offset = offset;
    byte[] serializedReport = serializeIngestionTaskReport(report);
    try {
      logger.info("Sending ingestion completion report for version:  " + topicName + " partition id:" + partitionId + " offset:" + offset);
      reportClient.sendRequest(reportClient.buildHttpRequest(IngestionAction.REPORT, serializedReport));
    } catch (Exception e) {
      logger.warn("Failed to send report to application with exception: " + e.getMessage());
    }
  }
}
