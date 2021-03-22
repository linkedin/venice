package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.channel.IngestionReportChannelInitializer;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.*;
import static java.lang.Thread.*;


/**
 * IngestionReportListener is the listener server that handles {@link com.linkedin.venice.ingestion.protocol.IngestionTaskReport}
 * and {@link com.linkedin.venice.ingestion.protocol.IngestionMetricsReport} sent from child process.
 */
public class IngestionReportListener extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(IngestionReportListener.class);
  private final ServerBootstrap bootstrap;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  // Application port is the port Da Vinci application is binding and listening to.
  private final int applicationPort;
  // Service port is the port isolated ingestion process is binding and listening to.
  private final int servicePort;
  private final IsolatedIngestionBackend ingestionBackend;
  private final ScheduledExecutorService metricsRequestScheduler = Executors.newScheduledThreadPool(1);
  private final ScheduledExecutorService heartbeatCheckScheduler = Executors.newScheduledThreadPool(1);
  private final IngestionRequestClient metricsClient;
  private final IngestionRequestClient heartbeatClient;
  // Topic name to partition set map, representing all topic partitions being ingested in Isolated Ingestion Backend.
  private final Map<String, Set<Integer>> topicNameToPartitionSetMap = new VeniceConcurrentHashMap<>();
  // Topic name to partition set map, representing all topic partitions that have completed ingestion in isolated process.
  private final Map<String, Set<Integer>> completedTopicPartitions = new VeniceConcurrentHashMap<>();
  private final List<VeniceNotifier> ingestionNotifierList = new ArrayList<>();
  private final List<VeniceNotifier> pushStatusNotifierList = new ArrayList<>();

  private ChannelFuture serverFuture;
  private MetricsRepository metricsRepository;
  private IngestionProcessStats ingestionProcessStats;
  private IngestionStorageMetadataService storageMetadataService;
  private KafkaStoreIngestionService storeIngestionService;

  private VeniceConfigLoader configLoader;
  private long latestHeartbeatTimestamp = -1;
  private long heartbeatTimeoutMs;

  public IngestionReportListener(IsolatedIngestionBackend ingestionBackend, int applicationPort, int servicePort) {
    this.applicationPort = applicationPort;
    this.servicePort = servicePort;
    this.ingestionBackend = ingestionBackend;

    // Initialize Netty server.
    Class<? extends ServerChannel> serverSocketChannelClass = NioServerSocketChannel.class;
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup();
    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup).channel(serverSocketChannelClass)
            .childHandler(new IngestionReportChannelInitializer(this))
            .option(ChannelOption.SO_BACKLOG, 1000)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.TCP_NODELAY, true);

    heartbeatClient = new IngestionRequestClient(this.servicePort);
    metricsClient = new IngestionRequestClient(this.servicePort);
  }

  @Override
  public boolean startInner() throws Exception {
    serverFuture = bootstrap.bind(applicationPort).sync();
    logger.info("Report listener service started on port: " + applicationPort);
    if (configLoader == null) {
      throw new VeniceException("Venice config not found in IngestionReportListener!");
    }
    heartbeatTimeoutMs = configLoader.getCombinedProperties().getLong(SERVER_INGESTION_ISOLATION_HEARTBEAT_TIMEOUT_MS, TimeUnit.SECONDS
        .toMillis(60));
    setupMetricsCollection();

    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    metricsRequestScheduler.shutdown();
    heartbeatCheckScheduler.shutdown();
    try {
      if (!metricsRequestScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        metricsRequestScheduler.shutdownNow();
      }
      if (!heartbeatCheckScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        heartbeatCheckScheduler.shutdownNow();
      }

    } catch (InterruptedException e) {
      currentThread().interrupt();
    }

    heartbeatClient.close();
    metricsClient.close();

    ChannelFuture shutdown = serverFuture.channel().closeFuture();
    workerGroup.shutdownGracefully();
    bossGroup.shutdownGracefully();
    shutdown.sync();
  }

  public void addIngestionNotifier(VeniceNotifier ingestionListener) {
    if (ingestionListener != null) {
      ingestionNotifierList.add(ingestionListener);
    }
  }

  public List<VeniceNotifier> getIngestionNotifierList() {
    return ingestionNotifierList;
  }

  public void addPushStatusNotifier(VeniceNotifier pushStatusNotifier) {
    if (pushStatusNotifier != null) {
      pushStatusNotifierList.add(pushStatusNotifier);
    }
  }

  public List<VeniceNotifier> getPushStatusNotifierList() {
    return pushStatusNotifierList;
  }

  public void setMetricsRepository(MetricsRepository metricsRepository) {
    this.metricsRepository = metricsRepository;
  }

  public void setConfigLoader(VeniceConfigLoader configLoader) {
    this.configLoader = configLoader;
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public void setStorageMetadataService(IngestionStorageMetadataService storageMetadataService) {
    this.storageMetadataService = storageMetadataService;
  }

  public IngestionStorageMetadataService getStorageMetadataService() {
    return storageMetadataService;
  }

  public VeniceConfigLoader getConfigLoader() {
    return configLoader;
  }

  public void setStoreIngestionService(KafkaStoreIngestionService storeIngestionService) {
    this.storeIngestionService = storeIngestionService;
  }

  public KafkaStoreIngestionService getStoreIngestionService() {
    return storeIngestionService;
  }

  public boolean isTopicPartitionIngestedInIsolatedProcess(String topicName, int partitionId) {
    if (completedTopicPartitions.containsKey(topicName)) {
      return completedTopicPartitions.get(topicName).contains(partitionId);
    }
    return false;
  }

  public void removeVersionPartitionFromIngestionMap(String topicName, int partitionId) {
    completedTopicPartitions.putIfAbsent(topicName, new HashSet<>());
    completedTopicPartitions.computeIfPresent(topicName, (key, val) -> {
      val.add(partitionId);
      return val;
    });

    topicNameToPartitionSetMap.computeIfPresent(topicName, (key, val) -> {
      val.remove(partitionId);
      return val;
    });
  }

  public void addVersionPartitionToIngestionMap(String topicName, int partitionId) {
    topicNameToPartitionSetMap.putIfAbsent(topicName, new HashSet<>());
    topicNameToPartitionSetMap.computeIfPresent(topicName, (key, val) -> {
      val.add(partitionId);
      return val;
    });
  }

  // Remove the topic entry in the subscription topic partition map.
  public void removedSubscribedTopicName(String topicName) {
    topicNameToPartitionSetMap.remove(topicName);
  }

  private void setupMetricsCollection() {
    if (metricsRepository == null) {
      logger.warn("No metrics repository is set up in ingestion report listener, skipping metrics collection");
      return;
    }

    ingestionProcessStats = new IngestionProcessStats(metricsRepository);
    metricsRequestScheduler.scheduleAtFixedRate(this::collectIngestionServiceMetrics, 0, 5, TimeUnit.SECONDS);
    heartbeatCheckScheduler.scheduleAtFixedRate(this::checkHeartbeatTimeout, 0, 10, TimeUnit.SECONDS);
  }

  private void restartForkedProcess() {
    try (IngestionRequestClient client = new IngestionRequestClient(servicePort)) {
      ingestionBackend.setIsolatedIngestionServiceProcess(client.startForkedIngestionProcess(configLoader));

      // Reset heartbeat time.
      latestHeartbeatTimestamp = -1;
      // Open metadata partitions in child process for all previously subscribed topics.
      topicNameToPartitionSetMap.keySet().forEach(client::openStorageEngine);
      // All previously subscribed topics are stored in the keySet of this topic partition map.
      topicNameToPartitionSetMap.forEach((topicName, partitionSet) -> {
        partitionSet.forEach(partitionId -> {
          client.startConsumption(topicName, partitionId);
        });
      });
    }
    logger.info("Restart forked process completed");
  }

  private void collectIngestionServiceMetrics() {
    if (metricsClient.collectMetrics(ingestionProcessStats)) {
      // Update heartbeat time.
      latestHeartbeatTimestamp = System.currentTimeMillis();
    }
  }

  private void checkHeartbeatTimeout() {
    long currentTimeMillis = System.currentTimeMillis();
    if (logger.isDebugEnabled()) {
      logger.info("Checking heartbeat timeout at " + currentTimeMillis + ", current heartbeat: " + latestHeartbeatTimestamp);
    }
    if (heartbeatClient.sendHeartbeatRequest()) {
      // Update heartbeat time.
      latestHeartbeatTimestamp = System.currentTimeMillis();
    }

    if (latestHeartbeatTimestamp != -1) {
      if ((currentTimeMillis - latestHeartbeatTimestamp) > heartbeatTimeoutMs) {
        logger.warn("Lost connection to forked ingestion process since timestamp, restarting forked process.");
        restartForkedProcess();
      }
    }
  }
}
