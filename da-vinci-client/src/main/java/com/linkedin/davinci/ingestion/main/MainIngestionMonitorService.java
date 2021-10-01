package com.linkedin.davinci.ingestion.main;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.IsolatedIngestionBackend;
import com.linkedin.davinci.ingestion.IsolatedIngestionProcessStats;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.IsolatedIngestionProcessHeartbeatStats;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Time;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.*;
import static java.lang.Thread.*;


/**
 * MainIngestionMonitorService is the listener service in main process which handles various kinds of reports sent from
 * isolated ingestion service. MainIngestionMonitorService itself is a Netty based server implementation, and the main
 * report handling logics happens in {@link MainIngestionReportHandler}.
 * Besides reports handling, it also maintains two executor services to send heartbeat check and collect metrics to/from
 * child process. Also, it maintains status for all the ongoing/completed topic partition ingestion tasks, which helps
 * {@link IsolatedIngestionBackend} to check which process a topic partition storage is located, as well as status recovery
 * when child process crashed and restarted.
 */
public class MainIngestionMonitorService extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(MainIngestionMonitorService.class);
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
  private final ExecutorService longRunningTaskExecutor = Executors.newSingleThreadExecutor();
  private final MainIngestionRequestClient metricsClient;
  private final MainIngestionRequestClient heartbeatClient;
  // Topic name to partition set map, representing all topic partitions being ingested in Isolated Ingestion Backend.
  private final Map<String, Set<Integer>> topicNameToPartitionSetMap = new VeniceConcurrentHashMap<>();
  // Topic name to partition set map, representing all topic partitions that have completed ingestion in isolated process.
  private final Map<String, Set<Integer>> completedTopicPartitions = new VeniceConcurrentHashMap<>();
  private final List<VeniceNotifier> ingestionNotifierList = new ArrayList<>();
  private final List<VeniceNotifier> pushStatusNotifierList = new ArrayList<>();
  private final Optional<SSLFactory> sslFactory;

  private IsolatedIngestionProcessHeartbeatStats heartbeatStats;
  private ChannelFuture serverFuture;
  private MetricsRepository metricsRepository;
  private IsolatedIngestionProcessStats isolatedIngestionProcessStats;
  private MainIngestionStorageMetadataService storageMetadataService;
  private KafkaStoreIngestionService storeIngestionService;
  private VeniceConfigLoader configLoader;
  private long heartbeatTimeoutMs;
  private volatile long latestHeartbeatTimestamp = -1;

  public MainIngestionMonitorService(IsolatedIngestionBackend ingestionBackend, VeniceConfigLoader configLoader, Optional<SSLFactory> sslFactory) {
    this.configLoader = configLoader;
    this.servicePort = configLoader.getVeniceServerConfig().getIngestionServicePort();
    this.applicationPort = configLoader.getVeniceServerConfig().getIngestionApplicationPort();
    this.ingestionBackend = ingestionBackend;
    this.sslFactory = sslFactory;

    // Initialize Netty server.
    Class<? extends ServerChannel> serverSocketChannelClass = NioServerSocketChannel.class;
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup();
    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup).channel(serverSocketChannelClass)
            .childHandler(new MainIngestionReportChannelInitializer(this, IsolatedIngestionUtils.getSSLEngineComponentFactory(configLoader)))
            .option(ChannelOption.SO_BACKLOG, 1000)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.TCP_NODELAY, true);

    heartbeatClient = new MainIngestionRequestClient(this.sslFactory, this.servicePort);
    metricsClient = new MainIngestionRequestClient(this.sslFactory, this.servicePort);

  }

  @Override
  public boolean startInner() throws Exception {
    serverFuture = bootstrap.bind(applicationPort).sync();
    logger.info("Report listener service started on port: " + applicationPort);
    heartbeatTimeoutMs = configLoader.getCombinedProperties().getLong(SERVER_INGESTION_ISOLATION_HEARTBEAT_TIMEOUT_MS, 60 * Time.MS_PER_SECOND);
    setupMetricsCollection();

    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    shutdownScheduler(metricsRequestScheduler, "Metrics collection");
    shutdownScheduler(heartbeatCheckScheduler, "Heartbeat check");
    shutdownScheduler(longRunningTaskExecutor, "Long running task");

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

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public void setStorageMetadataService(MainIngestionStorageMetadataService storageMetadataService) {
    this.storageMetadataService = storageMetadataService;
  }

  public MainIngestionStorageMetadataService getStorageMetadataService() {
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
    if (topicNameToPartitionSetMap.getOrDefault(topicName, Collections.emptySet()).contains(partitionId)) {
      // Add topic partition to completed task pool.
      completedTopicPartitions.putIfAbsent(topicName, new HashSet<>());
      completedTopicPartitions.computeIfPresent(topicName, (key, val) -> {
        val.add(partitionId);
        return val;
      });
      // Remove topic partition from ongoing task pool.
      topicNameToPartitionSetMap.computeIfPresent(topicName, (key, val) -> {
        val.remove(partitionId);
        return val;
      });
    } else {
      logger.error("Topic partition not found in ongoing ingestion tasks: " + topicName + ", partition id: " + partitionId);
    }
  }

  public void addVersionPartitionToIngestionMap(String topicName, int partitionId) {
    // Add topic partition to ongoing task pool.
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

  public void cleanupTopicPartitionState(String topicName, int partitionId) {
    topicNameToPartitionSetMap.getOrDefault(topicName, Collections.emptySet()).remove(partitionId);
    completedTopicPartitions.getOrDefault(topicName, Collections.emptySet()).remove(partitionId);
  }

  public void cleanupTopicState(String topicName) {
    topicNameToPartitionSetMap.remove(topicName);
    completedTopicPartitions.remove(topicName);
  }

  private void setupMetricsCollection() {
    if (metricsRepository == null) {
      logger.warn("No metrics repository is set up in ingestion report listener, skipping metrics collection");
      return;
    }
    heartbeatStats = new IsolatedIngestionProcessHeartbeatStats(metricsRepository);
    isolatedIngestionProcessStats = new IsolatedIngestionProcessStats(metricsRepository);
    metricsRequestScheduler.scheduleAtFixedRate(this::collectIngestionServiceMetrics, 0, 5, TimeUnit.SECONDS);
    heartbeatCheckScheduler.scheduleAtFixedRate(this::checkHeartbeatTimeout, 0, 10, TimeUnit.SECONDS);
  }

  private synchronized void tryRestartForkedProcess() {
    /**
     * Before we add timeout to client request, there might be multiple requests being blocked at the same time and
     * get responses at the same time. This might cause multiple heartbeat request think it is timing out and trigger
     * this call. Here we use synchronized modifier and add timeout checking here to make sure we only restart forked
     * process once.
     */
    if ((System.currentTimeMillis() - latestHeartbeatTimestamp) <= heartbeatTimeoutMs) {
      return;
    }
    logger.warn("Lost connection to forked ingestion process since timestamp " + latestHeartbeatTimestamp + ", restarting forked process.");
    heartbeatStats.recordForkedProcessRestart();
    try (MainIngestionRequestClient client = new MainIngestionRequestClient(sslFactory, servicePort)) {
      /**
       * We need to destroy the previous isolated ingestion process first.
       * The previous isolated ingestion process might have released the port binding, but it might still taking up all
       * RocksDB storage locks and JVM memory during slow shutdown process and new forked process might fail to start
       * without necessary resources.
       */
      IsolatedIngestionUtils.destroyIsolatedIngestionProcess(ingestionBackend.getIsolatedIngestionServiceProcess());
      Process newIsolatedIngestionProcess = client.startForkedIngestionProcess(configLoader);
      ingestionBackend.setIsolatedIngestionServiceProcess(newIsolatedIngestionProcess);
      logger.info("Forked process has been recovered.");
    }
    // Re-initialize latest heartbeat timestamp.
    latestHeartbeatTimestamp = System.currentTimeMillis();
    heartbeatStats.recordHeartbeatAge(0);
    // Use async long running task scheduler to avoid blocking periodic heartbeat jobs.
    longRunningTaskExecutor.execute(this::resumeOngoingIngestionTasks);
  }

  private void resumeOngoingIngestionTasks() {
    try (MainIngestionRequestClient client = new MainIngestionRequestClient(sslFactory, servicePort)) {
      logger.info("Start to recover ongoing ingestion tasks: " + topicNameToPartitionSetMap);
      // Open metadata partitions in child process for all previously subscribed topics.
      topicNameToPartitionSetMap.keySet().forEach(client::openStorageEngine);
      // All previously subscribed topics are stored in the keySet of this topic partition map.
      topicNameToPartitionSetMap.forEach((topicName, partitionSet) -> {
        partitionSet.forEach(partitionId -> {
          client.startConsumption(topicName, partitionId);
          logger.info("Recovered ingestion task for topic: " + topicName + ", partition: " + partitionId);
        });
      });
      logger.info("All ongoing ingestion tasks has resumed.");
    }
  }

  private void collectIngestionServiceMetrics() {
    if (metricsClient.collectMetrics(isolatedIngestionProcessStats)) {
      // Update heartbeat time.
      latestHeartbeatTimestamp = System.currentTimeMillis();
    }
  }

  private void checkHeartbeatTimeout() {
    long currentTimeMillis = System.currentTimeMillis();
    logger.info("Checking heartbeat timeout at " + currentTimeMillis + ", latest heartbeat received: " + latestHeartbeatTimestamp);
    if (heartbeatClient.sendHeartbeatRequest()) {
      // Update heartbeat time.
      latestHeartbeatTimestamp = System.currentTimeMillis();
      heartbeatStats.recordHeartbeatAge(0);
      logger.info("Received isolated ingestion server heartbeat at: " + latestHeartbeatTimestamp);
    } else {
      heartbeatStats.recordHeartbeatAge(currentTimeMillis - latestHeartbeatTimestamp);
      logger.warn("Failed to connect to forked ingestion process at " + currentTimeMillis + ", last successful timestamp: " + latestHeartbeatTimestamp);
    }

    if (latestHeartbeatTimestamp != -1) {
      if ((currentTimeMillis - latestHeartbeatTimestamp) > heartbeatTimeoutMs) {
        tryRestartForkedProcess();
      }
    }
  }

  private void shutdownScheduler(ExecutorService scheduler, String schedulerName) {
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
        logger.info(schedulerName + " scheduler has been shutdown.");
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }
  }
}
