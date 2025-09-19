package com.linkedin.davinci.ingestion.main;

import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_CONNECTION_TIMEOUT_SECONDS;
import static java.lang.Thread.currentThread;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.IsolatedIngestionBackend;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.IsolatedIngestionProcessHeartbeatStats;
import com.linkedin.davinci.stats.IsolatedIngestionProcessStats;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Time;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is the listener service in main process which handles various kinds of reports sent from
 * isolated ingestion service. MainIngestionMonitorService itself is a Netty based server implementation, and the main
 * report handling logics happens in {@link MainIngestionReportHandler}.
 * Besides reports handling, it also maintains two executor services to send heartbeat check and collect metrics to/from
 * child process. Also, it maintains status for all the ongoing/completed topic partition ingestion tasks, which helps
 * {@link IsolatedIngestionBackend} to check which process a topic partition storage is located, as well as status recovery
 * when child process crashed and restarted.
 */
public class MainIngestionMonitorService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(MainIngestionMonitorService.class);
  private final ServerBootstrap bootstrap;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final IsolatedIngestionBackend ingestionBackend;
  private final ScheduledExecutorService heartbeatCheckScheduler = Executors.newScheduledThreadPool(1);
  private final ExecutorService longRunningTaskExecutor = Executors.newSingleThreadExecutor();
  private final MainIngestionRequestClient heartbeatClient;
  private final Map<String, MainTopicIngestionStatus> topicIngestionStatusMap = new VeniceConcurrentHashMap<>();
  private final List<VeniceNotifier> ingestionNotifierList = new ArrayList<>();
  private final List<VeniceNotifier> pushStatusNotifierList = new ArrayList<>();

  private IsolatedIngestionProcessHeartbeatStats heartbeatStats;
  private ChannelFuture serverFuture;
  private MetricsRepository metricsRepository;
  private IsolatedIngestionProcessStats isolatedIngestionProcessStats;
  private MainIngestionStorageMetadataService storageMetadataService;
  private KafkaStoreIngestionService storeIngestionService;
  private final VeniceConfigLoader configLoader;
  /**
   * Heartbeat timeout acknowledge disconnection between main and forked processes. After this timeout, main process
   * should (1) Try to kill lingering forked process to release port (2) Respawn new forked ingestion process to continue
   * ingestion work.
   */
  private long connectionTimeoutMs;
  private volatile long latestHeartbeatTimestamp = -1;
  private final PubSubPositionDeserializer pubSubPositionDeserializer;

  public MainIngestionMonitorService(IsolatedIngestionBackend ingestionBackend, VeniceConfigLoader configLoader) {
    this.configLoader = configLoader;
    this.ingestionBackend = ingestionBackend;

    // Initialize Netty server.
    Class<? extends ServerChannel> serverSocketChannelClass = NioServerSocketChannel.class;
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup();
    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup)
        .channel(serverSocketChannelClass)
        .childHandler(
            new MainIngestionReportChannelInitializer(this, IsolatedIngestionUtils.getSSLFactory(configLoader)))
        .option(ChannelOption.SO_BACKLOG, 1000)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.TCP_NODELAY, true);

    heartbeatClient = new MainIngestionRequestClient(configLoader);
    PubSubPositionTypeRegistry pubSubPositionTypeRegistry =
        PubSubPositionTypeRegistry.fromPropertiesOrDefault(configLoader.getCombinedProperties());
    pubSubPositionDeserializer = new PubSubPositionDeserializer(pubSubPositionTypeRegistry);
  }

  @Override
  public boolean startInner() throws Exception {
    int applicationPort = configLoader.getVeniceServerConfig().getIngestionApplicationPort();
    serverFuture = bootstrap.bind(applicationPort).sync();
    LOGGER.info("Report listener service started on port: {}", applicationPort);
    connectionTimeoutMs =
        configLoader.getCombinedProperties().getLong(SERVER_INGESTION_ISOLATION_CONNECTION_TIMEOUT_SECONDS, 180)
            * Time.MS_PER_SECOND;
    setupMetricsCollection();

    // There is no async process in this function, so we are completely finished with the start-up process.
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    shutdownScheduler(heartbeatCheckScheduler, "Heartbeat check");
    shutdownScheduler(longRunningTaskExecutor, "Long running task");
    heartbeatClient.close();

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

  public List<VeniceNotifier> getIngestionNotifier() {
    return ingestionNotifierList;
  }

  public PubSubPositionDeserializer getPubSubPositionDeserializer() {
    return pubSubPositionDeserializer;
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

  public void setStorageMetadataService(MainIngestionStorageMetadataService storageMetadataService) {
    this.storageMetadataService = storageMetadataService;
  }

  public MainIngestionStorageMetadataService getStorageMetadataService() {
    return storageMetadataService;
  }

  public void setStoreIngestionService(KafkaStoreIngestionService storeIngestionService) {
    this.storeIngestionService = storeIngestionService;
  }

  public KafkaStoreIngestionService getStoreIngestionService() {
    return storeIngestionService;
  }

  public MainPartitionIngestionStatus getTopicPartitionIngestionStatus(String topicName, int partitionId) {
    MainTopicIngestionStatus topicIngestionStatus = getTopicIngestionStatusMap().get(topicName);
    if (topicIngestionStatus != null) {
      return topicIngestionStatus.getPartitionIngestionStatus(partitionId);
    }
    return MainPartitionIngestionStatus.NOT_EXIST;
  }

  public void setVersionPartitionToLocalIngestion(String topicName, int partitionId) {
    getTopicIngestionStatusMap().computeIfAbsent(topicName, x -> new MainTopicIngestionStatus(topicName))
        .setPartitionIngestionStatusToLocalIngestion(partitionId);
  }

  public void setVersionPartitionToIsolatedIngestion(String topicName, int partitionId) {
    getTopicIngestionStatusMap().computeIfAbsent(topicName, x -> new MainTopicIngestionStatus(topicName))
        .setPartitionIngestionStatusToIsolatedIngestion(partitionId);
  }

  public void cleanupTopicPartitionState(String topicName, int partitionId) {
    MainTopicIngestionStatus partitionIngestionStatus = getTopicIngestionStatusMap().get(topicName);
    if (partitionIngestionStatus != null) {
      partitionIngestionStatus.removePartitionIngestionStatus(partitionId);
    }
    String replicaId = Utils.getReplicaId(topicName, partitionId);
    LOGGER.info("Ingestion status removed from main process for replica: {}", replicaId);
  }

  public void cleanupTopicState(String topicName) {
    getTopicIngestionStatusMap().remove(topicName);
    LOGGER.info("Ingestion status removed from main process for topic: {}", topicName);
  }

  public long getTopicPartitionCount(String topicName) {
    MainTopicIngestionStatus topicIngestionStatus = getTopicIngestionStatusMap().get(topicName);
    if (topicIngestionStatus != null) {
      return topicIngestionStatus.getIngestingPartitionCount();
    }
    return 0;
  }

  private void setupMetricsCollection() {
    if (metricsRepository == null) {
      LOGGER.warn("No metrics repository is set up in ingestion report listener, skipping metrics collection");
      return;
    }
    heartbeatStats = new IsolatedIngestionProcessHeartbeatStats(metricsRepository);
    isolatedIngestionProcessStats = new IsolatedIngestionProcessStats(metricsRepository);
    heartbeatCheckScheduler.scheduleAtFixedRate(this::checkHeartbeatTimeout, 0, 10, TimeUnit.SECONDS);
  }

  private synchronized void tryRestartForkedProcess() {
    /**
     * Before we add timeout to client request, there might be multiple requests being blocked at the same time and
     * get responses at the same time. This might cause multiple heartbeat request think it is timing out and trigger
     * this call. Here we use synchronized modifier and add timeout checking here to make sure we only restart forked
     * process once.
     */
    if ((System.currentTimeMillis() - latestHeartbeatTimestamp) <= connectionTimeoutMs) {
      return;
    }
    LOGGER.warn(
        "Lost connection to forked ingestion process since timestamp {}, restarting forked process.",
        latestHeartbeatTimestamp);
    heartbeatStats.recordForkedProcessRestart();
    try (MainIngestionRequestClient client = new MainIngestionRequestClient(configLoader)) {
      /**
       * We need to destroy the previous isolated ingestion process first.
       * The previous isolated ingestion process might have released the port binding, but it might still taking up all
       * RocksDB storage locks and JVM memory during slow shutdown process and new forked process might fail to start
       * without necessary resources.
       */
      IsolatedIngestionUtils.destroyIsolatedIngestionProcess(ingestionBackend.getIsolatedIngestionServiceProcess());
      Process newIsolatedIngestionProcess = client.startForkedIngestionProcess(configLoader);
      ingestionBackend.setIsolatedIngestionServiceProcess(newIsolatedIngestionProcess);
      LOGGER.info("Forked process has been recovered.");
    }
    // Re-initialize the latest heartbeat timestamp.
    latestHeartbeatTimestamp = System.currentTimeMillis();
    heartbeatStats.recordHeartbeatAge(0);
    // Use async long-running task scheduler to avoid blocking periodic heartbeat jobs.
    longRunningTaskExecutor.execute(this::resumeOngoingIngestionTasks);
  }

  int resumeOngoingIngestionTasks() {
    AtomicInteger count = new AtomicInteger();
    try (MainIngestionRequestClient client = createClient()) {
      Map<String, MainTopicIngestionStatus> topicIngestionStatusMap = getTopicIngestionStatusMap();
      LOGGER.info("Start to recover ongoing ingestion tasks: {}", topicIngestionStatusMap.keySet());
      // Re-open metadata partitions in child process for all previously subscribed topics.
      topicIngestionStatusMap.keySet().forEach(client::openStorageEngine);
      // All previously subscribed topics are stored in the keySet of this topic partition map.
      topicIngestionStatusMap.forEach((topic, partitionStatus) -> {
        partitionStatus.getPartitionIngestionStatusSet().forEach((partition, status) -> {
          if (status.equals(MainPartitionIngestionStatus.ISOLATED)) {
            String replicaId = Utils.getReplicaId(topic, partition);
            try {
              client.startConsumption(topic, partition);
              LOGGER.info("Recovered ingestion task in isolated process for replica: {}", replicaId);
              count.addAndGet(1);
            } catch (Exception e) {
              LOGGER.warn("Recovery of ingestion failed for replica: {}", replicaId, e);
            }
          }
        });
      });
      LOGGER.info("Resumed {} topic partition ingestion tasks.", count.get());
    }
    return count.get();
  }

  private void checkHeartbeatTimeout() {
    long requestTimestamp = System.currentTimeMillis();
    LOGGER.info(
        "Checking heartbeat timeout at {}, latest heartbeat received: {}",
        requestTimestamp,
        latestHeartbeatTimestamp);
    if (heartbeatClient.sendHeartbeatRequest()) {
      // Update heartbeat time.
      latestHeartbeatTimestamp = System.currentTimeMillis();
      heartbeatStats.recordHeartbeatAge(0);
      LOGGER.info("Received forked process heartbeat ack at: {}", latestHeartbeatTimestamp);
    } else {
      long responseTimestamp = System.currentTimeMillis();
      heartbeatStats.recordHeartbeatAge(requestTimestamp - latestHeartbeatTimestamp);
      LOGGER.warn(
          "Heartbeat request to forked process issued at {}, failed at {}, latest successful timestamp: {}",
          responseTimestamp,
          requestTimestamp,
          latestHeartbeatTimestamp);
    }

    if (latestHeartbeatTimestamp != -1) {
      if ((requestTimestamp - latestHeartbeatTimestamp) > connectionTimeoutMs) {
        tryRestartForkedProcess();
      }
    }
  }

  private void shutdownScheduler(ExecutorService scheduler, String schedulerName) {
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
        LOGGER.info("{} scheduler has been shutdown.", schedulerName);
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }
  }

  MainIngestionRequestClient createClient() {
    return new MainIngestionRequestClient(configLoader);
  }

  public Map<String, MainTopicIngestionStatus> getTopicIngestionStatusMap() {
    return topicIngestionStatusMap;
  }

  IsolatedIngestionProcessStats getIsolatedIngestionProcessStats() {
    return isolatedIngestionProcessStats;
  }
}
