package com.linkedin.venice.ingestion;

import com.linkedin.venice.ingestion.channel.IngestionReportChannelInitializer;
import com.linkedin.venice.ingestion.protocol.IngestionMetricsReport;
import com.linkedin.venice.ingestion.protocol.IngestionTaskCommand;
import com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType;
import com.linkedin.venice.meta.IngestionAction;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ingestion.IngestionUtils.*;
import static java.lang.Thread.*;


/**
 * IngestionReportListener is the listener server that handles IngestionTaskReport sent from child process.
 */
public class IngestionReportListener extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(IngestionReportListener.class);
  private final ServerBootstrap bootstrap;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final int applicationPort;
  private final int ingestionServicePort;
  private final ScheduledExecutorService metricsRequestScheduler = Executors.newScheduledThreadPool(1);
  private final ScheduledExecutorService heartbeatCheckScheduler = Executors.newScheduledThreadPool(1);

  private ChannelFuture serverFuture;
  private MetricsRepository metricsRepository;
  private IngestionRequestClient metricsClient;
  private IngestionRequestClient heartbeatClient;
  private IngestionProcessStats ingestionProcessStats;
  private IngestionStorageMetadataService storageMetadataService;
  private Map<String, Set<Integer>> topicNameToPartitionSetMap = new VeniceConcurrentHashMap<>();

  private VeniceConfigLoader configLoader;
  private VeniceNotifier ingestionNotifier = null;
  private long heartbeatTime = -1;

  //TODO: move netty config to a config file
  private static int nettyBacklogSize = 1000;

  public IngestionReportListener(int applicationPort, int ingestionServicePort, InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer) {
    this.applicationPort = applicationPort;
    this.ingestionServicePort = ingestionServicePort;

    // Initialize Netty server.
    Class<? extends ServerChannel> serverSocketChannelClass = NioServerSocketChannel.class;
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup();
    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup).channel(serverSocketChannelClass)
            .childHandler(new IngestionReportChannelInitializer(this, partitionStateSerializer))
            .option(ChannelOption.SO_BACKLOG, nettyBacklogSize)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.TCP_NODELAY, true);

    heartbeatClient = new IngestionRequestClient(this.ingestionServicePort);
    metricsClient = new IngestionRequestClient(this.ingestionServicePort);
  }

  @Override
  public boolean startInner() throws Exception {
    serverFuture = bootstrap.bind(applicationPort).sync();
    logger.info("Report listener service started on port: " + applicationPort);
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

  public void setIngestionNotifier(VeniceNotifier ingestionListener) {
    this.ingestionNotifier = ingestionListener;
  }

  public VeniceNotifier getIngestionNotifier() {
    return ingestionNotifier;
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

  public void removeVersionPartitionFromIngestionMap(String topicName, int partitionId) {
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
    IngestionUtils.startForkedIngestionProcess(configLoader);
    IngestionRequestClient client = new IngestionRequestClient(this.ingestionServicePort);
    // Reset heartbeat time.
    heartbeatTime = -1;
    topicNameToPartitionSetMap.forEach((topicName, partitionSet) -> {
      partitionSet.forEach(partitionId -> {
        IngestionUtils.subscribeTopicPartition(client, topicName, partitionId);
      });
    });
    logger.info("Restart forked process completed");
  }

  private void collectIngestionServiceMetrics() {
    logger.info("Sending metrics collection request to isolated ingestion service.");
    byte[] content = new byte[0];
    HttpRequest httpRequest = metricsClient.buildHttpRequest(IngestionAction.METRIC, content);
    try {
      FullHttpResponse response = metricsClient.sendRequest(httpRequest);
      byte[] responseContent = new byte[response.content().readableBytes()];
      response.content().readBytes(responseContent);
      IngestionMetricsReport metricsReport = deserializeIngestionMetricsReport(responseContent);
      logger.info("Collecting " + metricsReport.aggregatedMetrics.size() + " metrics from isolated ingestion service.");
      ingestionProcessStats.updateMetricMap(metricsReport.aggregatedMetrics);
      // Update heartbeat time.
      heartbeatTime = System.currentTimeMillis();
      // FullHttpResponse is a reference-counted object that requires explicit de-allocation.
      response.release();
    } catch (Exception e) {
      logger.warn("Unable to collect metrics from ingestion service", e);
    }
  }

  private void checkHeartbeatTimeout() {
    long currentTimeMillis = System.currentTimeMillis();
    logger.info("Checking heartbeat timeout at " + currentTimeMillis + ", current heartbeat: " + heartbeatTime);
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.HEARTBEAT.getValue();
    ingestionTaskCommand.topicName = "";
    byte[] content = serializeIngestionTaskCommand(ingestionTaskCommand);
    HttpRequest httpRequest = heartbeatClient.buildHttpRequest(IngestionAction.COMMAND, content);
    try {
      FullHttpResponse response = heartbeatClient.sendRequest(httpRequest);
      if (response.status().equals(HttpResponseStatus.OK)) {
        // Update heartbeat time.
        heartbeatTime = System.currentTimeMillis();
      }
      // FullHttpResponse is a reference-counted object that requires explicit de-allocation.
      response.release();
    } catch (Exception e) {
      logger.warn("Unable to get heartbeat from ingestion service", e);
    }

    if (heartbeatTime != -1) {
      if ((currentTimeMillis - heartbeatTime) > 30000) {
        logger.warn("Lost connection to forked ingestion process since timestamp, restarting forked process.");
        restartForkedProcess();
      }
    }
  }


}
