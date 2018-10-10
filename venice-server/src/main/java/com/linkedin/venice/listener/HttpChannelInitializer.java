package com.linkedin.venice.listener;

import com.linkedin.ddsstorage.router.lnkd.netty4.SSLInitializer;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import com.linkedin.venice.stats.AggServerQuotaTokenBucketStats;
import com.linkedin.venice.stats.AggServerQuotaUsageStats;
import com.linkedin.venice.stats.ThreadPoolStats;
import com.linkedin.venice.storage.MetadataRetriever;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.queues.FairBlockingQueue;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.timeout.IdleStateHandler;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class HttpChannelInitializer extends ChannelInitializer<SocketChannel> {

  private final ThreadPoolExecutor executor;
  protected final StorageExecutionHandler storageExecutionHandler;
  private final AggServerHttpRequestStats singleGetStats, multiGetStats;
  private final Optional<SSLEngineComponentFactory> sslFactory;
  private final Optional<ServerAclHandler> aclHandler;
  private final VerifySslHandler verifySsl = new VerifySslHandler();
  private final VeniceServerConfig serverConfig;
  private final StorageQuotaEnforcementHandler quotaEnforcer;
  AggServerQuotaUsageStats quotaUsageStats;
  AggServerQuotaTokenBucketStats quotaTokenBucketStats;

  public HttpChannelInitializer(StoreRepository storeRepository, ReadOnlyStoreRepository storeMetadataRepository, CompletableFuture<RoutingDataRepository> routingRepository, MetadataRetriever metadataRetriever, MetricsRepository metricsRepository,
      Optional<SSLEngineComponentFactory> sslFactory, VeniceServerConfig serverConfig,
      Optional<StaticAccessController> accessController) {
    this.serverConfig = serverConfig;

    BlockingQueue<Runnable> executorQueue;
    if (serverConfig.isFairStorageExecutionQueue()) {
      executorQueue = new FairBlockingQueue<>();
    } else {
      executorQueue = new LinkedBlockingQueue<>();
    }
    this.executor = new ThreadPoolExecutor(serverConfig.getRestServiceStorageThreadNum(),
        serverConfig.getRestServiceStorageThreadNum(),
        0, TimeUnit.MILLISECONDS,
        executorQueue, new DaemonThreadFactory("StorageExecutionThread"));
    new ThreadPoolStats(metricsRepository, this.executor, "storage_execution_thread_pool");

    singleGetStats = new AggServerHttpRequestStats(metricsRepository, RequestType.SINGLE_GET);
    multiGetStats = new AggServerHttpRequestStats(metricsRepository, RequestType.MULTI_GET);

    storageExecutionHandler = new StorageExecutionHandler(executor, storeRepository, metadataRetriever, serverConfig.getDataBasePath());

    this.sslFactory = sslFactory;
    this.aclHandler = accessController.isPresent()
        ? Optional.of(new ServerAclHandler(accessController.get()))
        : Optional.empty();

    String nodeId = Utils.getHelixNodeIdentifier(serverConfig.getListenerPort());
    this.quotaUsageStats = new AggServerQuotaUsageStats(metricsRepository);
    this.quotaEnforcer = new StorageQuotaEnforcementHandler(serverConfig.getNodeCapacityInRcu(), storeMetadataRepository, routingRepository,
        nodeId, quotaUsageStats);
    if (serverConfig.isQuotaEnforcementDisabled()) {
      this.quotaEnforcer.disableEnforcement();
    }
    this.quotaTokenBucketStats = new AggServerQuotaTokenBucketStats(metricsRepository, quotaEnforcer);
  }

  @Override
  public void initChannel(SocketChannel ch) throws Exception {

    if (sslFactory.isPresent()){
      ch.pipeline()
          .addLast(new SSLInitializer(sslFactory.get()));
    }

    StatsHandler statsHandler = new StatsHandler(singleGetStats, multiGetStats);
    ch.pipeline().addLast(statsHandler)
        .addLast(new HttpServerCodec())
        .addLast(new HttpObjectAggregator(serverConfig.getMaxRequestSize()))
        .addLast(new OutboundHttpWrapperHandler(statsHandler))
        .addLast(new IdleStateHandler(0, 0, serverConfig.getNettyIdleTimeInSeconds()));

    if (sslFactory.isPresent()){
      ch.pipeline().addLast(verifySsl);
      if (aclHandler.isPresent()) {
        ch.pipeline().addLast(aclHandler.get());
      }
    }
    ch.pipeline()
        .addLast(new GetRequestHttpHandler(statsHandler))
        .addLast(quotaEnforcer)
        .addLast("storageExecutionHandler", storageExecutionHandler)
        .addLast(new ErrorCatchingHandler());
  }

}
