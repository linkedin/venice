package com.linkedin.venice.listener;

import com.linkedin.ddsstorage.router.lnkd.netty4.SSLInitializer;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import com.linkedin.venice.stats.AggServerQuotaTokenBucketStats;
import com.linkedin.venice.stats.AggServerQuotaUsageStats;
import com.linkedin.venice.stats.ThreadPoolStats;
import com.linkedin.venice.storage.DiskHealthCheckService;
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
  private final ThreadPoolExecutor computeExecutor;
  protected final StorageExecutionHandler storageExecutionHandler;
  private final AggServerHttpRequestStats singleGetStats;
  private final AggServerHttpRequestStats multiGetStats;
  private final AggServerHttpRequestStats computeStats;
  private final Optional<SSLEngineComponentFactory> sslFactory;
  private final Optional<ServerAclHandler> aclHandler;
  private final VerifySslHandler verifySsl = new VerifySslHandler();
  private final VeniceServerConfig serverConfig;
  private final StorageQuotaEnforcementHandler quotaEnforcer;
  AggServerQuotaUsageStats quotaUsageStats;
  AggServerQuotaTokenBucketStats quotaTokenBucketStats;

  public HttpChannelInitializer(StoreRepository storeRepository,
                                ReadOnlyStoreRepository storeMetadataRepository,
                                ReadOnlySchemaRepository schemaRepo,
                                CompletableFuture<RoutingDataRepository> routingRepository,
                                MetadataRetriever metadataRetriever,
                                MetricsRepository metricsRepository,
                                Optional<SSLEngineComponentFactory> sslFactory,
                                VeniceServerConfig serverConfig,
                                Optional<StaticAccessController> accessController,
                                DiskHealthCheckService diskHealthCheckService) {
    this.serverConfig = serverConfig;

    this.executor = getThreadPool(serverConfig.getRestServiceStorageThreadNum(), "StorageExecutionThread");
    this.computeExecutor = getThreadPool(serverConfig.getServerComputeThreadNum(), "StorageComputeThread");

    new ThreadPoolStats(metricsRepository, this.executor, "storage_execution_thread_pool");
    new ThreadPoolStats(metricsRepository, this.computeExecutor, "storage_compute_thread_pool");

    singleGetStats = new AggServerHttpRequestStats(metricsRepository, RequestType.SINGLE_GET);
    multiGetStats = new AggServerHttpRequestStats(metricsRepository, RequestType.MULTI_GET);
    computeStats = new AggServerHttpRequestStats(metricsRepository, RequestType.COMPUTE);

    storageExecutionHandler = new StorageExecutionHandler(executor, computeExecutor, storeRepository, schemaRepo,
        metadataRetriever, diskHealthCheckService);

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

    //Token Bucket Stats for a store must be initialized when that store is created
    this.quotaTokenBucketStats = new AggServerQuotaTokenBucketStats(metricsRepository, quotaEnforcer);
    storeMetadataRepository.registerStoreDataChangedListener(this.quotaTokenBucketStats);
    for (Store store : storeMetadataRepository.getAllStores()){
      this.quotaTokenBucketStats.initializeStatsForStore(store.getName());
    }
  }

  @Override
  public void initChannel(SocketChannel ch) throws Exception {

    if (sslFactory.isPresent()){
      ch.pipeline()
          .addLast(new SSLInitializer(sslFactory.get()));
    }

    StatsHandler statsHandler = new StatsHandler(singleGetStats, multiGetStats, computeStats);
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
        .addLast(new RouterRequestHttpHandler(statsHandler))
        .addLast(quotaEnforcer)
        .addLast("storageExecutionHandler", storageExecutionHandler)
        .addLast(new ErrorCatchingHandler());
  }

  private ThreadPoolExecutor getThreadPool(int threadNum, String threadNamePrefix) {
    return new ThreadPoolExecutor(threadNum, threadNum, 0, TimeUnit.MILLISECONDS,
        serverConfig.getExecutionQueue(), new DaemonThreadFactory(threadNamePrefix));
  }

}
