package com.linkedin.venice.listener;

import com.linkedin.ddsstorage.router.lnkd.netty4.SSLInitializer;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import com.linkedin.venice.stats.AggServerQuotaTokenBucketStats;
import com.linkedin.venice.stats.AggServerQuotaUsageStats;
import com.linkedin.venice.utils.Utils;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.timeout.IdleStateHandler;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.log4j.Logger;


public class HttpChannelInitializer extends ChannelInitializer<SocketChannel> {
  private static final Logger LOGGER = Logger.getLogger(HttpChannelInitializer.class);

  private final StorageExecutionHandler requestHandler;
  private final AggServerHttpRequestStats singleGetStats;
  private final AggServerHttpRequestStats multiGetStats;
  private final AggServerHttpRequestStats computeStats;
  private final Optional<SSLEngineComponentFactory> sslFactory;
  private final Optional<ServerAclHandler> aclHandler;
  private final Optional<ServerStoreAclHandler> storeAclHandler;
  private final VerifySslHandler verifySsl = new VerifySslHandler();
  private final VeniceServerConfig serverConfig;
  private final ReadQuotaEnforcementHandler quotaEnforcer;
  AggServerQuotaUsageStats quotaUsageStats;
  AggServerQuotaTokenBucketStats quotaTokenBucketStats;

  public HttpChannelInitializer(ReadOnlyStoreRepository storeMetadataRepository,
                                CompletableFuture<RoutingDataRepository> routingRepository,
                                MetricsRepository metricsRepository,
                                Optional<SSLEngineComponentFactory> sslFactory,
                                VeniceServerConfig serverConfig,
                                Optional<StaticAccessController> routerAccessController,
                                Optional<DynamicAccessController> storeAccessController,
                                StorageExecutionHandler requestHandler) {
    this.serverConfig = serverConfig;
    this.requestHandler = requestHandler;

    boolean isKeyValueProfilingEnabled = serverConfig.isKeyValueProfilingEnabled();

    this.singleGetStats = new AggServerHttpRequestStats(metricsRepository, RequestType.SINGLE_GET, isKeyValueProfilingEnabled);
    this.multiGetStats = new AggServerHttpRequestStats(metricsRepository, RequestType.MULTI_GET, isKeyValueProfilingEnabled);
    this.computeStats = new AggServerHttpRequestStats(metricsRepository, RequestType.COMPUTE, isKeyValueProfilingEnabled);

    if (serverConfig.isComputeFastAvroEnabled()) {
      LOGGER.info("Fast avro for compute is enabled");
    }

    this.sslFactory = sslFactory;
    this.storeAclHandler = storeAccessController.isPresent()
        ? Optional.of(new ServerStoreAclHandler(storeAccessController.get(), storeMetadataRepository)) : Optional.empty();
    /**
     * If the store-level access handler is present, we don't want to fail fast if the access gets denied by {@link ServerAclHandler}.
     */
    boolean aclHandlerFailOnAccessRejection = !this.storeAclHandler.isPresent();
    this.aclHandler = routerAccessController.isPresent()
        ? Optional.of(new ServerAclHandler(routerAccessController.get(), aclHandlerFailOnAccessRejection))
        : Optional.empty();

    String nodeId = Utils.getHelixNodeIdentifier(serverConfig.getListenerPort());
    this.quotaUsageStats = new AggServerQuotaUsageStats(metricsRepository);
    if (serverConfig.isQuotaEnforcementEnabled()) {
      this.quotaEnforcer = new ReadQuotaEnforcementHandler(serverConfig.getNodeCapacityInRcu(), storeMetadataRepository,
          routingRepository, nodeId, quotaUsageStats);

      //Token Bucket Stats for a store must be initialized when that store is created
      this.quotaTokenBucketStats = new AggServerQuotaTokenBucketStats(metricsRepository, quotaEnforcer);
      storeMetadataRepository.registerStoreDataChangedListener(quotaTokenBucketStats);
      for (Store store : storeMetadataRepository.getAllStores()) {
        this.quotaTokenBucketStats.initializeStatsForStore(store.getName());
      }
    } else {
      this.quotaEnforcer = null;
    }
  }

  /*
    Test only
   */
  protected ReadQuotaEnforcementHandler getQuotaEnforcer() {
    return quotaEnforcer;
  }

  @Override
  public void initChannel(SocketChannel ch) {
    if (sslFactory.isPresent()){
      ch.pipeline()
          .addLast(new SSLInitializer(sslFactory.get()));
    }

    StatsHandler statsHandler = new StatsHandler(singleGetStats, multiGetStats, computeStats);
    ch.pipeline()
        .addLast(statsHandler)
        .addLast(new HttpServerCodec())
        .addLast(new HttpObjectAggregator(serverConfig.getMaxRequestSize()))
        .addLast(new OutboundHttpWrapperHandler(statsHandler))
        .addLast(new IdleStateHandler(0, 0, serverConfig.getNettyIdleTimeInSeconds()));

    if (sslFactory.isPresent()){
      ch.pipeline().addLast(verifySsl);
      if (aclHandler.isPresent()) {
        ch.pipeline().addLast(aclHandler.get());
      }
      /**
       * {@link #storeAclHandler} if present must come after {@link #aclHandler}
       */
      if (storeAclHandler.isPresent()) {
        ch.pipeline().addLast(storeAclHandler.get());
      }
    }

    ch.pipeline()
        .addLast(new RouterRequestHttpHandler(statsHandler,
            serverConfig.isComputeFastAvroEnabled(),
            serverConfig.getStoreToEarlyTerminationThresholdMSMap()));

    if (quotaEnforcer != null) {
      ch.pipeline().addLast(quotaEnforcer);
    }

    ch.pipeline()
        .addLast(requestHandler)
        .addLast(new ErrorCatchingHandler());
  }
}
