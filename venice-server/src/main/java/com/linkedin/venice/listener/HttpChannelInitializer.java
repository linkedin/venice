package com.linkedin.venice.listener;

import com.linkedin.ddsstorage.router.lnkd.netty4.SSLInitializer;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
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
                                Optional<StaticAccessController> accessController,
                                StorageExecutionHandler requestHandler) {
    this.serverConfig = serverConfig;
    this.requestHandler = requestHandler;

    this.singleGetStats = new AggServerHttpRequestStats(metricsRepository, RequestType.SINGLE_GET);
    this.multiGetStats = new AggServerHttpRequestStats(metricsRepository, RequestType.MULTI_GET);
    this.computeStats = new AggServerHttpRequestStats(metricsRepository, RequestType.COMPUTE);

    if (serverConfig.isComputeFastAvroEnabled()) {
      LOGGER.info("Fast avro for compute is enabled");
    }

    this.sslFactory = sslFactory;
    this.aclHandler = accessController.isPresent()
        ? Optional.of(new ServerAclHandler(accessController.get()))
        : Optional.empty();

    String nodeId = Utils.getHelixNodeIdentifier(serverConfig.getListenerPort());
    this.quotaUsageStats = new AggServerQuotaUsageStats(metricsRepository);
    this.quotaEnforcer = new ReadQuotaEnforcementHandler(
        serverConfig.getNodeCapacityInRcu(), storeMetadataRepository, routingRepository, nodeId, quotaUsageStats);
    if (serverConfig.isQuotaEnforcementDisabled()) {
      this.quotaEnforcer.disableEnforcement();
    }

    //Token Bucket Stats for a store must be initialized when that store is created
    this.quotaTokenBucketStats = new AggServerQuotaTokenBucketStats(metricsRepository, quotaEnforcer);
    storeMetadataRepository.registerStoreDataChangedListener(quotaTokenBucketStats);
    for (Store store : storeMetadataRepository.getAllStores()) {
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
    }

    ch.pipeline()
        .addLast(new RouterRequestHttpHandler(statsHandler, serverConfig.isComputeFastAvroEnabled()))
        .addLast(quotaEnforcer)
        .addLast(requestHandler)
        .addLast(new ErrorCatchingHandler());
  }
}
