package com.linkedin.venice.listener;

import com.linkedin.ddsstorage.router.lnkd.netty4.SSLInitializer;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import com.linkedin.venice.utils.DaemonThreadFactory;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.timeout.IdleStateHandler;
import io.tehuti.metrics.MetricsRepository;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class HttpChannelInitializer extends ChannelInitializer<SocketChannel> {

  private final ExecutorService executor;
  protected final StorageExecutionHandler storageExecutionHandler;
  private final AggServerHttpRequestStats singleGetStats;
  private final AggServerHttpRequestStats multiGetStats;
  private final Optional<SSLEngineComponentFactory> sslFactory;
  private final VerifySslHandler verifySsl = new VerifySslHandler();
  private final VeniceServerConfig serverConfig;

  public HttpChannelInitializer(StoreRepository storeRepository, OffsetManager offsetManager,
      MetricsRepository metricsRepository, Optional<SSLEngineComponentFactory> sslFactory,
      VeniceServerConfig serverConfig) {
    this.serverConfig = serverConfig;

    this.executor = Executors.newFixedThreadPool(
        serverConfig.getRestServiceStorageThreadNum(),
        new DaemonThreadFactory("StorageExecutionThread"));

    singleGetStats = new AggServerHttpRequestStats(metricsRepository, RequestType.SINGLE_GET);
    multiGetStats = new AggServerHttpRequestStats(metricsRepository, RequestType.MULTI_GET);

    storageExecutionHandler = new StorageExecutionHandler(executor,
        storeRepository,
        offsetManager);

    this.sslFactory = sslFactory;
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
      ch.pipeline()
          .addLast(verifySsl);
    }
    ch.pipeline()
        .addLast(new GetRequestHttpHandler(statsHandler))
        .addLast("storageExecutionHandler", storageExecutionHandler)
        .addLast(new ErrorCatchingHandler());
  }

}
