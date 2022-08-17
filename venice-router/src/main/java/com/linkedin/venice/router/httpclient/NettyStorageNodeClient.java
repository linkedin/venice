package com.linkedin.venice.router.httpclient;

import static com.linkedin.venice.HttpConstants.*;

import com.linkedin.ddsstorage.base.misc.Msg;
import com.linkedin.ddsstorage.base.monitoring.CallTracker;
import com.linkedin.ddsstorage.base.monitoring.CallTrackerImpl;
import com.linkedin.ddsstorage.base.monitoring.NullCallTracker;
import com.linkedin.ddsstorage.consts.QOS;
import com.linkedin.ddsstorage.netty4.handlers.HttpClientResponseHandler;
import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpResponse;
import com.linkedin.ddsstorage.netty4.misc.BasicHttpResponse;
import com.linkedin.ddsstorage.netty4.pool.BasicDnsResolver;
import com.linkedin.ddsstorage.netty4.pool.ChannelPoolManager;
import com.linkedin.ddsstorage.netty4.pool.ChannelPoolManagerImpl;
import com.linkedin.ddsstorage.netty4.pool.ChannelPoolResolver;
import com.linkedin.ddsstorage.netty4.pool.Http2AwareChannelPoolFactory;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ResolveAllBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.util.AsciiString;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.net.SocketAddress;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StringBuilderFormattable;


/**
 * The netty client will maintain a channel pool for each SN; and all the events for a channel will be
 * handled by the same thread from inbound to outbound to avoid context switch as much as possible;
 * when the response of a channel returns, this event will also be handled by the thread that sends the
 * request. In a word, the events of a channel will always be in the same event loop.
 *
 * Each event loop has exactly one thread tied to it, so that there is no synchronization work between threads,
 * because different threads won't handle events for the same channel.
 *
 * The performance of the netty client can be scaled up by increasing the number of event loop (#threads).
 */
public class NettyStorageNodeClient implements StorageNodeClient {
  private static final Logger logger = LogManager.getLogger(NettyStorageNodeClient.class);
  private static final boolean loggerDebugEnabled = logger.isDebugEnabled();
  private static final AsciiString X_QUEUE_NAME = AsciiString.of("X-Queue-Name");
  private static final AsciiString X_QOS = AsciiString.of("X-QOS");
  private final ByteToMessageDecoder.Cumulator cumulator = ByteToMessageDecoder.MERGE_CUMULATOR;

  private final ChannelPoolManager manager;
  private final String scheme;

  private final RouterStats<AggRouterHttpRequestStats> routerStats;

  private final ChannelHealthChecker healthChecker = ch -> {
    if (ch.isActive()) {
      return ch.eventLoop().newSucceededFuture(true);
    } else {
      return ch.eventLoop().newSucceededFuture(false);
    }
  };

  private MultithreadEventLoopGroup eventLoopGroup;

  public NettyStorageNodeClient(
      VeniceRouterConfig config,
      Optional<SSLEngineComponentFactory> sslFactoryForRequests,
      RouterStats<AggRouterHttpRequestStats> routerStats,
      MultithreadEventLoopGroup workerEventLoopGroup,
      Class<? extends Channel> channelClass) {
    this.scheme = config.isSslToStorageNodes() ? HTTPS_PREFIX : HTTP_PREFIX;
    this.routerStats = routerStats;
    this.eventLoopGroup = workerEventLoopGroup;

    Bootstrap bootstrap =
        new ResolveAllBootstrap(NullCallTracker.INSTANCE, NullCallTracker.INSTANCE).channel(channelClass)
            .handler(new ChannelInitializer<Channel>() {
              @Override
              protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();

                pipeline.addLast(new HttpObjectAggregator(config.getNettyClientMaxAggregatedObjectLength(), true))
                    .addLast(new HttpClientResponseHandler());
              }
            });
    if (config.isSslToStorageNodes()) {
      SslContext sslContext = new JdkSslContext(sslFactoryForRequests.get().getSSLContext(), true, ClientAuth.NONE);
      bootstrap.attr(Http2AwareChannelPoolFactory.SSL_CONTEXT, sslContext);
    }

    ConcurrentMap<SocketAddress, CallTracker> healthCallTrackerMap = new VeniceConcurrentHashMap<>();
    ChannelPoolResolver resolver = new BasicDnsResolver();
    Http2AwareChannelPoolFactory factory = new Http2AwareChannelPoolFactory(
        bootstrap,
        config.getNettyClientChannelPoolAcquireTimeoutMs(),
        config.getNettyClientChannelPoolMinConnections(),
        config.getNettyClientChannelPoolMaxConnections(),
        config.getNettyClientChannelPoolMaxPendingAcquires(),
        true,
        config.getNettyClientChannelPoolHealthCheckIntervalMs(),
        healthChecker,
        socketAddress -> healthCallTrackerMap.computeIfAbsent(socketAddress, ignored -> new CallTrackerImpl()));

    manager = new ChannelPoolManagerImpl(
        eventLoopGroup,
        factory,
        resolver,
        config.getNettyClientChannelPoolMaxPendingAcquires());
  }

  @Override
  public void start() {
    logger.info("Nothing to do during start");
  }

  @Override
  public void close() {
    try {
      manager.closeAll().sync().getNow();
    } catch (InterruptedException e) {
      throw new VeniceException("Failed to close the ConnectionManager", e);
    }
  }

  @Override
  public void query(
      Instance host,
      VenicePath path,
      Consumer<PortableHttpResponse> completedCallBack,
      Consumer<Throwable> failedCallBack,
      BooleanSupplier cancelledCallBack,
      long queryStartTimeInNS) throws RouterException {
    String storeName = path.getStoreName();
    String hostAndPort = host.getHost() + ":" + host.getPort();
    String address = this.scheme + hostAndPort + "/" + path.getLocation();
    BasicFullHttpRequest request = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        path.getHttpMethod(),
        address,
        path.getRequestBody(),
        false,
        System.currentTimeMillis(),
        System.nanoTime());
    request.headers()
        .set(HttpHeaderNames.HOST, hostAndPort)
        .set(HttpHeaderNames.CONTENT_LENGTH, request.content().readableBytes());
    path.setupVeniceHeaders((k, v) -> request.headers().set(k, v));
    String queueName = request.headers().get(X_QUEUE_NAME, "DEFAULT");
    QOS qos = Optional.ofNullable(request.headers().get(X_QOS)).map(QOS::valueOf).orElse(QOS.NORMAL);
    Promise<FullHttpResponse> responsePromise = ImmediateEventExecutor.INSTANCE.newPromise();

    AggRouterHttpRequestStats stats = routerStats.getStatsByType(path.getRequestType());

    Future<Channel> future = manager.acquire(hostAndPort, queueName, qos);
    stats.recordNettyClientAcquireChannelLatency(storeName, LatencyUtils.getLatencyInMS(queryStartTimeInNS));

    future.addListener((Future<Channel> channelFuture) -> {
      if (channelFuture.isSuccess()) {
        Consumer<Object> responseConsumer = new Consumer<Object>() {
          private HttpResponse _response;
          private ByteBuf _content = Unpooled.EMPTY_BUFFER;
          private CompositeByteBuf _composite = null;
          ByteBufAllocator alloc = channelFuture.get().alloc();

          @Override
          public void accept(Object o) {
            if (o instanceof Throwable) {
              channelFuture.getNow().close().addListener((ChannelFuture closeFuture) -> {
                manager.release(closeFuture.channel());
              });
              _content.release();
              _content = null;
              failedCallBack.accept((Throwable) o);
              responsePromise.setFailure((Throwable) o);
              return;
            }
            if (responsePromise.isDone()) {
              return;
            }
            if (o instanceof HttpResponse) {
              stats.recordNettyClientFirstResponseLatency(storeName, LatencyUtils.getLatencyInMS(queryStartTimeInNS));
              HttpResponse response = (HttpResponse) o;
              _response = new BasicHttpResponse(request, response.status(), response.headers());
            }
            if (o instanceof HttpContent) {
              ByteBuf newContent = ((HttpContent) o).content();
              if (newContent.isReadable()) {
                if (_content == Unpooled.EMPTY_BUFFER) {
                  _content = newContent.retain();
                } else {
                  // use netty's recommended way of accumulating
                  _content = cumulator.cumulate(alloc, _content, newContent);
                }
              }
            }
            if (o instanceof LastHttpContent) {
              stats.recordNettyClientLastResponseLatency(storeName, LatencyUtils.getLatencyInMS(queryStartTimeInNS));
              LastHttpContent last = (LastHttpContent) o;
              FullHttpResponse fullHttpResponse =
                  new BasicFullHttpResponse(_response, _response.headers(), last.trailingHeaders(), _content);
              _content = null;
              HttpUtil.setContentLength(fullHttpResponse, fullHttpResponse.content().readableBytes());
              if (HttpUtil.isKeepAlive(fullHttpResponse)) {
                manager.release(channelFuture.getNow());
              } else {
                channelFuture.getNow()
                    .close()
                    .addListener((ChannelFuture closeFuture) -> manager.release(closeFuture.channel()));
              }
              if (responsePromise.trySuccess(fullHttpResponse)) {
                NettyClientPortableHttpResponse portableResponse =
                    new NettyClientPortableHttpResponse(fullHttpResponse);
                completedCallBack.accept(portableResponse);
              } else {
                // TODO: Determine what to do here...?
                fullHttpResponse.release();
              }
            }
          }
        };
        if (loggerDebugEnabled) {
          logger.debug(
              "Channel: " + channelFuture.getNow() + " className: " + channelFuture.getNow()
                  + channelFuture.getNow().getClass().getSimpleName()
                  + Msg.make(
                      channelFuture.getNow().pipeline(),
                      pipeline -> (StringBuilderFormattable) buffer -> pipeline.iterator()
                          .forEachRemaining(
                              entry -> buffer.append("\n  Name: ")
                                  .append(entry.getKey())
                                  .append("  Handler: ")
                                  .append(entry.getValue().getClass().getSimpleName()))));
        }
        channelFuture.getNow()
            .writeAndFlush(new FullHttpRequestResponseConsumer(request, responseConsumer))
            .addListener((ChannelFuture writeFuture) -> {
              if (!writeFuture.isSuccess()) {
                writeFuture.channel()
                    .close()
                    .addListener((ChannelFuture closeFuture) -> manager.release(closeFuture.channel()));
                failedCallBack.accept(writeFuture.cause());
                responsePromise.setFailure(writeFuture.cause());
              }
              if (responsePromise.isCancelled()) {
                cancelledCallBack.getAsBoolean();
              }
            });
      } else {
        failedCallBack.accept(channelFuture.cause());
        responsePromise.setFailure(channelFuture.cause());
      }
    });
  }

  private static class NettyClientPortableHttpResponse implements PortableHttpResponse {
    private final FullHttpResponse response;

    private NettyClientPortableHttpResponse(FullHttpResponse response) {
      this.response = response;
    }

    @Override
    public int getStatusCode() {
      return response.status().code();
    }

    @Override
    public ByteBuf getContentInByteBuf() {
      // Can not directly return the `content()` as it needs to explicitly release-ed after returning.
      ByteBuf byteBuf = response.content();
      byte[] results = new byte[byteBuf.readableBytes()];
      byteBuf.readBytes(results);
      byteBuf.release();
      return Unpooled.wrappedBuffer(results);
    }

    @Override
    public boolean containsHeader(String headerName) {
      return response.headers().contains(headerName);
    }

    @Override
    public String getFirstHeader(String headerName) {
      return response.headers().get(headerName);
    }
  }

  static final class FullHttpRequestResponseConsumer extends DefaultFullHttpRequest
      implements HttpClientResponseHandler.ResponseConsumer {
    private final Consumer<Object> _responseConsumer;

    FullHttpRequestResponseConsumer(FullHttpRequest request, Consumer<Object> responseConsumer) {
      super(
          request.protocolVersion(),
          request.method(),
          request.uri(),
          request.content(),
          request.headers(),
          request.trailingHeaders());
      _responseConsumer = responseConsumer;
    }

    @Override
    public Consumer<Object> responseConsumer() {
      return _responseConsumer;
    }

    @Override
    public FullHttpRequest copy() {
      return new FullHttpRequestResponseConsumer(super.copy(), responseConsumer());
    }

    @Override
    public FullHttpRequest duplicate() {
      return new FullHttpRequestResponseConsumer(super.duplicate(), responseConsumer());
    }

    @Override
    public FullHttpRequest retainedDuplicate() {
      return new FullHttpRequestResponseConsumer(super.retainedDuplicate(), responseConsumer());
    }

    @Override
    public FullHttpRequest replace(ByteBuf content) {
      return new FullHttpRequestResponseConsumer(super.replace(content), responseConsumer());
    }
  }
}
