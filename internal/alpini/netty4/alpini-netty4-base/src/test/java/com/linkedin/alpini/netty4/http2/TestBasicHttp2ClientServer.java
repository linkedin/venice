package com.linkedin.alpini.netty4.http2;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.concurrency.ScheduledExecutorService;
import com.linkedin.alpini.base.monitoring.NullCallTracker;
import com.linkedin.alpini.netty4.handlers.BasicHttpObjectAggregator;
import com.linkedin.alpini.netty4.handlers.HttpClientResponseHandler;
import com.linkedin.alpini.netty4.handlers.Log4J2FrameLogger;
import com.linkedin.alpini.netty4.handlers.RateLimitConnectHandler;
import com.linkedin.alpini.netty4.handlers.SimpleChannelInitializer;
import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicFullHttpResponse;
import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import com.linkedin.alpini.netty4.pool.Http2AwareChannelPool;
import com.linkedin.alpini.netty4.pool.ManagedChannelPool;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ResolveAllBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.handler.codec.PrematureChannelClosureException;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpClientUpgradeHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.Http2ClientUpgradeCodec;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2MultiplexCodec;
import io.netty.handler.codec.http2.Http2MultiplexCodecBuilder;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2SettingsFrame;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.net.ssl.SSLException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 4/19/18.
 */
public class TestBasicHttp2ClientServer {
  static final Logger _log = LogManager.getLogger(TestBasicHttp2ClientServer.class);

  SslContext _clientSslContext = initClientSllContext();
  private final LoggingHandler _loggingHandler = new LoggingHandler(LogLevel.DEBUG);
  private final Http2FrameLogger _clientFrameLogger = new Log4J2FrameLogger(Level.DEBUG, "client");
  private EpollEventLoopGroup _group;
  private ScheduledExecutorService _executorService;

  private final Http2MultiplexCodecBuilder http2MultiplexCodecBuilder =
      Http2MultiplexCodecBuilder.forClient(new ChannelInitializer<Channel>() {
        @Override
        protected void initChannel(Channel ch) throws Exception {
          throw new IllegalStateException("Server Push not supported");
        }
      }).initialSettings(Http2Settings.defaultSettings()).frameLogger(_clientFrameLogger);

  @BeforeClass(groups = "unit")
  public void beforeClass() {
    _group = new EpollEventLoopGroup();
    _executorService = Executors.newSingleThreadScheduledExecutor();
  }

  @AfterClass(groups = "unit")
  public void afterClass() {
    if (_group != null) {
      _group.shutdownGracefully();
    }
    if (_executorService != null) {
      _executorService.shutdownNow();
    }
  }

  @Test(enabled = false)
  public void testUsingPublicCDNTestFile() throws InterruptedException {
    testGet("1906714720.rsc.cdn77.org", 443, "/img/cdn77-test-563kb.jpg");
  }

  @Test(enabled = true)
  public void testUsingLocalSkeletonServerNoAPLN() throws InterruptedException, SSLException {
    testUsingLocalSkeletonServer(SSLContextBuilder.makeServerContext(0, 0, false));
  }

  @Test(enabled = true)
  public void testUsingLocalSkeletonServerWithALPN() throws InterruptedException, SSLException {
    testUsingLocalSkeletonServer(SSLContextBuilder.makeServerContext(0, 0));
  }

  private void testUsingLocalSkeletonServer(SslContext serverSslContext) throws InterruptedException, SSLException {
    ChannelFuture serverFuture = SkeletonHttp2Server.setupBootstrap(
        new ServerBootstrap().group(_group).channel(EpollServerSocketChannel.class),
        serverSslContext,
        new SimpleChannelInitializer<Channel>() {
          @Override
          protected void initChannel(Channel ch) throws Exception {
            addAfter(ch, new BasicHttpObjectAggregator(16 * 1024), new SimpleChannelInboundHandler<BasicHttpRequest>() {
              @Override
              protected void channelRead0(ChannelHandlerContext ctx, BasicHttpRequest msg) throws Exception {
                assert msg instanceof BasicFullHttpRequest;
                BasicFullHttpResponse response = new BasicFullHttpResponse(
                    msg,
                    HttpResponseStatus.OK,
                    Unpooled.copiedBuffer(LOREM_IPSUM, StandardCharsets.US_ASCII));
                HttpUtil.setContentLength(response, response.content().readableBytes());
                ctx.writeAndFlush(response);
              }
            });
          }
        }).bind(0).sync();
    try {
      InetSocketAddress address = (InetSocketAddress) serverFuture.channel().localAddress();

      testGet("localhost", address.getPort(), "/foo");

    } finally {
      serverFuture.channel().close();
    }
  }

  private SslContext initClientSllContext() {
    try {
      return SSLContextBuilder.makeClientContext(0, 0);
    } catch (SSLException e) {
      throw new Error(e);
    }
  }

  private Bootstrap clientBootstrap(EpollEventLoopGroup group) {
    return new ResolveAllBootstrap(NullCallTracker.INSTANCE, NullCallTracker.INSTANCE).group(group)
        .channel(EpollSocketChannel.class)
        .option(ChannelOption.SO_KEEPALIVE, true);
  }

  private ChannelPool poolFor(
      Bootstrap bootstrap,
      RateLimitConnectHandler rateLimitConnectHandler,
      InetSocketAddress address,
      int maxConnections) {

    class Pool extends FixedChannelPool implements ManagedChannelPool {
      public Pool(
          Bootstrap bootstrap,
          ChannelPoolHandler handler,
          ChannelHealthChecker healthCheck,
          AcquireTimeoutAction action,
          long acquireTimeoutMillis,
          int maxConnections,
          int maxPendingAcquires) {
        super(bootstrap, handler, healthCheck, action, acquireTimeoutMillis, maxConnections, maxPendingAcquires);
      }

      @Override
      public String name() {
        return bootstrap().config().remoteAddress().toString();
      }

      @Override
      public ChannelPoolHandler handler() {
        return super.handler();
      }

      @Override
      public int getConnectedChannels() {
        return 0; // dummy value since this is never called in this test
      }

      @Override
      public boolean isHealthy() {
        return true; // dummy value since this is never called in this test
      }

      @Override
      public Future<Void> closeFuture() {
        return ImmediateEventExecutor.INSTANCE.newPromise(); // dummy value since this is never called in this test
      }

      @Override
      public boolean isClosing() {
        return false; // dummy value since this is never called in this test
      }

      @Override
      public int getMaxConnections() {
        return 0;
      }

      @Override
      public int getMaxPendingAcquires() {
        return 0;
      }

      @Override
      public int getAcquiredChannelCount() {
        return 0;
      }

      @Override
      public int getPendingAcquireCount() {
        return 0;
      }
    }

    return new Http2AwareChannelPool(new Pool(bootstrap.clone().remoteAddress(address), new ChannelPoolHandler() {
      @Override
      public void channelReleased(Channel ch) throws Exception {
        _log.error("channel released: {}", ch);
      }

      @Override
      public void channelAcquired(Channel ch) throws Exception {
        _log.error("channel acquired: {}", ch);
      }

      @Override
      public void channelCreated(Channel ch) throws Exception {
        _log.error("channel created: {} {}", ch);
      }
    }, new ChannelHealthChecker() {
      @Override
      public Future<Boolean> isHealthy(Channel channel) {
        _log.error("check healthy {}", channel);
        // A channel isn't healthy until after SSL handshake is done!

        ChannelPipeline pipeline = channel.pipeline();
        SslHandler sslHandler = pipeline.get(SslHandler.class);
        if (sslHandler.handshakeFuture().isSuccess()) {
          return channel.eventLoop().newSucceededFuture(true);
        }
        Promise<Boolean> promise = channel.eventLoop().newPromise();
        sslHandler.handshakeFuture().addListener((Future<Channel> f) -> {
          _log.error("handshake complete {}", channel);
          if (f.isSuccess()) {
            _executorService.schedule(() -> promise.setSuccess(true), 100, TimeUnit.MILLISECONDS);
          } else {
            promise.setFailure(f.cause());
          }
        });
        return promise;
      }
    }, FixedChannelPool.AcquireTimeoutAction.FAIL, 100000, maxConnections, 1000) {
      final AttributeKey<Promise<Void>> _initKey = AttributeKey.valueOf(TestBasicHttp2ClientServer.class, "initKey");

      ChannelInitializer<Channel> _initializer = new ChannelInitializer<Channel>() {
        private final ChannelHandler _http11Initializer = new ChannelInitializer<Channel>() {
          @Override
          protected void initChannel(Channel ch) throws Exception {
            initHttp1(ch.pipeline());
          }
        };

        private void initHttp1(ChannelPipeline pipeline) {
          HttpClientCodec httpCodec = new HttpClientCodec();
          Http2ClientUpgradeCodec upgradeCodec = new Http2ClientUpgradeCodec(http2MultiplexCodecBuilder.build());
          HttpClientUpgradeHandler upgradeHandler = new HttpClientUpgradeHandler(httpCodec, upgradeCodec, 65536);
          pipeline.addLast(httpCodec, _loggingHandler, upgradeHandler, new HttpClientResponseHandler());
        }

        private Optional<ChannelHandler> constructAlpnInitializer(Channel ch) {
          try {
            return Optional.of(new ApplicationProtocolNegotiationHandler(ApplicationProtocolNames.HTTP_1_1) {
              @Override
              protected void configurePipeline(ChannelHandlerContext ctx, String protocol) throws Exception {
                _log.error("configurePipeline: {} {}", ctx.channel(), protocol);
                ChannelPipeline pipeline = ctx.pipeline();
                if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
                  Http2MultiplexCodec http2MultiplexCodec = http2MultiplexCodecBuilder.build();
                  pipeline.addLast(
                      _loggingHandler,
                      http2MultiplexCodec,
                      _loggingHandler,
                      new ChannelInboundHandlerAdapter() {
                        boolean complete;

                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                          _log.error("channelRead {}", msg.getClass());
                          complete |= msg instanceof Http2SettingsFrame;
                          super.channelRead(ctx, msg);
                        }

                        @Override
                        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
                          _log.error("channelReadComplete {}", complete);
                          if (complete) {
                            pipeline.fireUserEventTriggered(ReadyHandler.Event.EVENT);
                            pipeline.remove(this);
                          }
                          super.channelReadComplete(ctx);
                        }
                      });
                } else if (ApplicationProtocolNames.HTTP_1_1.equals(protocol)) {
                  // Just normal HTTP/1.1
                  initHttp1(pipeline);
                  pipeline.fireUserEventTriggered(ReadyHandler.Event.EVENT);
                } else {
                  ctx.close();
                  throw new IllegalStateException(
                      "unknown protocol from " + ctx.channel().remoteAddress() + " : " + protocol);
                }
              }
            });
          } catch (Throwable ignored) {
            return Optional.empty();
          }
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
          ChannelHandler protocolHandler = constructAlpnInitializer(ch).orElse(_http11Initializer);
          EventExecutorGroup executorGroup = NettyUtils.executorGroup(ch);
          ChannelPipeline pipeline = ch.pipeline();
          pipeline.addFirst(executorGroup, rateLimitConnectHandler)
              .addLast(executorGroup, _loggingHandler)
              .addLast(executorGroup, ReadyHandler.INSTANCE)
              .addLast(
                  executorGroup,
                  _clientSslContext.newHandler(ch.alloc(), address.getHostString(), address.getPort()))
              .addLast(executorGroup, protocolHandler);
        }
      };

      @Override
      protected ChannelFuture connectChannel(Bootstrap bs) {
        ChannelHandler initHandler = bs.config().handler();
        Promise<Void> voidPromise = ImmediateEventExecutor.INSTANCE.newPromise();
        ChannelFuture future = super.connectChannel(bs.attr(_initKey, voidPromise).handler(_initializer));
        ChannelPromise promise = future.channel().newPromise();
        future.addListener((ChannelFutureListener) future1 -> {
          if (future1.isSuccess()) {
            _log.error("connect success: {}", future1.channel());
            ChannelPipeline pipeline = future1.channel().pipeline();
            pipeline.addLast(initHandler);
            ChannelFuture readyFuture = ReadyHandler.INSTANCE.getFuture(future1.channel());
            if (readyFuture != null) {
              readyFuture.addListener(f -> {
                if (f.isSuccess()) {
                  promise.setSuccess();
                } else {
                  promise.setFailure(f.cause());
                }
              });
            } else {
              promise.setSuccess();
            }
          } else {
            promise.setFailure(future1.cause());
          }
        });
        return promise;
      }
    }, ch -> {
      _log.error("new stream channel: ", ch);
    }, ch -> {
      ChannelPipeline pipeline = ch.pipeline();
      pipeline.addLast(_loggingHandler, new HttpClientResponseHandler());
    });
  }

  private void testGet(String host, int port, String uri) throws InterruptedException {
    Bootstrap bootstrap = clientBootstrap(_group);
    RateLimitConnectHandler rateLimitConnectHandler = new RateLimitConnectHandler(_executorService, 100, 1000);

    InetSocketAddress test = new InetSocketAddress(host, port);
    ChannelPool pool = poolFor(bootstrap, rateLimitConnectHandler, test, 10);

    CountDownLatch done = new CountDownLatch(1);

    pool.acquire().addListener((Future<Channel> f) -> {

      if (!f.isSuccess()) {
        _log.error("Failed to acquire connection", f.cause());
        done.countDown();
        return;
      }

      // _log.error("isHttp2StreamChannel: {}", ((Http2StreamChannel) f.getNow()).stream().id());

      HttpRequest request = new SimpleRequest(uri, new Consumer<Object>() {
        Channel _channel = f.getNow();
        int _count = 5;

        @Override
        public void accept(Object o) {

          if (o instanceof HttpResponse) {
            _log.error("received httpresponse: {}", o);
            if (!(_channel instanceof Http2StreamChannel)) {
              pool.release(_channel);
              _channel = null;
            }
          }

          if (o instanceof HttpContent) {
            _log.error("content {}", o);
          }

          if (o instanceof LastHttpContent) {
            if (_channel != null) {
              pool.release(_channel);
              _channel = null;
            }

            if (_count-- <= 0) {
              _log.error("done");
              done.countDown();
              return;
            }

            pool.acquire().addListener((Future<Channel> f) -> {
              if (!f.isSuccess()) {
                _log.error("Failed to acquire connection", f.cause());
                done.countDown();
                return;
              }

              _channel = f.getNow();

              HttpRequest req = new SimpleRequest(uri, this);
              req.headers().set(HttpHeaderNames.HOST, host + ":" + port);

              _log.error("writing next request");
              _channel.writeAndFlush(req);

            });
          }
        }
      });
      request.headers().set(HttpHeaderNames.HOST, host + ":" + port);

      _log.error("writing first request");
      f.getNow().writeAndFlush(request).addListener(flushed -> {
        if (flushed.isSuccess()) {
          _log.error("flushed stream channel: {}", f.getNow());

        }
      });
    });

    Assert.assertTrue(done.await(100, TimeUnit.SECONDS));
  }

  private class SimpleRequest extends DefaultFullHttpRequest implements HttpClientResponseHandler.ResponseConsumer {
    private final Consumer<Object> _responseConsumer;

    SimpleRequest(String uri, Consumer<Object> responseConsumer) {
      super(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
      _responseConsumer = responseConsumer;
    }

    @Override
    public Consumer<Object> responseConsumer() {
      return _responseConsumer;
    }
  }

  @ChannelHandler.Sharable
  private static final class ReadyHandler extends ChannelInboundHandlerAdapter {
    static ReadyHandler INSTANCE = new ReadyHandler();

    public enum Event {
      EVENT
    }

    private static final AttributeKey<ChannelPromise> DONE = AttributeKey.valueOf(ReadyHandler.class, "DONE");

    public ChannelFuture getFuture(Channel ch) {
      return ch.attr(DONE).get();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      _log.error("handlerAdded");
      ctx.channel().attr(DONE).setIfAbsent(ctx.newPromise());
      super.handlerAdded(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
      _log.error("handlerRemoved");
      super.handlerRemoved(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      Optional.ofNullable(ctx.channel().attr(DONE).getAndSet(null)).ifPresent(this::failed);
      super.channelInactive(ctx);
    }

    private void failed(ChannelPromise channelPromise) {
      channelPromise.setFailure(new PrematureChannelClosureException());
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      _log.error("userEventTriggered {}", evt);
      if (evt == Event.EVENT) {
        Optional<ChannelPromise> promise = Optional.ofNullable(ctx.channel().attr(DONE).getAndSet(null));
        try {
          ctx.pipeline().remove(this);
        } finally {
          promise.ifPresent(ChannelPromise::setSuccess);
        }
      } else {
        super.userEventTriggered(ctx, evt);
      }
    }
  }

  static final String LOREM_IPSUM =
      "Proin auctor velit sodales dolor porttitor, eget laoreet ante pulvinar. In mattis ullamcorper odio iaculis dignissim. Maecenas odio mauris, fermentum non lorem id, feugiat pulvinar leo. In hendrerit, velit in posuere auctor, sapien nulla consequat nibh, eu suscipit nulla tellus quis nibh. Pellentesque tristique congue semper. Donec condimentum venenatis elit, eget finibus justo blandit non. Donec placerat ante elit, eget pretium nisi maximus in. Phasellus consequat iaculis malesuada. Phasellus in rutrum augue. Quisque in justo vestibulum, bibendum dui at, tempor mi. Etiam magna lacus, consectetur at sagittis eget, aliquam sed dolor. Vestibulum nec laoreet arcu. Vivamus ullamcorper tellus faucibus risus sagittis, quis aliquam tortor congue. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Sed dignissim, sem eu porta tincidunt, orci lacus gravida diam, at vehicula nulla urna vel lacus. Morbi at libero id mauris elementum fermentum id id elit.\n"
          + "\n"
          + "Praesent faucibus sem ut nulla volutpat malesuada vitae hendrerit sapien. Integer at elit id ante auctor posuere eu vel justo. Mauris at consequat eros. Suspendisse ornare et odio et efficitur. Suspendisse ac ullamcorper eros. Proin euismod felis est, nec aliquam odio faucibus vel. Donec varius, enim vitae tempus suscipit, erat leo hendrerit nibh, eu gravida augue turpis ac nunc. Curabitur feugiat risus eget laoreet ullamcorper. Donec sed vestibulum magna. Pellentesque consectetur neque eget eleifend aliquet. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Nulla egestas euismod enim, vitae volutpat orci rutrum a. Quisque tempor tortor vitae odio gravida tincidunt nec vitae mauris.\n"
          + "\n"
          + "Aenean tempor bibendum dolor sit amet vestibulum. Fusce sit amet tempus risus. Sed gravida, ligula vel efficitur ultrices, ipsum libero ultricies nunc, at interdum nibh nisi at dui. Nunc porta odio mauris. Morbi efficitur arcu quis ante iaculis iaculis. Donec bibendum blandit convallis. Donec vel pellentesque odio. Vivamus eget ex facilisis, auctor risus vitae, volutpat mi. Donec eu lectus vitae urna commodo elementum a nec ante. Morbi ac mauris dapibus, fringilla neque eu, sagittis sapien. Pellentesque pretium ex et augue rutrum luctus. Duis vehicula pulvinar lacus, sit amet imperdiet nibh dictum nec. Cras at leo libero.\n"
          + "\n"
          + "Mauris ut mi ac felis malesuada pretium. Maecenas pretium lorem vel turpis dignissim tincidunt. Pellentesque porta est eget lorem suscipit vestibulum. Maecenas vel purus et risus finibus condimentum quis quis nibh. Nulla sit amet ullamcorper dui. Donec porta pharetra sapien, ut dignissim ex facilisis eget. Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Praesent vel felis et turpis iaculis blandit. Pellentesque eget lobortis elit.\n"
          + "\n"
          + "Fusce tempor id risus et rutrum. Proin imperdiet luctus neque. Nunc convallis non nunc vitae commodo. Nam sit amet eros non nisl volutpat laoreet. Curabitur fermentum, neque id vehicula aliquam, augue purus finibus massa, vitae pellentesque eros felis eu erat. Mauris fringilla leo id consequat vestibulum. Praesent dapibus dui purus, quis aliquam tellus lobortis vitae. Interdum et malesuada fames ac ante ipsum primis in faucibus. Ut vel quam est. Praesent porttitor lectus et mauris vestibulum, id rhoncus ex porta. Ut ex sapien, blandit a pretium quis, molestie vel dui. Vestibulum rhoncus urna eu velit congue rhoncus vitae sit amet enim. Pellentesque quis vulputate mi. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Pellentesque eu elit eu est cursus auctor. Ut ut vehicula ligula, nec placerat velit.";
}
