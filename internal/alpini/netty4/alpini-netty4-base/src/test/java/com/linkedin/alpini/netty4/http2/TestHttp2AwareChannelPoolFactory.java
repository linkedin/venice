package com.linkedin.alpini.netty4.http2;

import com.linkedin.alpini.base.concurrency.RunOnce;
import com.linkedin.alpini.base.misc.Msg;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.monitoring.CallTracker;
import com.linkedin.alpini.base.monitoring.CallTrackerImpl;
import com.linkedin.alpini.base.monitoring.NullCallTracker;
import com.linkedin.alpini.consts.QOS;
import com.linkedin.alpini.netty4.handlers.BasicHttpObjectAggregator;
import com.linkedin.alpini.netty4.handlers.HttpClientResponseHandler;
import com.linkedin.alpini.netty4.handlers.Log4J2LoggingHandler;
import com.linkedin.alpini.netty4.handlers.SimpleChannelInitializer;
import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicFullHttpResponse;
import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicHttpResponse;
import com.linkedin.alpini.netty4.misc.Http2Utils;
import com.linkedin.alpini.netty4.pool.ChannelPoolManager;
import com.linkedin.alpini.netty4.pool.ChannelPoolManagerImpl;
import com.linkedin.alpini.netty4.pool.Http2AwareChannelPoolFactory;
import com.linkedin.alpini.netty4.pool.NettyDnsResolver;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ResolveAllBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.Http2FrameCodec;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.util.AsciiString;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import javax.net.ssl.SSLException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StringBuilderFormattable;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 4/20/18.
 */
public class TestHttp2AwareChannelPoolFactory {
  final Logger _log = LogManager.getLogger(getClass());

  final LoggingHandler _loggingHandler = new Log4J2LoggingHandler(LogLevel.TRACE);

  final ChannelHealthChecker _healthChecker = ch -> {
    if (ch.isActive()) {
      return ch.eventLoop().newSucceededFuture(true);
    } else {
      return ch.eventLoop().newSucceededFuture(false);
    }
  };

  EpollEventLoopGroup _eventLoop;

  @BeforeClass(groups = "unit")
  public void beforeClass() {
    _eventLoop = new EpollEventLoopGroup(1);
  }

  @AfterClass(groups = "unit")
  public void afterClass() {
    if (_eventLoop != null) {
      _eventLoop.shutdownGracefully();
    }
  }

  @Test(groups = "unit", enabled = false)
  public void testWithPublicCDN() throws InterruptedException, SSLException {
    foo(
        SSLContextBuilder.makeClientContext(0, 0),
        "1906714720.rsc.cdn77.org:443",
        "/img/cdn77-test-563kb.jpg",
        true,
        false,
        10,
        false);
    foo(
        SSLContextBuilder.makeClientContext(0, 0),
        "1906714720.rsc.cdn77.org:443",
        "/img/cdn77-test-563kb.jpg",
        true,
        true,
        10,
        false);
  }

  @Test(groups = "unit", dataProvider = "http2WithLocalSkeletonArgs", invocationTimeOut = 10000)
  public void testHttp2WithLocalSkeleton(
      boolean useMultiplexCodec,
      boolean useCustomH2Codec,
      boolean channelReuse,
      boolean offloadStream,
      boolean useFastPool) throws InterruptedException, SSLException {
    testWithLocalSkeleton(
        SSLContextBuilder.makeServerContext(0, 0),
        SSLContextBuilder.makeClientContext(0, 0),
        useMultiplexCodec,
        useCustomH2Codec,
        channelReuse,
        offloadStream,
        useFastPool,
        10,
        false);
  }

  @Test(groups = "unit", dataProvider = "http2WithLocalSkeletonArgs", invocationTimeOut = 10000)
  public void testHttp2WithRemoteBadConnection(
      boolean useMultiplexCodec,
      boolean useCustomH2Codec,
      boolean channelReuse,
      boolean offloadStream,
      boolean useFastPool) throws InterruptedException, SSLException {
    testWithLocalSkeleton(
        SSLContextBuilder.makeServerContext(0, 0),
        SSLContextBuilder.makeClientContext(0, 0),
        useMultiplexCodec,
        useCustomH2Codec,
        channelReuse,
        offloadStream,
        useFastPool,
        1,
        true);
  }

  @DataProvider
  public Object[][] http2WithLocalSkeletonArgs() {
    return new Object[][] { new Object[] { true, true, true, true, false },
        new Object[] { true, true, true, false, false }, new Object[] { true, false, true, true, false },
        new Object[] { true, false, false, true, false }, new Object[] { true, false, false, false, false },
        new Object[] { true, true, true, true, true }, new Object[] { true, true, true, false, true },
        new Object[] { true, false, true, true, true }, new Object[] { true, false, false, true, true },
        new Object[] { true, false, false, false, true },

        new Object[] { false, false, false, true, false }, new Object[] { false, false, false, false, false },
        new Object[] { false, false, false, true, true }, new Object[] { false, false, false, false, true }, };
  }

  @DataProvider
  public Object[][] noHttp2WithLocalSkeletonArgs() {
    return new Object[][] { new Object[] { true, false, false, true }, new Object[] { true, false, false, false },
        new Object[] { false, true, false, true }, new Object[] { false, false, false, true },
        new Object[] { true, false, true, true }, new Object[] { true, false, true, false },
        new Object[] { false, true, true, true }, new Object[] { false, false, true, true }, };
  }

  @Test(groups = "unit", dataProvider = "noHttp2WithLocalSkeletonArgs", invocationTimeOut = 10000)
  public void testNoHttp2WithLocalSkeleton(
      boolean serverPermitHttp2,
      boolean clientPermitHttp2,
      boolean useMultiplexCodec,
      boolean usingFastPool) throws InterruptedException, SSLException {
    testWithLocalSkeleton(
        SSLContextBuilder.makeServerContext(0, 0, serverPermitHttp2),
        SSLContextBuilder.makeClientContext(0, 0, clientPermitHttp2),
        useMultiplexCodec,
        usingFastPool);
  }

  public void testWithLocalSkeleton(
      SslContext serverSslContext,
      SslContext clientSslContext,
      boolean useMultiplexCodec,
      boolean usingFastPool) throws InterruptedException, SSLException {
    testWithLocalSkeleton(
        serverSslContext,
        clientSslContext,
        useMultiplexCodec,
        false,
        false,
        false,
        usingFastPool,
        10,
        false);
  }

  public void testWithLocalSkeleton(
      SslContext serverSslContext,
      SslContext clientSslContext,
      boolean useMultiplexCodec,
      boolean useCustomH2Codec,
      boolean channelReuse,
      boolean offloadStream,
      boolean usingFastPool,
      int maxConnections,
      boolean withDie) throws InterruptedException, SSLException {
    LongAdder serverChannelCount = new LongAdder();
    ChannelFuture serverFuture = SkeletonHttp2Server.setupBootstrap(
        new ServerBootstrap().group(_eventLoop).channel(EpollServerSocketChannel.class),
        serverSslContext,
        new SimpleChannelInitializer<Channel>() {
          @Override
          protected void initChannel(Channel ch) throws Exception {
            addAfter(ch, new BasicHttpObjectAggregator(16 * 1024), new SimpleChannelInboundHandler<BasicHttpRequest>() {
              boolean _wantsFlush;

              @Override
              protected void channelRead0(ChannelHandlerContext ctx, BasicHttpRequest msg) throws Exception {
                assert msg instanceof BasicFullHttpRequest;
                BasicFullHttpResponse response = new BasicFullHttpResponse(
                    msg,
                    HttpResponseStatus.OK,
                    Unpooled.copiedBuffer(TestBasicHttp2ClientServer.LOREM_IPSUM, StandardCharsets.US_ASCII));
                HttpUtil.setContentLength(response, response.content().readableBytes());
                ctx.write(response);
                _wantsFlush = true;
                if ("/die".equals(msg.uri())) {
                  Channel ch = ctx.channel();
                  if (ch instanceof Http2StreamChannel) {
                    ch = ch.parent();
                  }
                  ch.unsafe().closeForcibly();
                }
              }

              @Override
              public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
                if (_wantsFlush) {
                  ctx.flush();
                  _wantsFlush = false;
                }
                super.channelReadComplete(ctx);
              }

              @Override
              public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                serverChannelCount.increment();
                super.handlerAdded(ctx);
              }

              @Override
              public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                serverChannelCount.decrement();
                super.channelInactive(ctx);
              }
            });
          }
        }).bind(0).sync();
    try {
      InetSocketAddress address = (InetSocketAddress) serverFuture.channel().localAddress();

      foo(
          clientSslContext,
          "localhost:" + address.getPort(),
          "/foo",
          useMultiplexCodec,
          useCustomH2Codec,
          channelReuse,
          offloadStream,
          usingFastPool,
          maxConnections,
          withDie);

    } finally {
      serverFuture.channel().close();
      Assert.assertEquals(serverChannelCount.sum(), 0);
    }
  }

  public void foo(
      SslContext sslContext,
      String hostAndPort,
      String uri,
      boolean useMultiplexCodec,
      boolean usingFastPool,
      int maxConnections,
      boolean withDie) throws InterruptedException, SSLException {
    foo(sslContext, hostAndPort, uri, useMultiplexCodec, false, false, false, usingFastPool, maxConnections, withDie);
  }

  public void foo(
      SslContext sslContext,
      String hostAndPort,
      String uri,
      boolean useMultiplexCodec,
      boolean useCustomH2Codec,
      boolean channelReuse,
      boolean offloadStream,
      boolean usingFastPool,
      int maxConnections,
      boolean withDie) throws InterruptedException, SSLException {

    NettyDnsResolver resolver = new NettyDnsResolver(EpollDatagramChannel.class, _eventLoop);

    Bootstrap bootstrap =
        new ResolveAllBootstrap(NullCallTracker.INSTANCE, NullCallTracker.INSTANCE).channel(EpollSocketChannel.class)
            .handler(new ChannelInitializer<Channel>() {
              @Override
              protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();

                pipeline.addLast(_loggingHandler).addLast(new HttpClientResponseHandler());
              }
            })
            .attr(Http2AwareChannelPoolFactory.SSL_CONTEXT, sslContext)
            .resolver(resolver.getAddressResolverGroup());

    CallTracker callTracker = new CallTrackerImpl();

    Http2AwareChannelPoolFactory factory = new Http2AwareChannelPoolFactory(
        bootstrap,
        10000,
        maxConnections,
        100,
        true,
        3600000,
        _healthChecker,
        callTracker);
    factory.setUsingMultiplexHandler(useMultiplexCodec);
    factory.setReuseChannels(channelReuse);
    factory.setOffloadStreams(offloadStream);
    factory.setUseCustomH2Codec(useCustomH2Codec);
    factory.setUsingFastPool(usingFastPool);
    factory.setHttp1MaxConnections(() -> maxConnections);
    factory.setHttp1MinConnections(() -> maxConnections);

    ChannelPoolManager manager = new ChannelPoolManagerImpl(_eventLoop, factory, resolver, 100);
    try {

      LinkedList<Future<FullHttpResponse>> responses = new LinkedList<>();

      for (int i = 50; i >= 0; i--) {
        BasicFullHttpRequest request = new BasicFullHttpRequest(
            HttpVersion.HTTP_1_1,
            HttpMethod.GET,
            uri,
            Unpooled.EMPTY_BUFFER,
            false,
            Time.currentTimeMillis(),
            Time.nanoTime());
        request.headers().set(HttpHeaderNames.HOST, hostAndPort);

        if (withDie && i == 0) {
          request.setUri("/die");
        }

        responses.add(send(manager, request, ImmediateEventExecutor.INSTANCE.newPromise()));
      }

      for (Future<FullHttpResponse> responseFuture: responses) {

        if (withDie) {
          responseFuture.await();
          if (!responseFuture.isSuccess()) {
            _log.info("response failed", responseFuture.cause());
            continue;
          }
        }

        responseFuture.sync();

        _log.info("Received response of {} bytes", responseFuture.getNow().content().readableBytes());

        responseFuture.getNow().release();
      }

    } finally {
      manager.closeAll().sync();
    }

    Assert.assertEquals(manager.activeCount(), 0);
    Assert.assertEquals(manager.openConnections(), 0);

    _log.error("calltracker: {}", callTracker.getCallStats());
  }

  private static final AsciiString X_QUEUE_NAME = AsciiString.of("X-Queue-Name");
  private static final AsciiString X_QOS = AsciiString.of("X-QOS");

  Future<FullHttpResponse> send(
      ChannelPoolManager manager,
      FullHttpRequest request,
      Promise<FullHttpResponse> responsePromise) {
    String hostAndPort = Objects.requireNonNull(request.headers().get(HttpHeaderNames.HOST), "Header missing: Host");
    String queueName = request.headers().get(X_QUEUE_NAME, "DEFAULT");
    QOS qos = Optional.ofNullable(request.headers().get(X_QOS)).map(QOS::valueOf).orElse(QOS.NORMAL);

    manager.acquire(hostAndPort, queueName, qos).addListener((Future<Channel> channelFuture) -> {
      if (channelFuture.isSuccess()) {
        Runnable releaseChannel = RunOnce.make(channelFuture.getNow(), manager::release);
        Consumer<Object> responseConsumer = new Consumer<Object>() {
          private HttpResponse _response;
          private ByteBuf _content = Unpooled.EMPTY_BUFFER;
          private CompositeByteBuf _composite = null;

          @Override
          public void accept(Object o) {
            if (o instanceof Throwable) {
              Throwable throwable = (Throwable) o;
              if (channelFuture.getNow().pipeline().context(Http2FrameCodec.class) == null) {
                channelFuture.getNow().close().addListener((ChannelFuture closeFuture) -> {
                  releaseChannel.run();
                });
              } else {
                releaseChannel.run();
              }

              Optional.ofNullable(_content).ifPresent(ByteBuf::release);
              _content = null;

              if (!responsePromise.tryFailure(throwable)) {
                _log.warn(
                    "promise already completed {}",
                    responsePromise.isSuccess() ? "successfully" : responsePromise.cause().getClass().getSimpleName(),
                    throwable);
              }
              return;
            }

            if (o instanceof HttpResponse) {
              HttpResponse response = (HttpResponse) o;
              _response = new BasicHttpResponse(request, response.status(), response.headers());
            }

            if (o instanceof HttpContent) {
              if (_content == Unpooled.EMPTY_BUFFER) {
                _content = ((HttpContent) o).content().retain();
              } else {
                _content = Optional.ofNullable(_composite)
                    .orElseGet(() -> _composite = _content.alloc().compositeBuffer().addComponent(true, _content))
                    .addComponent(true, ((HttpContent) o).content().retain());
              }
              _log.debug("content -> {}", _content);
            }

            if (o instanceof LastHttpContent) {
              LastHttpContent last = (LastHttpContent) o;
              FullHttpResponse fullHttpResponse =
                  new BasicFullHttpResponse(_response, _response.headers(), last.trailingHeaders(), _content);
              _content = null;

              HttpUtil.setContentLength(fullHttpResponse, fullHttpResponse.content().readableBytes());

              if (HttpUtil.isKeepAlive(fullHttpResponse)) {
                releaseChannel.run();
              } else {
                channelFuture.getNow().close().addListener((ChannelFuture closeFuture) -> releaseChannel.run());
              }

              if (!responsePromise.trySuccess(fullHttpResponse)) {
                fullHttpResponse.release();
              }
            }
          }
        };

        _log.error(
            "Channel: {}  className: {}{}",
            channelFuture.getNow(),
            channelFuture.getNow().getClass().getSimpleName(),
            Msg.make(
                channelFuture.getNow().pipeline(),
                pipeline -> (StringBuilderFormattable) buffer -> pipeline.iterator()
                    .forEachRemaining(
                        entry -> buffer.append("\n  Name: ")
                            .append(entry.getKey())
                            .append("  Handler: ")
                            .append(entry.getValue().getClass().getSimpleName()))));

        channelFuture.getNow()
            .writeAndFlush(new FullHttpRequestResponseConsumer(request, responseConsumer))
            .addListener((ChannelFuture writeFuture) -> {
              if (!writeFuture.isSuccess()) {
                writeFuture.channel()
                    .close()
                    .addListener((ChannelFuture closeFuture) -> manager.release(closeFuture.channel()));
                responsePromise.setFailure(writeFuture.cause());
              }
            });
        if (Http2Utils.isHttp2ParentChannelPipeline(channelFuture.getNow().pipeline())) {
          releaseChannel.run();
        }
      } else {
        responsePromise.setFailure(channelFuture.cause());
      }
    });
    return responsePromise;
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
