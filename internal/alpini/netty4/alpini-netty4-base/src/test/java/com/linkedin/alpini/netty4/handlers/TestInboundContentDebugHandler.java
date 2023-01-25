package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.misc.TypedFieldAccessor;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test(groups = "unit", singleThreaded = true)
public class TestInboundContentDebugHandler {
  DefaultEventLoopGroup _group;
  ChannelGroup _channels;
  InboundContentDebugHandler _debugHandler;
  BlockingQueue<Map.Entry<FullHttpRequest, ChannelHandlerContext>> _receivedRequest = new ArrayBlockingQueue<>(5);
  BlockingQueue<Map.Entry<Throwable, ChannelHandlerContext>> _exceptionRequest = new ArrayBlockingQueue<>(5);
  BlockingQueue<Map.Entry<FullHttpResponse, ChannelHandlerContext>> _receivedResponse = new ArrayBlockingQueue<>(5);
  Bootstrap _bootstrap;

  private void testEnd() throws InterruptedException {
    Assert.assertTrue(_exceptionRequest.isEmpty());
    Time.sleep(10);
  }

  @BeforeClass
  public void setUp() throws InterruptedException {
    LocalAddress localAddress = new LocalAddress(UUID.randomUUID().toString());
    _group = new DefaultEventLoopGroup(4);

    _debugHandler = new InboundContentDebugHandler(10240);
    _channels = new DefaultChannelGroup(_group.next());

    _channels.add(
        new ServerBootstrap().group(_group)
            .channel(LocalServerChannel.class)
            .childHandler(new ChannelInitializer<LocalChannel>() {
              @Override
              protected void initChannel(LocalChannel ch) throws Exception {
                ch.pipeline()
                    .addLast(_debugHandler)
                    .addLast(new HttpServerCodec())
                    .addLast(InboundContentDebugHandler.HttpDecodeResult.INSTANCE)
                    .addLast(new HttpObjectAggregator(1024 * 1024))
                    .addLast(new ChannelInboundHandlerAdapter() {
                      @Override
                      public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        _receivedRequest.put(new AbstractMap.SimpleImmutableEntry<>((FullHttpRequest) msg, ctx));
                      }

                      @Override
                      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                        _exceptionRequest.put(new AbstractMap.SimpleImmutableEntry<>(cause, ctx));
                      }
                    });
                _channels.add(ch);
              }
            })
            .bind(localAddress)
            .sync()
            .channel());

    _bootstrap =
        new Bootstrap().group(_group).channel(LocalChannel.class).handler(new ChannelInitializer<LocalChannel>() {
          @Override
          protected void initChannel(LocalChannel ch) throws Exception {
            ch.pipeline().addLast(new ChannelOutboundHandlerAdapter() {
              @Override
              public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                ByteBuf bytes = (ByteBuf) msg;

                while (bytes.readableBytes() > 512) {
                  super.write(ctx, bytes.readRetainedSlice(512), ctx.voidPromise());
                }

                super.write(ctx, msg, promise);
              }
            })
                .addLast(new HttpClientCodec())
                .addLast(new HttpObjectAggregator(1024 * 1024))
                .addLast(new ChannelInboundHandlerAdapter() {
                  @Override
                  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                    _receivedResponse.put(new AbstractMap.SimpleImmutableEntry<>((FullHttpResponse) msg, ctx));
                  }
                });
            _channels.add(ch);
          }
        }).remoteAddress(localAddress);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws InterruptedException {
    if (_channels != null) {
      _channels.close().sync();
      _channels = null;
    }
    if (_group != null) {
      _group.shutdownGracefully();
      _group = null;
    }
  }

  @BeforeMethod
  public void beforeMethod() {
    InboundContentDebugHandler.LOG.info("beforeMethod");
    Assert.assertTrue(_exceptionRequest.isEmpty());
    Assert.assertTrue(_receivedRequest.isEmpty());
    Assert.assertTrue(_receivedResponse.isEmpty());
  }

  @Test(invocationCount = 3)
  public void testBasicHttp() throws InterruptedException {

    Channel client = _bootstrap.connect().sync().channel();
    ByteBuf postContent = client.alloc().buffer(65536);
    Random random = new Random(ThreadLocalRandom.current().nextLong());
    while (postContent.readableBytes() < 48 * 1024) {
      ByteBufUtil.writeAscii(postContent, new UUID(random.nextLong(), random.nextLong()).toString());
    }

    FullHttpRequest request = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_0,
        HttpMethod.POST,
        "/HelloWorld/" + UUID.randomUUID(),
        postContent);
    HttpUtil.setContentLength(request, postContent.readableBytes());
    Assert.assertTrue(client.writeAndFlush(request).await(1, TimeUnit.SECONDS));
    Map.Entry<FullHttpRequest, ChannelHandlerContext> receivedRequest = _receivedRequest.poll(1, TimeUnit.SECONDS);
    assert receivedRequest != null;
    Assert.assertTrue(receivedRequest.getKey().decoderResult().isSuccess());
    Assert.assertEquals(receivedRequest.getKey().uri(), request.uri());

    // wait for eventloop idle/complete
    receivedRequest.getValue().executor().submit(() -> {}).sync();

    Assert.assertTrue(receivedRequest.getValue().channel().hasAttr(InboundContentDebugHandler.BUFFER_KEY));
    ByteBuf requestBytes = InboundContentDebugHandler.fetchLastBytesOf(receivedRequest.getValue().channel(), 1024);
    Assert.assertEquals(requestBytes.readableBytes(), 0);

    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    HttpUtil.setContentLength(response, 0);

    Assert.assertTrue(receivedRequest.getValue().writeAndFlush(response).await(1, TimeUnit.SECONDS));

    Map.Entry<FullHttpResponse, ChannelHandlerContext> receivedResponse = _receivedResponse.poll(1, TimeUnit.SECONDS);
    assert receivedResponse != null;

    Assert.assertSame(receivedResponse.getKey().status(), HttpResponseStatus.OK);

    client.close().sync();

    testEnd();
  }

  @Test(invocationCount = 3)
  public void testTooLongRequestUri() throws InterruptedException {

    Channel client = _bootstrap.connect().sync().channel();

    StringBuilder requestUri = new StringBuilder("/HelloWorld");
    Random random = new Random(ThreadLocalRandom.current().nextLong());
    while (requestUri.length() < 4096) {
      requestUri.append("/").append(new UUID(random.nextLong(), random.nextLong()));
    }

    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, requestUri.toString());
    request.headers().set("X-Random-Header", "HelloWorld");
    Assert.assertTrue(client.writeAndFlush(request).await(1, TimeUnit.SECONDS));
    Map.Entry<FullHttpRequest, ChannelHandlerContext> receivedRequest = _receivedRequest.poll(1, TimeUnit.SECONDS);
    assert receivedRequest != null;
    Assert.assertTrue(receivedRequest.getKey().decoderResult().isFailure());
    Assert.assertTrue(receivedRequest.getKey().decoderResult().cause() instanceof TooLongFrameException);
    Assert.assertEquals(
        receivedRequest.getKey().decoderResult().cause().getMessage(),
        "An HTTP line is larger than 4096 bytes.");
    Assert.assertEquals(receivedRequest.getKey().uri(), "/bad-request");

    Assert.assertTrue(receivedRequest.getValue().channel().hasAttr(InboundContentDebugHandler.BUFFER_KEY));
    ByteBuf requestBytes = InboundContentDebugHandler.fetchLastBytesOf(receivedRequest.getValue().channel(), 10240);
    Assert.assertEquals(requestBytes.readableBytes(), 4164);

    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST);
    HttpUtil.setContentLength(response, 0);

    Assert.assertTrue(receivedRequest.getValue().writeAndFlush(response).await(1, TimeUnit.SECONDS));

    Map.Entry<FullHttpResponse, ChannelHandlerContext> receivedResponse = _receivedResponse.poll(1, TimeUnit.SECONDS);
    assert receivedResponse != null;

    Assert.assertSame(receivedResponse.getKey().status(), HttpResponseStatus.BAD_REQUEST);

    client.close().sync();

    testEnd();
  }

  @Test(invocationCount = 3)
  public void testTooLongHeader() throws InterruptedException {

    Channel client = _bootstrap.connect().sync().channel();

    Random random = new Random(ThreadLocalRandom.current().nextLong());

    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/HelloWorld");
    int estimatedSize = 0;
    while (estimatedSize < 8192) {
      request.headers()
          .add("X-" + new UUID(random.nextLong(), random.nextLong()), new UUID(random.nextLong(), random.nextLong()));
      estimatedSize += 74;
    }

    Assert.assertTrue(client.writeAndFlush(request).await(1, TimeUnit.SECONDS));
    Map.Entry<FullHttpRequest, ChannelHandlerContext> receivedRequest = _receivedRequest.poll(1, TimeUnit.SECONDS);
    assert receivedRequest != null;
    Assert.assertTrue(receivedRequest.getKey().decoderResult().isFailure());
    Assert.assertTrue(receivedRequest.getKey().decoderResult().cause() instanceof TooLongFrameException);
    Assert.assertEquals(
        receivedRequest.getKey().decoderResult().cause().getMessage(),
        "HTTP header is larger than 8192 bytes.");
    Assert.assertEquals(receivedRequest.getKey().uri(), "/HelloWorld");

    Assert.assertTrue(receivedRequest.getValue().channel().hasAttr(InboundContentDebugHandler.BUFFER_KEY));
    ByteBuf requestBytes = InboundContentDebugHandler.fetchLastBytesOf(receivedRequest.getValue().channel(), 10240);
    Assert.assertEquals(requestBytes.readableBytes(), 8686);

    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST);
    HttpUtil.setContentLength(response, 0);

    Assert.assertTrue(receivedRequest.getValue().writeAndFlush(response).await(1, TimeUnit.SECONDS));

    Map.Entry<FullHttpResponse, ChannelHandlerContext> receivedResponse = _receivedResponse.poll(1, TimeUnit.SECONDS);
    assert receivedResponse != null;

    Assert.assertSame(receivedResponse.getKey().status(), HttpResponseStatus.BAD_REQUEST);

    client.close().sync();

    testEnd();
  }

  @Test(invocationCount = 3)
  public void testBadHttpHeader() throws InterruptedException {

    Channel client = _bootstrap.connect().sync().channel();

    FullHttpRequest request =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/HelloWorld/" + UUID.randomUUID(), false);
    request.headers().add("X-Value-Bad", "This is a \u0000surprise");
    request.headers().add("X-Value-Ok", "This is a ok");
    Assert.assertTrue(client.writeAndFlush(request).await(1, TimeUnit.SECONDS));
    Map.Entry<FullHttpRequest, ChannelHandlerContext> receivedRequest = _receivedRequest.poll(1, TimeUnit.SECONDS);
    assert receivedRequest != null;
    Assert.assertTrue(receivedRequest.getKey().decoderResult().isFailure());
    Assert.assertTrue(receivedRequest.getKey().decoderResult().cause() instanceof IllegalArgumentException);
    Assert.assertTrue(
        receivedRequest.getKey()
            .decoderResult()
            .cause()
            .getMessage()
            .startsWith("a header value contains a prohibited character"));
    Assert.assertEquals(receivedRequest.getKey().uri(), request.uri());

    Assert.assertTrue(receivedRequest.getValue().channel().hasAttr(InboundContentDebugHandler.BUFFER_KEY));
    ByteBuf requestBytes = InboundContentDebugHandler.fetchLastBytesOf(receivedRequest.getValue().channel(), 1024);
    Assert.assertEquals(requestBytes.readableBytes(), 125);

    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST);
    HttpUtil.setContentLength(response, 0);
    HttpUtil.setKeepAlive(response, false);

    Assert.assertTrue(receivedRequest.getValue().writeAndFlush(response).await(1, TimeUnit.SECONDS));

    Map.Entry<FullHttpResponse, ChannelHandlerContext> receivedResponse = _receivedResponse.poll(1, TimeUnit.SECONDS);
    assert receivedResponse != null;

    Assert.assertSame(receivedResponse.getKey().status(), HttpResponseStatus.BAD_REQUEST);

    client.close().sync();

    testEnd();
  }

  @Test(invocationCount = 3)
  public void testBadHttpVersion() throws InterruptedException {

    HttpVersion badVersion = new HttpVersion("BAD", 0, 0, false);
    TypedFieldAccessor.forField(HttpVersion.class, "text").accept(badVersion, "BAD\u0000xyz foo");

    Channel client = _bootstrap.connect().sync().channel();

    FullHttpRequest request =
        new DefaultFullHttpRequest(badVersion, HttpMethod.GET, "/HelloWorld/" + UUID.randomUUID(), false);
    request.headers().add("X-Value-Ok", "This is a ok");
    Assert.assertTrue(client.writeAndFlush(request).await(1, TimeUnit.SECONDS));
    Map.Entry<FullHttpRequest, ChannelHandlerContext> receivedRequest = _receivedRequest.poll(1, TimeUnit.SECONDS);
    assert receivedRequest != null;
    Assert.assertTrue(receivedRequest.getKey().decoderResult().isFailure());
    Assert.assertTrue(receivedRequest.getKey().decoderResult().cause() instanceof IllegalArgumentException);
    Assert
        .assertTrue(receivedRequest.getKey().decoderResult().cause().getMessage().startsWith("invalid version format"));
    Assert.assertEquals(receivedRequest.getKey().uri(), "/bad-request");

    Assert.assertTrue(receivedRequest.getValue().channel().hasAttr(InboundContentDebugHandler.BUFFER_KEY));
    ByteBuf requestBytes = InboundContentDebugHandler.fetchLastBytesOf(receivedRequest.getValue().channel(), 1024);
    Assert.assertEquals(requestBytes.readableBytes(), 94);

    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST);
    HttpUtil.setContentLength(response, 0);
    HttpUtil.setKeepAlive(response, false);

    Assert.assertTrue(receivedRequest.getValue().writeAndFlush(response).await(1, TimeUnit.SECONDS));

    Map.Entry<FullHttpResponse, ChannelHandlerContext> receivedResponse = _receivedResponse.poll(1, TimeUnit.SECONDS);
    assert receivedResponse != null;

    Assert.assertSame(receivedResponse.getKey().status(), HttpResponseStatus.BAD_REQUEST);

    client.close().sync();

    testEnd();
  }
}
