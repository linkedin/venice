package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.PrematureChannelClosureException;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.NetUtil;
import io.netty.util.ReferenceCountUtil;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestBasicHttpClientCodec {
  private static final String EMPTY_RESPONSE = "HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n";
  private static final String RESPONSE = "HTTP/1.0 200 OK\r\n" + "Date: Fri, 31 Dec 1999 23:59:59 GMT\r\n"
      + "Content-Type: text/html\r\n" + "Content-Length: 28\r\n" + "\r\n" + "<html><body></body></html>\r\n";
  private static final String INCOMPLETE_CHUNKED_RESPONSE = "HTTP/1.1 200 OK\r\n" + "Content-Type: text/plain\r\n"
      + "Transfer-Encoding: chunked\r\n" + "\r\n" + "5\r\n" + "first\r\n" + "6\r\n" + "second\r\n" + "0\r\n";
  private static final String CHUNKED_RESPONSE = INCOMPLETE_CHUNKED_RESPONSE + "\r\n";

  @Test(groups = "unit")
  public void testConnectWithResponseContent() {
    BasicHttpClientCodec codec = new BasicHttpClientCodec(4096, 8192, 8192, true);
    EmbeddedChannel ch = new EmbeddedChannel(codec);

    sendRequestAndReadResponse(ch, HttpMethod.CONNECT, RESPONSE);
    ch.finish();
  }

  @Test(groups = "unit")
  public void testFailsNotOnRequestResponseChunked() {
    BasicHttpClientCodec codec = new BasicHttpClientCodec(4096, 8192, 8192, true);
    EmbeddedChannel ch = new EmbeddedChannel(codec);

    sendRequestAndReadResponse(ch, HttpMethod.GET, CHUNKED_RESPONSE);
    ch.finish();
  }

  @Test(groups = "unit")
  public void testFailsOnMissingResponse() {
    BasicHttpClientCodec codec = new BasicHttpClientCodec(4096, 8192, 8192, true);
    EmbeddedChannel ch = new EmbeddedChannel(codec);

    Assert.assertTrue(
        ch.writeOutbound(
            new BasicFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.GET,
                "http://localhost/",
                Time.currentTimeMillis(),
                Time.nanoTime())));
    ByteBuf buffer = ch.readOutbound();
    Assert.assertNotNull(buffer);
    buffer.release();
    try {
      ch.finish();
      Assert.fail();
    } catch (CodecException e) {
      Assert.assertTrue(e instanceof PrematureChannelClosureException);
    }
  }

  @Test(groups = "unit")
  public void testFailsOnIncompleteChunkedResponse() {
    BasicHttpClientCodec codec = new BasicHttpClientCodec(4096, 8192, 8192, true);
    EmbeddedChannel ch = new EmbeddedChannel(codec);

    ch.writeOutbound(
        new BasicFullHttpRequest(
            HttpVersion.HTTP_1_1,
            HttpMethod.GET,
            "http://localhost/",
            Time.currentTimeMillis(),
            Time.nanoTime()));
    ByteBuf buffer = ch.readOutbound();
    Assert.assertNotNull(buffer);
    buffer.release();
    Assert.assertNull(ch.readInbound());
    ch.writeInbound(Unpooled.copiedBuffer(INCOMPLETE_CHUNKED_RESPONSE, StandardCharsets.ISO_8859_1));
    Assert.assertTrue(ch.readInbound() instanceof HttpResponse);
    ((HttpContent) ch.readInbound()).release(); // Chunk 'first'
    ((HttpContent) ch.readInbound()).release(); // Chunk 'second'
    Assert.assertNull(ch.readInbound());

    try {
      ch.finish();
      Assert.fail();
    } catch (CodecException e) {
      Assert.assertTrue(e instanceof PrematureChannelClosureException);
    }
  }

  @Test(groups = "unit")
  public void testServerCloseSocketInputProvidesData() throws InterruptedException {
    ServerBootstrap sb = new ServerBootstrap();
    Bootstrap cb = new Bootstrap();
    final CountDownLatch serverChannelLatch = new CountDownLatch(1);
    final CountDownLatch responseReceivedLatch = new CountDownLatch(1);
    try {
      sb.group(new NioEventLoopGroup(2));
      sb.channel(NioServerSocketChannel.class);
      sb.childHandler(new ChannelInitializer<Channel>() {
        @Override
        protected void initChannel(Channel ch) throws Exception {
          // Don't use the HttpServerCodec, because we don't want to have content-length or anything added.
          ch.pipeline().addLast(new HttpRequestDecoder(4096, 8192, 8192, true));
          ch.pipeline().addLast(new BasicHttpObjectAggregator(4096));
          ch.pipeline().addLast(new SimpleChannelInboundHandler<FullHttpRequest>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) {
              // This is just a simple demo...don't block in IO
              Assert.assertTrue(ctx.channel() instanceof SocketChannel);
              final SocketChannel sChannel = (SocketChannel) ctx.channel();
              /*
               * The point of this test is to not add any content-length or content-encoding headers
               * and the client should still handle this.
               * See <a href="https://tools.ietf.org/html/rfc7230#section-3.3.3">RFC 7230, 3.3.3</a>.
               */
              sChannel
                  .writeAndFlush(
                      Unpooled.wrappedBuffer(
                          ("HTTP/1.0 200 OK\r\n" + "Date: Fri, 31 Dec 1999 23:59:59 GMT\r\n"
                              + "Content-Type: text/html\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1)))
                  .addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                      Assert.assertTrue(future.isSuccess());
                      sChannel.writeAndFlush(
                          Unpooled.wrappedBuffer(
                              "<html><body>hello half closed!</body></html>\r\n".getBytes(StandardCharsets.ISO_8859_1)))
                          .addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                              Assert.assertTrue(future.isSuccess());
                              sChannel.shutdownOutput();
                            }
                          });
                    }
                  });
            }
          });
          serverChannelLatch.countDown();
        }
      });

      cb.group(new NioEventLoopGroup(1));
      cb.channel(NioSocketChannel.class);
      cb.option(ChannelOption.ALLOW_HALF_CLOSURE, true);
      cb.handler(new ChannelInitializer<Channel>() {
        @Override
        protected void initChannel(Channel ch) {
          ch.pipeline().addLast(new BasicHttpClientCodec(4096, 8192, 8192, true, true));
          ch.pipeline().addLast(new BasicHttpObjectAggregator(4096));
          ch.pipeline().addLast(new SimpleChannelInboundHandler<FullHttpResponse>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
              responseReceivedLatch.countDown();
            }
          });
        }
      });

      Channel serverChannel = sb.bind(new InetSocketAddress(0)).sync().channel();
      int port = ((InetSocketAddress) serverChannel.localAddress()).getPort();

      ChannelFuture ccf = cb.connect(new InetSocketAddress(NetUtil.LOCALHOST, port));
      Assert.assertTrue(ccf.awaitUninterruptibly().isSuccess());
      Channel clientChannel = ccf.channel();
      Assert.assertTrue(serverChannelLatch.await(5, TimeUnit.SECONDS));
      clientChannel.writeAndFlush(new BasicHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"));
      Assert.assertTrue(responseReceivedLatch.await(5, TimeUnit.SECONDS));
    } finally {
      sb.config().group().shutdownGracefully();
      sb.config().childGroup().shutdownGracefully();
      cb.config().group().shutdownGracefully();
    }
  }

  @Test(groups = "unit")
  public void testContinueParsingAfterConnect() throws Exception {
    testAfterConnect(true);
  }

  @Test(groups = "unit")
  public void testPassThroughAfterConnect() throws Exception {
    testAfterConnect(false);
  }

  private static void testAfterConnect(final boolean parseAfterConnect) throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new BasicHttpClientCodec(4096, 8192, 8192, true, true, parseAfterConnect));

    Consumer connectResponseConsumer = new Consumer();
    sendRequestAndReadResponse(ch, HttpMethod.CONNECT, EMPTY_RESPONSE, connectResponseConsumer);
    Assert.assertTrue(connectResponseConsumer.getReceivedCount() > 0, "No connect response messages received.");
    Consumer responseConsumer = new Consumer() {
      @Override
      void accept(Object object) {
        if (parseAfterConnect) {
          Assert.assertTrue(object instanceof HttpObject, "Unexpected response message type.");
        } else {
          Assert.assertFalse(object instanceof HttpObject, "Unexpected response message type.");
        }
      }
    };
    sendRequestAndReadResponse(ch, HttpMethod.GET, RESPONSE, responseConsumer);
    Assert.assertTrue(responseConsumer.getReceivedCount() > 0, "No response messages received.");
    Assert.assertFalse(ch.finish(), "Channel finish failed.");
  }

  private static void sendRequestAndReadResponse(EmbeddedChannel ch, HttpMethod httpMethod, String response) {
    sendRequestAndReadResponse(ch, httpMethod, response, new Consumer());
  }

  private static void sendRequestAndReadResponse(
      EmbeddedChannel ch,
      HttpMethod httpMethod,
      String response,
      Consumer responseConsumer) {
    Assert.assertTrue(
        ch.writeOutbound(
            new BasicFullHttpRequest(
                HttpVersion.HTTP_1_1,
                httpMethod,
                "http://localhost/",
                Time.currentTimeMillis(),
                Time.nanoTime())),
        "Channel outbound write failed.");
    Assert.assertTrue(
        ch.writeInbound(Unpooled.copiedBuffer(response, StandardCharsets.ISO_8859_1)),
        "Channel inbound write failed.");

    for (;;) {
      Object msg = ch.readOutbound();
      if (msg == null) {
        break;
      }
      ReferenceCountUtil.release(msg);
    }
    for (;;) {
      Object msg = ch.readInbound();
      if (msg == null) {
        break;
      }
      responseConsumer.onResponse(msg);
      ReferenceCountUtil.release(msg);
    }
  }

  private static class Consumer {
    private int receivedCount;

    final void onResponse(Object object) {
      receivedCount++;
      accept(object);
    }

    void accept(Object object) {
      // Default noop.
    }

    int getReceivedCount() {
      return receivedCount;
    }
  }

  @Test(groups = "unit")
  public void testDecodesFinalResponseAfterSwitchingProtocols() {
    String SWITCHING_PROTOCOLS_RESPONSE =
        "HTTP/1.1 101 Switching Protocols\r\n" + "Connection: Upgrade\r\n" + "Upgrade: TLS/1.2, HTTP/1.1\r\n\r\n";

    BasicHttpClientCodec codec = new BasicHttpClientCodec(4096, 8192, 8192, true);
    EmbeddedChannel ch = new EmbeddedChannel(codec, new BasicHttpObjectAggregator(1024));

    HttpRequest request = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "http://localhost/",
        Time.currentTimeMillis(),
        Time.nanoTime());
    request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE);
    request.headers().set(HttpHeaderNames.UPGRADE, "TLS/1.2");
    Assert.assertTrue(ch.writeOutbound(request), "Channel outbound write failed.");

    Assert.assertTrue(
        ch.writeInbound(Unpooled.copiedBuffer(SWITCHING_PROTOCOLS_RESPONSE, StandardCharsets.ISO_8859_1)),
        "Channel inbound write failed.");
    Object switchingProtocolsResponse = ch.readInbound();
    Assert.assertNotNull(switchingProtocolsResponse, "No response received");
    Assert.assertTrue(switchingProtocolsResponse instanceof FullHttpResponse, "Response was not decoded");
    ((FullHttpResponse) switchingProtocolsResponse).release();

    Assert.assertTrue(
        ch.writeInbound(Unpooled.copiedBuffer(RESPONSE, StandardCharsets.ISO_8859_1)),
        "Channel inbound write failed");
    Object finalResponse = ch.readInbound();
    Assert.assertNotNull(finalResponse, "No response received");
    Assert.assertTrue(finalResponse instanceof FullHttpResponse, "Response was not decoded");
    ((FullHttpResponse) finalResponse).release();
    Assert.assertTrue(ch.finishAndReleaseAll(), "Channel finish failed");
  }

  @Test(groups = "unit")
  public void testWebSocket00Response() {
    byte[] data = ("HTTP/1.1 101 WebSocket Protocol Handshake\r\n" + "Upgrade: WebSocket\r\n"
        + "Connection: Upgrade\r\n" + "Sec-WebSocket-Origin: http://localhost:8080\r\n"
        + "Sec-WebSocket-Location: ws://localhost/some/path\r\n" + "\r\n" + "1234567812345678").getBytes();
    EmbeddedChannel ch = new EmbeddedChannel(new BasicHttpClientCodec());
    Assert.assertTrue(ch.writeInbound(Unpooled.wrappedBuffer(data)));

    HttpResponse res = ch.readInbound();
    Assert.assertSame(res.protocolVersion(), HttpVersion.HTTP_1_1);
    Assert.assertEquals(res.status(), HttpResponseStatus.SWITCHING_PROTOCOLS);
    HttpContent content = ch.readInbound();
    Assert.assertEquals(content.content().readableBytes(), 16);
    content.release();

    Assert.assertFalse(ch.finish());

    Assert.assertNull(ch.readInbound());
  }
}
