package com.linkedin.alpini.netty4.handlers;

import static org.mockito.Mockito.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.NetUtil;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;


public class TestClientConnectionTracker {
  private EventLoopGroup _group = new NioEventLoopGroup(1);;

  private static final Logger LOG = LogManager.getLogger(InstrumentedTracker.class);

  @AfterClass
  public void afterClass() {
    Optional.ofNullable(_group).ifPresent(EventLoopGroup::shutdownGracefully);
  }

  private ChannelHandlerContext preparedMockContextWithDifferentIpAddress(String hostName) throws UnknownHostException {
    ChannelHandlerContext context = mock(ChannelHandlerContext.class, withSettings().stubOnly());
    Channel channel = mock(Channel.class, withSettings().stubOnly());
    when(context.channel()).thenReturn(channel);

    // the socketAddress's getAddress is final so can't be mocked. We have to use a real address.
    InetSocketAddress socketAddress = new InetSocketAddress(hostName, 80);
    when(channel.remoteAddress()).thenReturn(socketAddress);

    return context;
  }

  private static class InstrumentedTracker extends ClientConnectionTracker {
    public InstrumentedTracker(int connectionCountLimit, int connectionWarningResetThreshold) {
      super(connectionCountLimit, connectionWarningResetThreshold);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      ClientConnectionTracker.ConnectionStats stat = getStatsByContext(ctx);
      Assert.assertNotNull(stat);
      LOG.warn("in Read stats before: {}", stat);
      super.channelRead(ctx, msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      ClientConnectionTracker.ConnectionStats stat = getStatsByContext(ctx);
      Assert.assertNotNull(stat);
      int activeReqeustBefore = stat.activeRequestCount();
      LOG.warn("in Write stats: {}", stat);
      if (msg instanceof HttpRequest) {
        LOG.warn("in Read stats after: {}", stat);
        Assert.assertTrue(activeReqeustBefore > 0);
      }
      super.write(ctx, msg, promise);
      int activeRequestAfter = stat.activeRequestCount();
      if (msg instanceof HttpResponse) {
        LOG.warn("in Write stats after: {}", stat);
        Assert.assertEquals(activeReqeustBefore - 1, activeRequestAfter);
      }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
      ClientConnectionTracker.ConnectionStats stat = getStatsByContext(ctx);
      Assert.assertNotNull(stat);
      int activeCountBefore = stat.activeRequestCount();
      LOG.warn("in Close stats: {}", stat);
      super.channelInactive(ctx);
      LOG.warn("in Close map: {}", stat);
      if (activeCountBefore == 1) {
        Assert.assertEquals(statsMap().values(), 0);
      } else {
        Assert.assertEquals(activeCountBefore - 1, stat.connectionCount());
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      ClientConnectionTracker.ConnectionStats stat = getStatsByContext(ctx);
      Assert.assertNotNull(stat);
      int activeCountBefore = stat.connectionCount();
      LOG.warn("in InActive stats: {}", statsMap().values().iterator().next());
      try {
        super.channelInactive(ctx);
      } catch (Throwable t) {
        LOG.warn("ignoring reset by peer {}", t.getMessage());
      }
      LOG.warn("in InActive after map: {}", statsMap().values());
      if (activeCountBefore == 1) {
        Assert.assertEquals(statsMap().values().size(), 0);
      } else {
        Assert.assertEquals(activeCountBefore - 1, stat.connectionCount());
      }
    }

  }

  static final FullHttpResponse RESPONSE = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      HttpResponseStatus.OK,
      Unpooled.copiedBuffer("Hola", StandardCharsets.US_ASCII));

  private ServerBootstrap prepareServer(ClientConnectionTracker tracker) {
    ServerBootstrap sb = new ServerBootstrap().group(_group)
        .channel(NioServerSocketChannel.class)
        .childHandler(new io.netty.channel.ChannelInitializer<Channel>() {
          @Override
          protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast(new HttpServerCodec(), tracker, new SimpleChannelInboundHandler<HttpRequest>() {
              @Override
              protected void channelRead0(ChannelHandlerContext ctx, HttpRequest msg) {
                ctx.channel().writeAndFlush(RESPONSE);
              }

              @Override
              public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                if (cause instanceof IOException && cause.getMessage().contains("Connection reset by peer")) {
                  LOG.info("Client closed connection: ");
                } else {
                  LOG.warn("Got an exception: ", cause);
                }
              }

            });
          }
        });
    return sb;
  }

  private Bootstrap prepareClient() {

    return new Bootstrap().group(_group).channel(NioSocketChannel.class).handler(new ChannelInitializer<Channel>() {
      @Override
      protected void initChannel(Channel ch) throws Exception {
        ch.pipeline()
            .addLast(
                new HttpClientCodec(),
                new HttpObjectAggregator(1024 * 1024),
                new SimpleChannelInboundHandler<HttpResponse>() {
                  @Override
                  protected void channelRead0(ChannelHandlerContext ctx, HttpResponse msg) {
                    LOG.warn("got response from Server: {}", msg);
                  }
                });
      }
    });
  }

  @Test(groups = "unit")
  public void testSimpleActiveRequest() throws InterruptedException, UnknownHostException, ExecutionException {
    InstrumentedTracker clientConnectionTracker = new InstrumentedTracker(200, 100);
    ;
    ChannelGroup channelGroup = new DefaultChannelGroup(_group.next());
    try {
      ServerBootstrap sb = prepareServer(clientConnectionTracker);
      Bootstrap cb = prepareClient();

      Channel serverChannel = sb.bind(new InetSocketAddress(0)).sync().channel();
      channelGroup.add(serverChannel);
      int port = ((InetSocketAddress) serverChannel.localAddress()).getPort();

      Channel clientChannel = cb.connect(new InetSocketAddress(NetUtil.LOCALHOST, port)).sync().channel();
      channelGroup.add(clientChannel);

      Map<InetAddress, ClientConnectionTracker.ConnectionStats> map = clientConnectionTracker.statsMap();
      Assert.assertEquals(map.values().size(), 1);
      ClientConnectionTracker.ConnectionStats stats = map.values().iterator().next();
      Assert.assertNotNull(stats);
      Assert.assertEquals(stats.activeRequestCount(), 0);
      Assert.assertEquals(stats.connectionCount(), 1);

      FullHttpRequest request =
          new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/foo", Unpooled.EMPTY_BUFFER);
      clientChannel.writeAndFlush(request).sync().get();
      // after flush, this should be zero
      Assert.assertEquals(stats.activeRequestCount(), 0);
      Assert.assertEquals(stats.connectionCount(), 1);
      clientChannel.close().sync();
    } finally {
      channelGroup.close().sync();
    }
  }

  @Test(groups = "unit")
  public void givenAClientConnectionWentOverLimitWarningFired() throws Exception {

    int limit = 3;
    int resetThreshold = 1;
    ClientConnectionTracker spy = spy(new ClientConnectionTracker(limit, resetThreshold));
    String hostAddress = "www.linkedIn.com";

    spy.channelActive(preparedMockContextWithDifferentIpAddress(hostAddress));
    spy.channelActive(preparedMockContextWithDifferentIpAddress(hostAddress));
    verify(spy, times(2)).checkConnectionLimit(any(ChannelHandlerContext.class));
    verify(spy, times(0))
        .whenOverLimit(any(ClientConnectionTracker.ConnectionStats.class), any(ChannelHandlerContext.class));
    spy.channelActive(preparedMockContextWithDifferentIpAddress(hostAddress));
    spy.channelActive(preparedMockContextWithDifferentIpAddress(hostAddress));
    verify(spy, times(1))
        .whenOverLimit(any(ClientConnectionTracker.ConnectionStats.class), any(ChannelHandlerContext.class));

    // reduce two connections
    spy.channelInactive(preparedMockContextWithDifferentIpAddress(hostAddress));
    spy.channelInactive(preparedMockContextWithDifferentIpAddress(hostAddress));
    spy.channelInactive(preparedMockContextWithDifferentIpAddress(hostAddress));
    spy.channelInactive(preparedMockContextWithDifferentIpAddress(hostAddress));
    // add one more and there should not be any warning
    spy.channelActive(preparedMockContextWithDifferentIpAddress(hostAddress));
    verify(spy, times(5)).checkConnectionLimit(any(ChannelHandlerContext.class));
    verify(spy, times(1))
        .whenOverLimit(any(ClientConnectionTracker.ConnectionStats.class), any(ChannelHandlerContext.class));
  }
}
