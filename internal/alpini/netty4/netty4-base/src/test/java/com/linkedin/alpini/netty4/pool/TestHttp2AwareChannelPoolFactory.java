package com.linkedin.alpini.netty4.pool;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.monitoring.CallTracker;
import com.linkedin.alpini.netty4.handlers.HttpClientResponseHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * TODO: need to add more comprehensive tests and exercise the class fully.
 */
public class TestHttp2AwareChannelPoolFactory {
  private EventLoopGroup _eventLoopGroup;
  private long _acquireTimeoutMillis;
  private int _minConnections;
  private int _maxConnections;
  private int _maxPendingAcquires;
  private boolean _releaseHealthCheck;
  private long _healthCheckIntervalMillis;
  private ChannelHealthChecker _channelHealthChecker;
  private boolean _useFastPool;

  @BeforeMethod(groups = "unit")
  public void beforeMethod() {
    _eventLoopGroup = null;
    // defaults
    _acquireTimeoutMillis = 100;
    _minConnections = 10;
    _maxConnections = 100;
    _maxPendingAcquires = 100;
    _releaseHealthCheck = false;
    _healthCheckIntervalMillis = 30000;
    _channelHealthChecker = ChannelHealthChecker.ACTIVE;
  }

  @AfterMethod(groups = "unit")
  public void afterMethod() throws InterruptedException {
    if (_eventLoopGroup != null) {
      _eventLoopGroup.shutdownGracefully().sync();
      _eventLoopGroup = null;
    }
  }

  @DataProvider
  public Object[][] params() {
    return new Object[][] { new Object[] { false }, new Object[] { true }, };
  }

  @Test(groups = "unit", dataProvider = "params")
  public void testBasicHttp1(boolean useFastPool) throws Exception {
    _useFastPool = useFastPool;
    _eventLoopGroup = new NioEventLoopGroup(2);

    basicTestHarness(new ChannelInitializer<NioSocketChannel>() {
      @Override
      protected void initChannel(NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(new HttpServerCodec()).addLast(new ChannelInboundHandlerAdapter() {
          @Override
          public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            try {
              if (msg instanceof LastHttpContent) {
                FullHttpResponse response =
                    new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
                HttpUtil.setKeepAlive(response, true);
                HttpUtil.setContentLength(response, 0);
                ctx.writeAndFlush(response);
              }
            } finally {
              ReferenceCountUtil.release(msg);
            }
          }
        });
      }
    }, new ChannelInitializer<NioSocketChannel>() {
      @Override
      protected void initChannel(NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(new HttpObjectAggregator(1024 * 1024), new HttpClientResponseHandler());
      }
    }, pool -> {
      Assert.assertEquals(pool.getConnectedChannels(), 0);
      Future<Channel> foo = pool.acquire().sync();
      Assert.assertNotEquals(pool.getConnectedChannels(), 0);
      Time.sleep(2000);
      pool.release(foo.getNow()).sync();
      Assert.assertEquals(pool.getConnectedChannels(), _minConnections);
    });
  }

  interface TestMethod {
    void run(ManagedChannelPool pool) throws Exception;
  }

  private void basicTestHarness(ChannelHandler serverInitializer, ChannelHandler clientInitializer, TestMethod method)
      throws Exception {
    ServerBootstrap serverBootstrap = new ServerBootstrap().channel(NioServerSocketChannel.class)
        .group(_eventLoopGroup)
        .childHandler(serverInitializer);

    NioServerSocketChannel serverChannel =
        (NioServerSocketChannel) serverBootstrap.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
            .sync()
            .channel();
    try {
      Bootstrap bootstrap =
          new Bootstrap().channel(NioSocketChannel.class).group(_eventLoopGroup).handler(clientInitializer);

      Http2AwareChannelPoolFactory factory = new Http2AwareChannelPoolFactory(
          bootstrap,
          _acquireTimeoutMillis,
          _minConnections,
          _maxConnections,
          _maxPendingAcquires,
          _releaseHealthCheck,
          _healthCheckIntervalMillis,
          _channelHealthChecker,
          ignore -> CallTracker.nullTracker());
      factory.setHttp1MaxConnections(() -> _maxConnections);
      factory.setHttp1MinConnections(() -> _minConnections);

      factory.setUsingFastPool(_useFastPool);
      ChannelPoolManager channelPoolManager = Mockito.mock(ChannelPoolManager.class);
      ChannelPoolHandler channelPoolHandler = Mockito.mock(ChannelPoolHandler.class);

      Mockito.when(channelPoolManager.subpoolCount()).thenReturn(1);

      ManagedChannelPool pool =
          factory.construct(channelPoolManager, channelPoolHandler, _eventLoopGroup, serverChannel.localAddress());
      try {
        method.run(pool);
      } finally {
        pool.close();
      }
    } finally {
      serverChannel.close().await();
    }
  }

}
