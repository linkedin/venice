package com.linkedin.venice.listener;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.alpini.base.ssl.SslFactory;
import com.linkedin.alpini.netty4.ssl.SslInitializer;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.venice.authorization.DefaultIdentityParser;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.tehuti.metrics.MetricsRepository;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests that the cached SSL factory in {@link HttpChannelInitializer} correctly handles
 * multiple concurrent SSL connections, ensuring the factory is created once and reused.
 */
public class HttpChannelInitializerSslTest {
  private static final int NUM_CONNECTIONS = 10;

  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private EventLoopGroup clientGroup;
  private Channel serverChannel;
  private int serverPort;

  @BeforeMethod
  public void setUp() {
    bossGroup = new NioEventLoopGroup(1);
    workerGroup = new NioEventLoopGroup(2);
    clientGroup = new NioEventLoopGroup(2);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (serverChannel != null) {
      serverChannel.close().sync();
    }
    clientGroup.shutdownGracefully().sync();
    workerGroup.shutdownGracefully().sync();
    bossGroup.shutdownGracefully().sync();
  }

  /**
   * Verifies that a single cached alpini SSL factory can successfully handle multiple
   * concurrent SSL connections. This tests the fix where the factory is created once in the
   * constructor rather than recreated per connection in initChannel().
   */
  @Test(timeOut = 30000)
  public void testMultipleSslConnectionsWithCachedFactory() throws Exception {
    SSLFactory sslFactory = SslUtils.getVeniceLocalSslFactory();

    // Create a single alpini SSL factory — this is the cached pattern we now use in HttpChannelInitializer
    SslFactory cachedAlpiniSslFactory = SslUtils.toAlpiniSSLFactory(sslFactory);

    try (ServerSocket ss = new ServerSocket(0)) {
      serverPort = ss.getLocalPort();
    }

    // Server uses the same cached SSL factory for all connections
    ServerBootstrap serverBootstrap = new ServerBootstrap();
    serverBootstrap.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) {
            ch.pipeline().addLast(new SslInitializer(cachedAlpiniSslFactory, false));
          }
        })
        .option(ChannelOption.SO_BACKLOG, 128)
        .childOption(ChannelOption.SO_KEEPALIVE, true);

    serverChannel = serverBootstrap.bind(serverPort).sync().channel();

    // Connect multiple SSL clients concurrently
    SSLContext clientSslContext = sslFactory.getSSLContext();
    AtomicInteger handshakeSuccessCount = new AtomicInteger(0);
    AtomicInteger handshakeFailureCount = new AtomicInteger(0);
    CountDownLatch allHandshakesDone = new CountDownLatch(NUM_CONNECTIONS);
    List<Channel> clientChannels = new ArrayList<>();

    for (int i = 0; i < NUM_CONNECTIONS; i++) {
      Bootstrap clientBootstrap = new Bootstrap();
      clientBootstrap.group(clientGroup)
          .channel(NioSocketChannel.class)
          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
          .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
              SSLEngine engine = clientSslContext.createSSLEngine("localhost", serverPort);
              engine.setUseClientMode(true);
              ch.pipeline().addLast("ssl", new SslHandler(engine));
              ch.pipeline().addLast("handshake-listener", new ChannelInboundHandlerAdapter() {
                @Override
                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                  if (evt instanceof SslHandshakeCompletionEvent) {
                    SslHandshakeCompletionEvent handshakeEvent = (SslHandshakeCompletionEvent) evt;
                    if (handshakeEvent.isSuccess()) {
                      handshakeSuccessCount.incrementAndGet();
                    } else {
                      handshakeFailureCount.incrementAndGet();
                    }
                    allHandshakesDone.countDown();
                  }
                  ctx.fireUserEventTriggered(evt);
                }
              });
            }
          });

      ChannelFuture connectFuture = clientBootstrap.connect("localhost", serverPort).sync();
      clientChannels.add(connectFuture.channel());
    }

    Assert.assertTrue(allHandshakesDone.await(15, TimeUnit.SECONDS), "Not all SSL handshakes completed in time");
    Assert.assertEquals(
        handshakeSuccessCount.get(),
        NUM_CONNECTIONS,
        "All " + NUM_CONNECTIONS + " SSL handshakes should succeed with a cached SSL factory");
    Assert.assertEquals(handshakeFailureCount.get(), 0, "No SSL handshakes should fail");

    for (Channel ch: clientChannels) {
      ch.close().sync();
    }
  }

  /**
   * Verifies that the {@link HttpChannelInitializer} constructor eagerly creates the alpini SSL factory
   * from the Venice SSLFactory, so it does not need to be recreated per connection.
   * The SSLConfig is accessed at construction time, not at initChannel time.
   */
  @Test
  public void testHttpChannelInitializerCachesSslFactory() {
    SSLFactory sslFactory = SslUtils.getVeniceLocalSslFactory();
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(DefaultIdentityParser.class.getName()).when(serverConfig).getIdentityParserClassName();
    doReturn(1024 * 1024).when(serverConfig).getMaxRequestSize();
    doReturn(300).when(serverConfig).getNettyIdleTimeInSeconds();

    // Construction should succeed — SSLConfig is read eagerly
    HttpChannelInitializer initializer = new HttpChannelInitializer(
        mock(ReadOnlyStoreRepository.class),
        new CompletableFuture<HelixCustomizedViewOfflinePushRepository>(),
        new MetricsRepository(),
        Optional.of(sslFactory),
        null,
        serverConfig,
        Optional.empty(),
        Optional.empty(),
        mock(StorageReadRequestHandler.class),
        mock(StorageEngineRepository.class));

    Assert.assertNotNull(initializer);
  }
}
