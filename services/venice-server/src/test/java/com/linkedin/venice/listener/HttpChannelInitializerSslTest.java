package com.linkedin.venice.listener;

import com.linkedin.alpini.base.ssl.SslFactory;
import com.linkedin.alpini.netty4.ssl.SslInitializer;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.DaemonThreadFactory;
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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests that a single cached SSL factory can correctly handle multiple concurrent
 * SSL connections, ensuring the factory is created once and reused.
 */
public class HttpChannelInitializerSslTest {
  private static final int NUM_CONNECTIONS = 10;
  private static final String SSL_HANDSHAKE_EXECUTOR_THREAD_PREFIX = "SSLHandShakeThread";

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

    // Server uses the same cached SSL factory for all connections.
    // Bind to port 0 to let the OS assign an available port atomically, avoiding
    // the race condition of closing a temporary ServerSocket and then rebinding.
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

    serverChannel = serverBootstrap.bind(0).sync().channel();
    serverPort = ((InetSocketAddress) serverChannel.localAddress()).getPort();

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

  @Test(timeOut = 30000)
  public void testSslHandshakeDelegatedTasksRunOnConfiguredExecutor() throws Exception {
    SSLFactory sslFactory = SslUtils.getVeniceLocalSslFactory();
    SslFactory cachedAlpiniSslFactory = SslUtils.toAlpiniSSLFactory(sslFactory);

    CountDownLatch handshakeComplete = new CountDownLatch(1);
    CountDownLatch delegatedTaskExecuted = new CountDownLatch(1);
    AtomicInteger delegatedTaskExecutionCount = new AtomicInteger(0);
    List<String> delegatedTaskThreadNames = new ArrayList<>();
    List<Integer> delegatedTaskThreadPriorities = new ArrayList<>();
    ThreadPoolExecutor sslHandshakeExecutor = new ThreadPoolExecutor(
        1,
        1,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        new DaemonThreadFactory(SSL_HANDSHAKE_EXECUTOR_THREAD_PREFIX, Thread.NORM_PRIORITY, null)) {
      @Override
      protected void beforeExecute(Thread thread, Runnable runnable) {
        super.beforeExecute(thread, runnable);
        synchronized (delegatedTaskThreadNames) {
          delegatedTaskThreadNames.add(thread.getName());
        }
        synchronized (delegatedTaskThreadPriorities) {
          delegatedTaskThreadPriorities.add(thread.getPriority());
        }
        delegatedTaskExecutionCount.incrementAndGet();
        delegatedTaskExecuted.countDown();
      }
    };

    Channel clientChannel = null;
    try {
      ServerBootstrap serverBootstrap = new ServerBootstrap();
      serverBootstrap.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
              SslInitializer sslInitializer = new SslInitializer(cachedAlpiniSslFactory, false);
              sslInitializer.enableSslTaskExecutor(sslHandshakeExecutor);
              ch.pipeline().addLast(sslInitializer);
            }
          })
          .option(ChannelOption.SO_BACKLOG, 128)
          .childOption(ChannelOption.SO_KEEPALIVE, true);

      serverChannel = serverBootstrap.bind(0).sync().channel();
      serverPort = ((InetSocketAddress) serverChannel.localAddress()).getPort();

      SSLContext clientSslContext = sslFactory.getSSLContext();
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
                    Assert.assertTrue(handshakeEvent.isSuccess(), "SSL handshake should succeed");
                    handshakeComplete.countDown();
                  }
                  ctx.fireUserEventTriggered(evt);
                }
              });
            }
          });

      clientChannel = clientBootstrap.connect("localhost", serverPort).sync().channel();

      Assert.assertTrue(handshakeComplete.await(15, TimeUnit.SECONDS), "SSL handshake did not complete in time");
      Assert.assertTrue(
          delegatedTaskExecuted.await(5, TimeUnit.SECONDS),
          "Expected Netty SSL delegated tasks to execute on the configured sslHandshakeExecutor");
      Assert.assertTrue(delegatedTaskExecutionCount.get() > 0, "Expected at least one SSL delegated task execution");
      synchronized (delegatedTaskThreadNames) {
        Assert.assertTrue(
            delegatedTaskThreadNames.stream().allMatch(name -> name.startsWith(SSL_HANDSHAKE_EXECUTOR_THREAD_PREFIX)),
            "SSL delegated tasks should run on the configured executor threads, but ran on: "
                + delegatedTaskThreadNames);
      }
      synchronized (delegatedTaskThreadPriorities) {
        Assert.assertTrue(
            delegatedTaskThreadPriorities.stream().allMatch(priority -> priority == Thread.NORM_PRIORITY),
            "SSL delegated tasks should run on normal-priority executor threads, but priorities were: "
                + delegatedTaskThreadPriorities);
      }
    } finally {
      if (clientChannel != null) {
        clientChannel.close().sync();
      }
      sslHandshakeExecutor.shutdownNow();
      Assert.assertTrue(sslHandshakeExecutor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }
}
