package com.linkedin.venice.router;

import static com.linkedin.venice.HttpConstants.HTTP_GET;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.alpini.netty4.ssl.SslInitializer;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.router.httpclient.HttpClient5StorageNodeClient;
import com.linkedin.venice.router.httpclient.PortableHttpResponse;
import com.linkedin.venice.router.httpclient.VeniceMetaDataRequest;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Integration test to verify that when a server process is paused (accepts connections but never responds),
 * the HTTP_CLIENT_5_CLIENT will block indefinitely due to the socket timeout being set to indefinite (0).
 *
 * This test simulates the behavior observed during chaos testing where a server process is paused using SIGSTOP.
 * In such cases:
 * 1. The TCP connection is established and stays OPEN
 * 2. The server doesn't respond (as if paused)
 * 3. The HTTP client waits indefinitely because ConnectionConfig.socketTimeout = 0 (indefinite)
 * 4. The request only completes when LeakedCompletableFutureCleanupService cleans it up
 *
 * This confirms that the safety net (LeakedCompletableFutureCleanupService) is necessary for handling
 * paused servers, as the HTTP client itself has no effective timeout for this scenario.
 */
public class TestHttpClient5PausedServer {
  private static final Logger LOGGER = LogManager.getLogger(TestHttpClient5PausedServer.class);

  /**
   * Test that HTTP_CLIENT_5_CLIENT blocks indefinitely when the server accepts connections but never responds.
   * This simulates a paused server process (e.g., SIGSTOP during chaos testing).
   */
  @Test(timeOut = 30000) // Test timeout to prevent infinite hang if something goes wrong
  public void testHttpClient5BlocksIndefinitelyOnPausedServer() throws Exception {
    if (Utils.getJavaMajorVersion() < 11) {
      LOGGER.info("Skipping test - HTTP_CLIENT_5_CLIENT requires JDK 11 or above");
      return;
    }

    int port = TestUtils.getFreePort();
    CountDownLatch requestReceived = new CountDownLatch(1);

    // Start a mock H2 server that accepts connections but never responds (simulating paused server)
    PausedH2Server pausedServer = new PausedH2Server(port, requestReceived);
    pausedServer.start();

    HttpClient5StorageNodeClient client = null;
    try {
      Optional<SSLFactory> sslFactory = Optional.of(SslUtils.getVeniceLocalSslFactory());

      VeniceRouterConfig config = mock(VeniceRouterConfig.class);
      doReturn(1).when(config).getHttpClient5PoolSize();
      doReturn(2).when(config).getHttpClient5TotalIOThreadCount();
      doReturn(true).when(config).isHttpClient5SkipCipherCheck();
      // Set a short socket timeout (5 seconds) - but this won't be effective due to
      // ConnectionConfig.socketTimeout being set to 0 (indefinite) in HttpClient5Utils
      doReturn(5000).when(config).getSocketTimeout();

      client = new HttpClient5StorageNodeClient(sslFactory, config);
      Instance serverInstance = new Instance("paused-server", "localhost", port);

      // Create a simple metadata request (like a health check)
      VeniceMetaDataRequest request = new VeniceMetaDataRequest(serverInstance, "/health", HTTP_GET, true);
      // Don't set timeout - we want to test the default behavior

      CompletableFuture<PortableHttpResponse> responseFuture = new CompletableFuture<>();

      // Send request to the paused server
      client.sendRequest(request, responseFuture);

      // Wait for the server to receive the request
      boolean requestMade = requestReceived.await(10, TimeUnit.SECONDS);
      Assert.assertTrue(requestMade, "Server should have received the request");

      // Now verify that the request is blocked - it should NOT complete within 10 seconds
      // even though we configured a 5 second socket timeout.
      // This is because ConnectionConfig.socketTimeout = 0 (indefinite) in HttpClient5Utils
      // overrides the response timeout for established connections.
      try {
        responseFuture.get(10, TimeUnit.SECONDS);
        Assert.fail(
            "Request should NOT have completed - HTTP_CLIENT_5_CLIENT should block indefinitely on paused server");
      } catch (TimeoutException e) {
        // This is EXPECTED - the request is blocked indefinitely
        LOGGER.info(
            "Confirmed: HTTP_CLIENT_5_CLIENT blocks indefinitely when server is paused. "
                + "The request did not complete within 10 seconds despite 5 second socket timeout config. "
                + "This confirms that ConnectionConfig.socketTimeout=0 (indefinite) overrides the response timeout. "
                + "LeakedCompletableFutureCleanupService is necessary to handle this scenario.");
      }

      // Verify the future is still not done
      Assert.assertFalse(responseFuture.isDone(), "Request should still be pending - blocked indefinitely");

      // Cancel the pending request to allow cleaner shutdown
      responseFuture.cancel(true);
    } finally {
      // Clean up - stop server first to close connections, then close client
      pausedServer.stop();

      if (client != null) {
        try {
          client.close();
        } catch (Exception e) {
          // The HTTP client may throw exceptions during close() when there are pending requests
          // This is expected and acceptable - the main test assertion has already passed
          LOGGER.info("Expected exception during HTTP client cleanup: {}", e.getMessage());
        }
      }
    }
  }

  /**
   * A mock HTTP/2 server that accepts connections and receives requests but NEVER sends a response.
   * This simulates a server process that has been paused (e.g., via SIGSTOP).
   *
   * When a process is paused:
   * - Existing TCP connections remain open
   * - New connections can be accepted (by the OS kernel's listen backlog)
   * - But no application-level response is ever sent
   */
  private static class PausedH2Server {
    private static final Logger LOGGER = LogManager.getLogger(PausedH2Server.class);
    private final int port;
    private final CountDownLatch requestReceived;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture serverFuture;

    public PausedH2Server(int port, CountDownLatch requestReceived) {
      this.port = port;
      this.requestReceived = requestReceived;
    }

    public void start() throws Exception {
      bossGroup = new NioEventLoopGroup(1);
      workerGroup = new NioEventLoopGroup(4);

      SSLFactory sslFactory = SslUtils.getVeniceLocalSslFactory();

      ServerBootstrap bootstrap = new ServerBootstrap();
      bootstrap.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
              ch.pipeline()
                  .addLast(new SslInitializer(SslUtils.toAlpiniSSLFactory(sslFactory), false))
                  .addLast(getServerAPNHandler());
            }
          })
          .option(ChannelOption.SO_BACKLOG, 128)
          .childOption(ChannelOption.SO_KEEPALIVE, true)
          .option(ChannelOption.SO_REUSEADDR, true)
          .childOption(ChannelOption.TCP_NODELAY, true);

      serverFuture = bootstrap.bind(port).sync();
      LOGGER.info("Paused H2 mock server started on port {}", port);
    }

    public void stop() throws Exception {
      if (serverFuture != null) {
        serverFuture.channel().close();
      }
      if (workerGroup != null) {
        workerGroup.shutdownGracefully();
      }
      if (bossGroup != null) {
        bossGroup.shutdownGracefully();
      }
      LOGGER.info("Paused H2 mock server stopped");
    }

    /**
     * ALPN handler that sets up HTTP/2 pipeline with a handler that never responds.
     */
    private ApplicationProtocolNegotiationHandler getServerAPNHandler() {
      return new ApplicationProtocolNegotiationHandler(ApplicationProtocolNames.HTTP_1_1) {
        @Override
        protected void configurePipeline(ChannelHandlerContext ctx, String protocol) throws Exception {
          if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
            LOGGER.info("Received ALPN request for HTTP/2");
            Http2Settings settings = new Http2Settings().maxConcurrentStreams(100)
                .maxFrameSize(8 * 1024 * 1024)
                .initialWindowSize(8 * 1024 * 1024)
                .headerTableSize(4096)
                .maxHeaderListSize(8192);
            ctx.pipeline()
                .addLast(
                    Http2FrameCodecBuilder.forServer().initialSettings(settings).build(),
                    new PausedServerHandler(requestReceived));
            return;
          }
          throw new IllegalStateException("Protocol: " + protocol + " not supported");
        }
      };
    }
  }

  /**
   * Handler that receives HTTP/2 requests but NEVER responds - simulating a paused server.
   */
  private static class PausedServerHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOGGER = LogManager.getLogger(PausedServerHandler.class);
    private final CountDownLatch requestReceived;

    public PausedServerHandler(CountDownLatch requestReceived) {
      this.requestReceived = requestReceived;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof Http2HeadersFrame) {
        Http2HeadersFrame headersFrame = (Http2HeadersFrame) msg;
        Http2Headers headers = headersFrame.headers();
        LOGGER.info(
            "Request received: {} {} - will NOT respond (simulating paused server)",
            headers.method(),
            headers.path());
        requestReceived.countDown();
        // Intentionally do NOT send any response - this simulates a paused server
        // The client will wait indefinitely for a response that never comes
      } else {
        super.channelRead(ctx, msg);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      LOGGER.warn("Exception in paused server handler", cause);
      // Don't close the connection - keep it open like a paused server would
    }
  }
}
