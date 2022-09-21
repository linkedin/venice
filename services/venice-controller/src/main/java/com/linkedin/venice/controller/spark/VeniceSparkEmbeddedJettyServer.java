package com.linkedin.venice.controller.spark;

import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
import spark.embeddedserver.EmbeddedServer;
import spark.embeddedserver.jetty.websocket.WebSocketHandlerWrapper;
import spark.embeddedserver.jetty.websocket.WebSocketServletContextHandlerFactory;
import spark.ssl.SslStores;


/**
 * Spark server implementation
 *
 * @author Per Wendel
 */
public class VeniceSparkEmbeddedJettyServer implements EmbeddedServer {
  private static final int SPARK_DEFAULT_PORT = 4567;
  private static final String NAME = "Spark";

  private final VeniceProperties jettyConfigOverrides;
  private final Handler handler;
  private Server server;

  private static final Logger LOGGER = LogManager.getLogger(VeniceSparkEmbeddedJettyServer.class);

  private Map<String, WebSocketHandlerWrapper> webSocketHandlers;
  private Optional<Long> webSocketIdleTimeoutMillis;

  private ThreadPool threadPool = null;
  private boolean trustForwardHeaders = true; // true by default

  public VeniceSparkEmbeddedJettyServer(VeniceProperties jettyConfigOverrides, Handler handler) {
    this.handler = handler;
    this.jettyConfigOverrides = jettyConfigOverrides;
  }

  @Override
  public void configureWebSockets(
      Map<String, WebSocketHandlerWrapper> webSocketHandlers,
      Optional<Long> webSocketIdleTimeoutMillis) {

    this.webSocketHandlers = webSocketHandlers;
    this.webSocketIdleTimeoutMillis = webSocketIdleTimeoutMillis;
  }

  @Override
  public void trustForwardHeaders(boolean trust) {
    this.trustForwardHeaders = trust;
  }

  /**
   * {@inheritDoc}
   */
  @Override

  public int ignite(
      String host,
      int port,
      SslStores sslStores,
      int maxThreads,
      int minThreads,
      int threadIdleTimeoutMillis) throws Exception {

    boolean hasCustomizedConnectors = false;

    if (port == 0) {
      try (ServerSocket s = new ServerSocket(0)) {
        port = s.getLocalPort();
      } catch (IOException e) {
        LOGGER.error("Could not get first available port (port set to 0), using default: {}", SPARK_DEFAULT_PORT);
        port = SPARK_DEFAULT_PORT;
      }
    }

    // Create instance of jetty server with either default or supplied queued thread pool
    if (this.threadPool == null) {
      if (maxThreads > 0) {
        int min = minThreads > 0 ? minThreads : 8;
        int idleTimeout = threadIdleTimeoutMillis > 0 ? threadIdleTimeoutMillis : '\uea60';
        server = new Server(new QueuedThreadPool(maxThreads, min, idleTimeout));
      } else {
        server = new Server();
      }
    } else {
      this.server = new Server(threadPool);
    }

    if (this.jettyConfigOverrides != null) {
      for (final String key: this.jettyConfigOverrides.keySet()) {
        String value = this.jettyConfigOverrides.getString(key, (String) null);
        if (value != null) {
          this.server.setAttribute(key, value);
        }
      }
    }

    ServerConnector connector;

    if (sslStores == null) {
      connector = VeniceSocketConnectorFactory.createSocketConnector(server, host, port, trustForwardHeaders);
    } else {
      connector =
          VeniceSocketConnectorFactory.createSecureSocketConnector(server, host, port, sslStores, trustForwardHeaders);
    }

    Connector previousConnectors[] = server.getConnectors();
    server = connector.getServer();
    if (previousConnectors.length != 0) {
      server.setConnectors(previousConnectors);
      hasCustomizedConnectors = true;
    } else {
      server.setConnectors(new Connector[] { connector });
    }

    ServletContextHandler webSocketServletContextHandler =
        WebSocketServletContextHandlerFactory.create(webSocketHandlers, webSocketIdleTimeoutMillis);

    // Handle web socket routes
    if (webSocketServletContextHandler == null) {
      server.setHandler(handler);
    } else {
      List<Handler> handlersInList = new ArrayList<>();
      handlersInList.add(handler);

      // WebSocket handler must be the last one
      handlersInList.add(webSocketServletContextHandler);

      HandlerList handlers = new HandlerList();
      handlers.setHandlers(handlersInList.toArray(new Handler[handlersInList.size()]));
      server.setHandler(handlers);
    }

    LOGGER.info("== {} has ignited ...", NAME);
    if (hasCustomizedConnectors) {
      LOGGER.info(">> Listening on Custom Server ports!");
    } else {
      LOGGER.info(">> Listening on {}:{}", host, port);
    }

    server.start();
    return port;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void join() throws InterruptedException {
    server.join();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void extinguish() {
    LOGGER.info(">>> {} shutting down ...", NAME);
    try {
      if (server != null) {
        server.stop();
      }
    } catch (Exception e) {
      LOGGER.error("stop failed", e);
      System.exit(100); // NOSONAR
    }
    LOGGER.info("done");
  }

  @Override
  public int activeThreadCount() {
    if (server == null) {
      return 0;
    }
    return server.getThreadPool().getThreads() - server.getThreadPool().getIdleThreads();
  }

  /**
   * Sets optional thread pool for jetty server.  This is useful for overriding the default thread pool
   * behaviour for example io.dropwizard.metrics.jetty9.InstrumentedQueuedThreadPool.
   * @param threadPool thread pool
   * @return Builder pattern - returns this instance
   */
  public VeniceSparkEmbeddedJettyServer withThreadPool(ThreadPool threadPool) {
    this.threadPool = threadPool;
    return this;
  }
}
