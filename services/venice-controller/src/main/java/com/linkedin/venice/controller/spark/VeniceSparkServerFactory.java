package com.linkedin.venice.controller.spark;

import com.linkedin.venice.utils.VeniceProperties;
import org.eclipse.jetty.util.thread.ThreadPool;
import spark.ExceptionMapper;
import spark.embeddedserver.EmbeddedServer;
import spark.embeddedserver.EmbeddedServerFactory;
import spark.embeddedserver.jetty.JettyHandler;
import spark.http.matching.MatcherFilter;
import spark.route.Routes;
import spark.staticfiles.StaticFilesConfiguration;


/**
 * Creates instances of embedded jetty containers.
 */
public class VeniceSparkServerFactory implements EmbeddedServerFactory {
  private ThreadPool threadPool;
  private boolean httpOnly = true;
  private VeniceProperties jettyConfig;

  public VeniceSparkServerFactory(VeniceProperties jettyConfig) {
    super();
    this.jettyConfig = jettyConfig;
  }

  public EmbeddedServer create(
      Routes routeMatcher,
      StaticFilesConfiguration staticFilesConfiguration,
      ExceptionMapper exceptionMapper,
      boolean hasMultipleHandler) {
    MatcherFilter matcherFilter =
        new MatcherFilter(routeMatcher, staticFilesConfiguration, exceptionMapper, false, hasMultipleHandler);
    matcherFilter.init(null);

    JettyHandler handler = new JettyHandler(matcherFilter);
    handler.getSessionCookieConfig().setHttpOnly(httpOnly);
    return (new VeniceSparkEmbeddedJettyServer(jettyConfig, handler)).withThreadPool(this.threadPool);
  }

  /**
   * Sets optional thread pool for jetty server.  This is useful for overriding the default thread pool
   * behaviour for example io.dropwizard.metrics.jetty9.InstrumentedQueuedThreadPool.
   *
   * @param threadPool thread pool
   * @return Builder pattern - returns this instance
   */
  public VeniceSparkServerFactory withThreadPool(ThreadPool threadPool) {
    this.threadPool = threadPool;
    return this;
  }
}
