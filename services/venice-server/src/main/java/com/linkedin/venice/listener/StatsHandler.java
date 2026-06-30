package com.linkedin.venice.listener;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.net.SocketAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StatsHandler extends ChannelDuplexHandler {
  private static final Logger LOGGER = LogManager.getLogger(StatsHandler.class);
  private final ServerStatsContext serverStatsContext;
  private final AggServerHttpRequestStats singleGetStats;
  private final AggServerHttpRequestStats multiGetStats;
  private final AggServerHttpRequestStats computeStats;
  private final ServerLoadControllerHandler loadControllerHandler;
  /**
   * Used to rate-limit the {@link #logUnattributedError} warnings so a noisy probe/scanner cannot flood the logs.
   * Keyed on (status, remote, principal, uri), so each distinct source/cause is still logged within the filter window.
   * Defaults to the process-wide shared filter so rate-limiting spans all channels; an alternate instance can be
   * injected for deterministic testing.
   */
  private final RedundantExceptionFilter redundantExceptionFilter;

  public StatsHandler(
      AggServerHttpRequestStats singleGetStats,
      AggServerHttpRequestStats multiGetStats,
      AggServerHttpRequestStats computeStats,
      ServerLoadControllerHandler loadControllerHandler) {
    this(
        singleGetStats,
        multiGetStats,
        computeStats,
        loadControllerHandler,
        RedundantExceptionFilter.getRedundantExceptionFilter());
  }

  StatsHandler(
      AggServerHttpRequestStats singleGetStats,
      AggServerHttpRequestStats multiGetStats,
      AggServerHttpRequestStats computeStats,
      ServerLoadControllerHandler loadControllerHandler,
      RedundantExceptionFilter redundantExceptionFilter) {
    this.singleGetStats = singleGetStats;
    this.multiGetStats = multiGetStats;
    this.computeStats = computeStats;
    this.loadControllerHandler = loadControllerHandler;
    this.redundantExceptionFilter = redundantExceptionFilter;

    this.serverStatsContext = new ServerStatsContext(singleGetStats, multiGetStats, computeStats);
  }

  public ServerStatsContext getNewStatsContext() {
    return new ServerStatsContext(singleGetStats, multiGetStats, computeStats);
  }

  public void setResponseStatus(HttpResponseStatus status) {
    serverStatsContext.setResponseStatus(status);
  }

  public void setStoreName(String name) {
    serverStatsContext.setStoreName(name);
  }

  public void setMetadataRequest(boolean metadataRequest) {
    serverStatsContext.setMetadataRequest(metadataRequest);
  }

  public void setRequestTerminatedEarly() {
    serverStatsContext.setRequestTerminatedEarly();
  }

  public void setRequestInfo(RouterRequest request) {
    serverStatsContext.setRequestInfo(request);
  }

  public void setRequestSize(int requestSizeInBytes) {
    serverStatsContext.setRequestSize(requestSizeInBytes);
  }

  public long getRequestStartTimeInNS() {
    return serverStatsContext.getRequestStartTimeInNS();
  }

  public ServerStatsContext getServerStatsContext() {
    return serverStatsContext;
  }

  public void setMisroutedStoreVersionRequest(boolean misroutedStoreVersionRequest) {
    serverStatsContext.setMisroutedStoreVersion(misroutedStoreVersionRequest);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (serverStatsContext.isNewRequest()) {
      // Reset for every request
      serverStatsContext.resetContext();
    }
    /**
     * For a single 'channelRead' invocation, Netty will guarantee all the following 'channelRead' functions
     * registered by the pipeline to be executed in the same thread.
     */
    ctx.fireChannelRead(msg);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws VeniceException {
    ChannelFuture future = ctx.writeAndFlush(msg);
    long beforeFlushTimestampNs = System.nanoTime();
    future.addListener((result) -> {
      // reset the StatsHandler for the new request. This is necessary since instances are channel-based
      // and channels are ready for the future requests as soon as the current has been handled.
      serverStatsContext.setNewRequest();

      if (serverStatsContext.getResponseStatus() == null) {
        throw new VeniceException("request status could not be null");
      }

      // we don't record if it is a metatadata request
      if (serverStatsContext.isMetadataRequest()) {
        return;
      }

      /**
       * TODO: Need to do more investigation to figure out why this callback could be triggered
       * multiple times for a single request
       */
      if (!serverStatsContext.isStatCallBackExecuted()) {
        serverStatsContext.setFlushLatency(LatencyUtils.getElapsedTimeFromNSToMS(beforeFlushTimestampNs));
        ServerHttpRequestStats serverHttpRequestStats = serverStatsContext.getStoreName() == null
            ? null
            : serverStatsContext.getCurrentStats().getStoreStats(serverStatsContext.getStoreName());
        serverStatsContext.recordBasicMetrics(serverHttpRequestStats);
        double elapsedTime = LatencyUtils.getElapsedTimeFromNSToMS(serverStatsContext.getRequestStartTimeInNS());
        // if ResponseStatus is either OK or NOT_FOUND and the channel write is succeed,
        // records a successRequest in stats. Otherwise, records a errorRequest in stats
        // For TOO_MANY_REQUESTS do not record either success or error. Recording as success would give out
        // wrong interpretation of latency, recording error would give out impression that server failed to serve
        if (result.isSuccess() && (serverStatsContext.getResponseStatus().equals(OK)
            || serverStatsContext.getResponseStatus().equals(NOT_FOUND))) {
          serverStatsContext.successRequest(serverHttpRequestStats, elapsedTime);
        } else if (!serverStatsContext.getResponseStatus().equals(TOO_MANY_REQUESTS)) {
          serverStatsContext.errorRequest(serverHttpRequestStats, elapsedTime);
          if (serverHttpRequestStats == null) {
            /**
             * No store name was resolved for this request, so the error only moved the aggregate
             * {@code total--error_request} metric and is invisible at the store level. Such requests are rejected
             * upstream of {@link RouterRequestHttpHandler} (e.g. malformed-URI BAD_REQUEST in the store ACL handler,
             * an unauthorized 401/403, or an unexpected 5xx) and are otherwise unlogged. Log the source here so they
             * can be attributed.
             */
            logUnattributedError(ctx, serverStatsContext);
          }
        }
        if (loadControllerHandler != null) {
          loadControllerHandler.recordLatency(
              serverStatsContext.getRequestType(),
              elapsedTime,
              serverStatsContext.getResponseStatus().code());
        }
        serverStatsContext.setStatCallBackExecuted(true);
      }
    });
  }

  /**
   * Logs the source of an error that was recorded against the aggregate {@code total--error_request} metric without a
   * store attribution. These are requests rejected/failed before a store name was resolved (malformed-URI BAD_REQUEST,
   * unauthorized 401/403, or an unexpected 5xx), which are otherwise invisible because the rejection points either do
   * not log or log only at DEBUG.
   *
   * <p>Everything here is best-effort and fully guarded: this is observability only and must never disrupt request
   * handling. The {@code remote}, client {@code principal} and {@code uri} are all best-effort and may be absent; the
   * URI in particular is usually {@code null} for these upstream rejections (see {@link ServerStatsContext#getRequestUri()}).
   */
  private void logUnattributedError(ChannelHandlerContext ctx, ServerStatsContext statsContext) {
    try {
      HttpResponseStatus status = statsContext.getResponseStatus();
      SocketAddress remote = ctx.channel() == null ? null : ctx.channel().remoteAddress();
      // extractClientPrincipal is already best-effort: it returns "" rather than throwing when there is no client cert.
      String principal = ServerHandlerUtils.extractClientPrincipal(ctx);
      if (principal == null || principal.isEmpty()) {
        principal = "none";
      }
      // Best-effort: include the URI only when something upstream managed to capture it, otherwise "unknown".
      String uri = statsContext.getRequestUri();
      if (uri == null || uri.isEmpty()) {
        uri = "unknown";
      }
      String filterKey = "UNATTRIBUTED_ERROR_REQUEST:" + (status == null ? "?" : status.code()) + ':' + remote + ':'
          + principal + ':' + uri;
      if (!redundantExceptionFilter.isRedundantException(filterKey)) {
        LOGGER.warn(
            "Recorded an error_request with no store attribution (only the aggregate total--error_request metric "
                + "moved); the request was rejected before a store name was resolved. status={}, remote={}, "
                + "clientPrincipal={}, uri={}",
            status,
            remote,
            principal,
            uri);
      }
    } catch (Throwable t) {
      // Observability must never disrupt request handling.
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Failed to log unattributed error_request source; continuing.", t);
      }
    }
  }
}
