package com.linkedin.venice.listener;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.reliability.LoadController;
import com.linkedin.venice.stats.ServerLoadStats;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This handler is used to control the load on the server by measuring the read latency.
 * If there are many requests, which are suffering from the high latency, the server will
 * start rejecting the requests and return the service overloaded response, so that the
 * clients can back off upon overloaded signals.
 */
@ChannelHandler.Sharable
public class ServerLoadControllerHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger LOGGER = LogManager.getLogger(ServerLoadControllerHandler.class);
  private static final String STORAGE_QUERY_ACTION = QueryAction.STORAGE.toString().toLowerCase();

  public static final HttpResponseStatus OVERLOADED_RESPONSE_STATUS =
      new HttpResponseStatus(HttpConstants.SC_SERVICE_OVERLOADED, "Service Overloaded");
  public static final String SERVICE_OVERLOADED_MESSAGE = "Service overloaded, please try again later";

  private final VeniceServerConfig serverConfig;
  private final LoadController loadController;
  private final ServerLoadStats loadStats;

  public ServerLoadControllerHandler(VeniceServerConfig serverConfig, ServerLoadStats loadStats) {
    this.serverConfig = serverConfig;
    this.loadController = LoadController.newBuilder()
        .setWindowSizeInSec(serverConfig.getLoadControllerWindowSizeInSec())
        .setAcceptMultiplier(serverConfig.getLoadControllerAcceptMultiplier())
        .setMaxRejectionRatio(serverConfig.getLoadControllerMaxRejectionRatio())
        .setRejectionRatioUpdateIntervalInSec(serverConfig.getLoadControllerRejectionRatioUpdateIntervalInSec())
        .build();
    this.loadStats = loadStats;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpRequest msg) throws Exception {
    if (!msg.uri().contains(STORAGE_QUERY_ACTION)) {
      ReferenceCountUtil.retain(msg);
      ctx.fireChannelRead(msg);
      return;
    }
    // Only throttle regular requests
    loadStats.recordRejectionRatio(loadController.getRejectionRatio());

    if (loadController.shouldRejectRequest()) {
      loadStats.recordRejectedRequest();
      ctx.writeAndFlush(new HttpShortcutResponse(SERVICE_OVERLOADED_MESSAGE, OVERLOADED_RESPONSE_STATUS));
    } else {
      ReferenceCountUtil.retain(msg);
      ctx.fireChannelRead(msg);
    }
  }

  public void recordLatency(RequestType requestType, double latency, int responseStatusCode) {
    /**
     * Only record the request when the response is available to avoid rejecting many requests at startup time.
     */
    loadController.recordRequest();
    loadStats.recordTotalRequest();

    if (responseStatusCode == HttpConstants.SC_SERVICE_OVERLOADED) {
      // No need to check the latency threshold.
      return;
    }
    double latencyThreshold = 0;
    switch (requestType) {
      case SINGLE_GET:
        latencyThreshold = serverConfig.getLoadControllerSingleGetLatencyAcceptThresholdMs();
        break;
      case MULTI_GET:
      case MULTI_GET_STREAMING:
        latencyThreshold = serverConfig.getLoadControllerMultiGetLatencyAcceptThresholdMs();
        break;
      case COMPUTE:
      case COMPUTE_STREAMING:
        latencyThreshold = serverConfig.getLoadControllerComputeLatencyAcceptThresholdMs();
        break;
    }
    if (latency <= latencyThreshold) {
      loadStats.recordAcceptedRequest();
      loadController.recordAccept();
    }

  }

  // For testing purpose
  LoadController getLoadController() {
    return loadController;
  }
}
