package com.linkedin.venice.listener.grpc.handlers;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.ServerStatsContext;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.utils.LatencyUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GrpcOutboundStatsHandler extends VeniceServerGrpcHandler {
  private static final Logger LOGGER = LogManager.getLogger(GrpcOutboundStatsHandler.class);

  @Override
  public void processRequest(GrpcRequestContext ctx) {
    writeResponse(ctx);

    // do not exit early if there is an error, we want to record stats for error cases as well
    ServerStatsContext statsContext = ctx.getGrpcStatsContext();
    HttpResponseStatus responseStatus = statsContext.getResponseStatus();
    if (statsContext.getResponseStatus() == null) {
      LOGGER.error("Received error in outbound gRPC Stats Handler: response status could not be null");
      throw new VeniceException("response status could not be null");
    }

    if (statsContext.getStoreName() == null) {
      LOGGER.error("Received error in outbound gRPC Stats Handler: store name could not be null");
      throw new VeniceException("store name could not be null");
    }

    ServerHttpRequestStats serverHttpRequestStats = statsContext.getStoreName() == null
        ? null
        : statsContext.getCurrentStats().getStoreStats(statsContext.getStoreName());

    statsContext.recordBasicMetrics(serverHttpRequestStats);

    double elapsedTime = LatencyUtils.getElapsedTimeFromNSToMS(statsContext.getRequestStartTimeInNS());

    if (!ctx.hasError() && !responseStatus.equals(HttpResponseStatus.OK)
        || responseStatus.equals(HttpResponseStatus.NOT_FOUND)) {
      statsContext.successRequest(serverHttpRequestStats, elapsedTime);
    } else {
      statsContext.errorRequest(serverHttpRequestStats, elapsedTime);
    }
  }
}
