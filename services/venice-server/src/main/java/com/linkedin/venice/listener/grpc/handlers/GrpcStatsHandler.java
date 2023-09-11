package com.linkedin.venice.listener.grpc.handlers;

import com.linkedin.venice.listener.ServerStatsContext;
import com.linkedin.venice.listener.StatsHandler;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;


public class GrpcStatsHandler extends VeniceServerGrpcHandler {
  private final StatsHandler statsHandler;

  public GrpcStatsHandler(StatsHandler statsHandler) {
    this.statsHandler = statsHandler;
  }

  @Override
  public void processRequest(GrpcRequestContext ctx) {
    if (ctx.getGrpcStatsContext() == null) {
      // new request
      ServerStatsContext statsContext = statsHandler.getNewStatsContext();
      statsContext.resetContext();

      ctx.setGrpcStatsContext(statsContext);
    }

    invokeNextHandler(ctx);
  }
}
