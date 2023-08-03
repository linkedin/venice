package com.linkedin.venice.listener.grpc;

import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.utils.LatencyUtils;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GrpcHandlerPipeline {
  private static final Logger LOGGER = LogManager.getLogger(GrpcHandlerPipeline.class);
  private final ArrayList<VeniceGrpcHandler> inboundHandlers;
  private final ArrayList<VeniceGrpcHandler> outboundHandlers;
  private VeniceGrpcHandler currHandler;
  private int inboundIdx;
  private int outboundIdx;
  private boolean hasError = false;

  public GrpcHandlerPipeline() {
    inboundHandlers = new ArrayList<>();
    outboundHandlers = new ArrayList<>();

    inboundIdx = 0;
    outboundIdx = 0;
  }

  public GrpcHandlerPipeline(
      ArrayList<VeniceGrpcHandler> inboundHandlers,
      ArrayList<VeniceGrpcHandler> outboundHandlers) {
    this.inboundHandlers = inboundHandlers;
    this.outboundHandlers = outboundHandlers;

    inboundIdx = 0;
    outboundIdx = 0;
  }

  public ArrayList<VeniceGrpcHandler> getInboundHandlers() {
    return inboundHandlers;
  }

  public ArrayList<VeniceGrpcHandler> getOutboundHandlers() {
    return outboundHandlers;
  }

  public GrpcHandlerPipeline getNewPipeline() {
    return new GrpcHandlerPipeline(inboundHandlers, outboundHandlers);
  }

  public void addHandler(VeniceGrpcHandler handler) {
    inboundHandlers.add(handler);
    outboundHandlers.add(0, handler);
  }

  public void processRequest(GrpcHandlerContext ctx) {
    if (inboundIdx == inboundHandlers.size() || hasError) {
      return;
    }

    currHandler = inboundHandlers.get(inboundIdx++);
    currHandler.grpcRead(ctx, this);
  }

  public void processResponse(GrpcHandlerContext ctx) {
    if (outboundIdx == outboundHandlers.size() || hasError) {
      return;
    }

    currHandler = outboundHandlers.get(outboundIdx++);
    currHandler.grpcWrite(ctx, this);
  }

  public void onError(GrpcHandlerContext ctx) {
    hasError = true;
    if (ctx.getGrpcStatsContext() != null && ctx.getGrpcStatsContext().getStoreName() != null
        && !ctx.getGrpcStatsContext().isComplete()) {
      GrpcStatsContext statsContext = ctx.getGrpcStatsContext();
      double elapsedTime = LatencyUtils.getElapsedTimeInMs(statsContext.getRequestStartTimeInNS());
      statsContext.errorRequest(statsContext.getCurrentStats().getStoreStats(statsContext.getStoreName()), elapsedTime);
    }
    StreamObserver<VeniceServerResponse> responseObserver = ctx.getResponseObserver();
    responseObserver.onNext(ctx.getVeniceServerResponseBuilder().build());
    responseObserver.onCompleted();
    // exit handler pipeline, will send response in Service Implementation, error code is set in ctx...responseBuilder
  }

  public boolean hasError() {
    return hasError;
  }
}
