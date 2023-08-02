package com.linkedin.venice.listener.grpc;

import java.util.ArrayList;


public class GrpcHandlerPipeline {
  private ArrayList<VeniceGrpcHandler> inboundHandlers;
  private ArrayList<VeniceGrpcHandler> outboundHandlers;
  private VeniceGrpcHandler currHandler;
  private int inboundIdx;
  private int outboundIdx;

  public GrpcHandlerPipeline() {
    inboundHandlers = new ArrayList<>();
    outboundHandlers = new ArrayList<>();

    inboundIdx = 0;
    outboundIdx = 0;
  }

  public void addHandler(VeniceGrpcHandler handler) {
    inboundHandlers.add(handler);
    outboundHandlers.add(0, handler);
  }

  public void processRequest(GrpcHandlerContext ctx) {
    if (inboundIdx == inboundHandlers.size()) {
      return;
    }

    currHandler = inboundHandlers.get(inboundIdx);
    currHandler.grpcRead(ctx, this);
    inboundIdx++;
  }

  public void processResponse(GrpcHandlerContext ctx) {
    if (outboundIdx == outboundHandlers.size()) {
      return;
    }

    currHandler = outboundHandlers.get(outboundIdx);
    currHandler.grpcWrite(ctx, this);
    outboundIdx++;
  }

  public void onError(GrpcHandlerContext ctx) {
    // exit handler pipeline, will send response in Service Implementation, error code is set in ctx...responseBuilder
  }

}
