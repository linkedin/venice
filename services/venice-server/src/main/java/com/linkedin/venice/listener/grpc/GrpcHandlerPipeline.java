package com.linkedin.venice.listener.grpc;

import java.util.ArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * We use this class to manage the pipeline of handlers for gRPC requests to follow a similar logical flow to
 * the Netty pipeline. On every new request, we create pipeline instance in order to keep track of the request
 * in the pipeline as a whole. This allows us to keep track of the state of a request with respect to the handler that
 * is currently processing the request, and allows for the handlers to be stateless.
 *
 * The order of the handlers in the pipeline is modeled after the Netty pipeline, and both are initialized in
 * {@link com.linkedin.venice.listener.HttpChannelInitializer}.
 */
public class GrpcHandlerPipeline {
  private static final Logger LOGGER = LogManager.getLogger(GrpcHandlerPipeline.class);
  private final ArrayList<VeniceGrpcHandler> inboundHandlers;
  private final ArrayList<VeniceGrpcHandler> outboundHandlers;
  private VeniceGrpcHandler currHandler;
  private int inboundIdx;
  private int outboundIdx;

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
    if (inboundIdx == inboundHandlers.size()) {
      return;
    }

    currHandler = inboundHandlers.get(inboundIdx++);
    currHandler.grpcRead(ctx, this);
  }

  public void processResponse(GrpcHandlerContext ctx) {
    if (outboundIdx == outboundHandlers.size()) {
      return;
    }

    currHandler = outboundHandlers.get(outboundIdx++);
    currHandler.grpcWrite(ctx, this);
  }
}
