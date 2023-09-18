package com.linkedin.venice.listener.grpc.handlers;

import com.linkedin.venice.listener.ServerStatsContext;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.protocols.VeniceClientRequest;


public class GrpcRouterRequestHandler extends VeniceServerGrpcHandler {
  public GrpcRouterRequestHandler() {
  }

  @Override
  public void processRequest(GrpcRequestContext ctx) {
    if (ctx.hasError()) {
      invokeNextHandler(ctx);
      return;
    }

    VeniceClientRequest clientRequest = ctx.getVeniceClientRequest();
    ServerStatsContext statsContext = ctx.getGrpcStatsContext();

    RouterRequest routerRequest = clientRequest.getIsBatchRequest()
        ? MultiGetRouterRequestWrapper.parseMultiGetGrpcRequest(clientRequest)
        : GetRouterRequest.grpcGetRouterRequest(clientRequest);

    statsContext.setRequestInfo(routerRequest);

    ctx.setRouterRequest(routerRequest);
    invokeNextHandler(ctx);
  }
}
