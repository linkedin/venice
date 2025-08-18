package com.linkedin.venice.listener.grpc.handlers;

import com.linkedin.venice.listener.ServerStatsContext;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.listener.request.CountByValueRouterRequestWrapper;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.protocols.VeniceClientRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;


public class GrpcRouterRequestHandler extends VeniceServerGrpcHandler {
  public GrpcRouterRequestHandler() {
  }

  @Override
  public void processRequest(GrpcRequestContext ctx) {
    if (ctx.hasError()) {
      invokeNextHandler(ctx);
      return;
    }

    RouterRequest routerRequest;
    ServerStatsContext statsContext = ctx.getGrpcStatsContext();

    if (ctx.isCountByValueRequest()) {
      // Handle CountByValue requests
      routerRequest = new CountByValueRouterRequestWrapper(
          ctx.getCountByValueRequest().getResourceName(),
          ctx.getCountByValueRequest(),
          new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/count_by_value"));
    } else {
      // Handle regular get/batchget requests
      VeniceClientRequest clientRequest = ctx.getVeniceClientRequest();
      routerRequest = clientRequest.getIsBatchRequest()
          ? MultiGetRouterRequestWrapper.parseMultiGetGrpcRequest(clientRequest)
          : GetRouterRequest.grpcGetRouterRequest(clientRequest);
    }

    statsContext.setRequestInfo(routerRequest);
    ctx.setRouterRequest(routerRequest);
    invokeNextHandler(ctx);
  }
}
