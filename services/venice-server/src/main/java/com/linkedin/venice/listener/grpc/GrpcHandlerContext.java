package com.linkedin.venice.listener.grpc;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceServerResponse;
import io.grpc.stub.StreamObserver;


public class GrpcHandlerContext {
  private VeniceClientRequest veniceClientRequest;
  private VeniceServerResponse.Builder veniceServerResponseBuilder;
  private StreamObserver<VeniceServerResponse> responseObserver;

  private boolean isCompleted = false;
  private RouterRequest routerRequest;
  private ReadResponse readResponse;

  public GrpcHandlerContext(
      VeniceClientRequest veniceClientRequest,
      VeniceServerResponse.Builder veniceServerResponseBuilder,
      StreamObserver<VeniceServerResponse> responseObserver) {
    this.veniceClientRequest = veniceClientRequest;
    this.veniceServerResponseBuilder = veniceServerResponseBuilder;
    this.responseObserver = responseObserver;
  }

  public VeniceClientRequest getVeniceClientRequest() {
    return veniceClientRequest;
  }

  public void setVeniceClientRequest(VeniceClientRequest veniceClientRequest) {
    this.veniceClientRequest = veniceClientRequest;
  }

  public VeniceServerResponse.Builder getVeniceServerResponseBuilder() {
    return veniceServerResponseBuilder;
  }

  public void setVeniceServerResponseBuilder(VeniceServerResponse.Builder veniceServerResponseBuilder) {
    this.veniceServerResponseBuilder = veniceServerResponseBuilder;
  }

  public StreamObserver<VeniceServerResponse> getResponseObserver() {
    return responseObserver;
  }

  public void setResponseObserver(StreamObserver<VeniceServerResponse> responseObserver) {
    this.responseObserver = responseObserver;
  }

  public RouterRequest getRouterRequest() {
    return routerRequest;
  }

  public void setRouterRequest(RouterRequest routerRequest) {
    this.routerRequest = routerRequest;
  }

  public ReadResponse getReadResponse() {
    return readResponse;
  }

  public void setReadResponse(ReadResponse readResponse) {
    this.readResponse = readResponse;
  }

  public void setCompleted() {
    isCompleted = true;
  }

  public boolean isCompleted() {
    return isCompleted;
  }
}
