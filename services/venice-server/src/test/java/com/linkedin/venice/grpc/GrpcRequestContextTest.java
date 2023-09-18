package com.linkedin.venice.grpc;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.listener.ServerStatsContext;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceServerResponse;
import io.grpc.stub.StreamObserver;
import org.testng.annotations.Test;


public class GrpcRequestContextTest {
  @Test
  public void testSetAndGetGrpcStatsContext() {
    ServerStatsContext context = new ServerStatsContext(null, null, null);
    GrpcRequestContext handlerContext = new GrpcRequestContext(null, null, null);
    handlerContext.setGrpcStatsContext(context);

    assertEquals(context, handlerContext.getGrpcStatsContext());
  }

  @Test
  public void testSetAndGetVeniceClientRequest() {
    VeniceClientRequest request = VeniceClientRequest.newBuilder().build();
    GrpcRequestContext handlerContext = new GrpcRequestContext(null, null, null);
    handlerContext.setVeniceClientRequest(request);

    assertEquals(request, handlerContext.getVeniceClientRequest());
  }

  @Test
  public void testSetAndGetVeniceServerResponseBuilder() {
    VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
    GrpcRequestContext handlerContext = new GrpcRequestContext(null, null, null);
    handlerContext.setVeniceServerResponseBuilder(builder);

    assertEquals(builder, handlerContext.getVeniceServerResponseBuilder());
  }

  @Test
  public void testSetAndGetResponseObserver() {
    StreamObserver<VeniceServerResponse> observer = new StreamObserver<VeniceServerResponse>() {
      @Override
      public void onNext(VeniceServerResponse value) {
      }

      @Override
      public void onError(Throwable t) {
      }

      @Override
      public void onCompleted() {
      }
    };

    GrpcRequestContext handlerContext = new GrpcRequestContext(null, null, null);
    handlerContext.setResponseObserver(observer);

    assertEquals(observer, handlerContext.getResponseObserver());
  }

  @Test
  public void testSetAndGetRouterRequest() {
    RouterRequest request = mock(RouterRequest.class);
    GrpcRequestContext handlerContext = new GrpcRequestContext(null, null, null);
    handlerContext.setRouterRequest(request);

    assertEquals(request, handlerContext.getRouterRequest());
  }

  @Test
  public void testSetAndGetReadResponse() {
    ReadResponse response = mock(ReadResponse.class);
    GrpcRequestContext handlerContext = new GrpcRequestContext(null, null, null);
    handlerContext.setReadResponse(response);

    assertEquals(response, handlerContext.getReadResponse());
  }

  @Test
  public void testIsCompleted() {
    GrpcRequestContext handlerContext = new GrpcRequestContext(null, null, null);

    assertFalse(handlerContext.isCompleted());

    handlerContext.setCompleted();

    assertTrue(handlerContext.isCompleted());
  }

  @Test
  public void testHasError() {
    GrpcRequestContext handlerContext = new GrpcRequestContext(null, null, null);

    assertFalse(handlerContext.hasError());

    handlerContext.setError();

    assertTrue(handlerContext.hasError());
  }
}
