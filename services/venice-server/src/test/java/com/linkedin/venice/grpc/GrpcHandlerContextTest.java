package com.linkedin.venice.grpc;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.listener.grpc.GrpcHandlerContext;
import com.linkedin.venice.listener.grpc.GrpcStatsContext;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceServerResponse;
import io.grpc.stub.StreamObserver;
import org.testng.annotations.Test;


public class GrpcHandlerContextTest {
  @Test
  public void testSetAndGetGrpcStatsContext() {
    GrpcStatsContext context = new GrpcStatsContext(null, null, null);
    GrpcHandlerContext handlerContext = new GrpcHandlerContext(null, null, null);
    handlerContext.setGrpcStatsContext(context);

    assertEquals(context, handlerContext.getGrpcStatsContext());
  }

  @Test
  public void testSetAndGetVeniceClientRequest() {
    VeniceClientRequest request = VeniceClientRequest.newBuilder().build();
    GrpcHandlerContext handlerContext = new GrpcHandlerContext(null, null, null);
    handlerContext.setVeniceClientRequest(request);

    assertEquals(request, handlerContext.getVeniceClientRequest());
  }

  @Test
  public void testSetAndGetVeniceServerResponseBuilder() {
    VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
    GrpcHandlerContext handlerContext = new GrpcHandlerContext(null, null, null);
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

    GrpcHandlerContext handlerContext = new GrpcHandlerContext(null, null, null);
    handlerContext.setResponseObserver(observer);

    assertEquals(observer, handlerContext.getResponseObserver());
  }

  @Test
  public void testSetAndGetRouterRequest() {
    RouterRequest request = mock(RouterRequest.class);
    GrpcHandlerContext handlerContext = new GrpcHandlerContext(null, null, null);
    handlerContext.setRouterRequest(request);

    assertEquals(request, handlerContext.getRouterRequest());
  }

  @Test
  public void testSetAndGetReadResponse() {
    ReadResponse response = mock(ReadResponse.class);
    GrpcHandlerContext handlerContext = new GrpcHandlerContext(null, null, null);
    handlerContext.setReadResponse(response);

    assertEquals(response, handlerContext.getReadResponse());
  }

  @Test
  public void testIsCompleted() {
    GrpcHandlerContext handlerContext = new GrpcHandlerContext(null, null, null);

    assertFalse(handlerContext.isCompleted());

    handlerContext.setCompleted();

    assertTrue(handlerContext.isCompleted());
  }

  @Test
  public void testHasError() {
    GrpcHandlerContext handlerContext = new GrpcHandlerContext(null, null, null);

    assertFalse(handlerContext.hasError());

    handlerContext.setError();

    assertTrue(handlerContext.hasError());
  }
}
