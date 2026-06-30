package com.linkedin.venice.listener.grpc.handlers;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.listener.ServerStatsContext;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import io.grpc.stub.StreamObserver;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class GrpcOutboundStatsHandlerTest {
  private static final String STORE_NAME = "test_store";

  /*
   * Each row is: the response status, whether a prior handler flagged an error, and the expected outcome -
   * TRUE = recorded as success, FALSE = recorded as error, null = recorded as neither (throttled).
   */
  @DataProvider(name = "responseStatusCases", parallel = true)
  public Object[][] responseStatusCases() {
    return new Object[][] { { OK, false, Boolean.TRUE }, // value found -> success (regression guard for 200-as-fail)
        { NOT_FOUND, false, Boolean.TRUE }, // key absent -> success
        { OK, true, Boolean.FALSE }, // error flagged despite OK status -> error
        { BAD_REQUEST, false, Boolean.FALSE }, // malformed request -> error
        { INTERNAL_SERVER_ERROR, false, Boolean.FALSE }, // server failure -> error
        { TOO_MANY_REQUESTS, false, null } }; // throttled -> neither success nor error
  }

  @Test(dataProvider = "responseStatusCases")
  public void testRequestCategorization(HttpResponseStatus responseStatus, boolean hasError, Boolean expectSuccess) {
    ServerHttpRequestStats storeStats = mock(ServerHttpRequestStats.class);
    AggServerHttpRequestStats aggStats = mock(AggServerHttpRequestStats.class);
    when(aggStats.getStoreStats(STORE_NAME)).thenReturn(storeStats);

    ServerStatsContext statsContext = mock(ServerStatsContext.class);
    when(statsContext.getResponseStatus()).thenReturn(responseStatus);
    when(statsContext.getStoreName()).thenReturn(STORE_NAME);
    when(statsContext.getCurrentStats()).thenReturn(aggStats);
    when(statsContext.getRequestStartTimeInNS()).thenReturn(0L);

    @SuppressWarnings("unchecked")
    StreamObserver<VeniceServerResponse> responseObserver = mock(StreamObserver.class);
    GrpcRequestContext ctx = new GrpcRequestContext(null, VeniceServerResponse.newBuilder(), responseObserver);
    ctx.setGrpcStatsContext(statsContext);
    if (hasError) {
      ctx.setError();
    }

    new GrpcOutboundStatsHandler().processRequest(ctx);

    if (expectSuccess == null) {
      verify(statsContext, never()).successRequest(eq(storeStats), anyDouble());
      verify(statsContext, never()).errorRequest(eq(storeStats), anyDouble());
    } else if (expectSuccess) {
      verify(statsContext, times(1)).successRequest(eq(storeStats), anyDouble());
      verify(statsContext, never()).errorRequest(eq(storeStats), anyDouble());
    } else {
      verify(statsContext, times(1)).errorRequest(eq(storeStats), anyDouble());
      verify(statsContext, never()).successRequest(eq(storeStats), anyDouble());
    }
  }
}
