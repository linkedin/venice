package com.linkedin.venice.grpc;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.listener.ServerStatsContext;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.response.stats.ComputeResponseStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import org.mockito.MockingDetails;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class ServerStatsContextTest {
  private AggServerHttpRequestStats singleGetStats;

  private AggServerHttpRequestStats multiGetStats;

  private AggServerHttpRequestStats computeStats;

  @BeforeTest
  public void setUp() {
    singleGetStats = mock(AggServerHttpRequestStats.class);
    multiGetStats = mock(AggServerHttpRequestStats.class);
    computeStats = mock(AggServerHttpRequestStats.class);
  }

  @Test
  public void testSetStoreName() {
    ServerStatsContext context = new ServerStatsContext(singleGetStats, multiGetStats, computeStats);
    context.setStoreName("testStore");
    assertEquals("testStore", context.getStoreName());
  }

  @Test
  public void testSetRequestType() {
    ServerStatsContext context = new ServerStatsContext(singleGetStats, multiGetStats, computeStats);

    context.setRequestType(RequestType.SINGLE_GET);
    assertEquals(singleGetStats, context.getCurrentStats());

    context.setRequestType(RequestType.MULTI_GET);
    assertEquals(multiGetStats, context.getCurrentStats());

    context.setRequestType(RequestType.COMPUTE);
    assertEquals(computeStats, context.getCurrentStats());
  }

  @Test
  public void testSuccessRequest() {
    ServerStatsContext context = new ServerStatsContext(singleGetStats, multiGetStats, computeStats);
    context.setStoreName("testStore");
    ServerHttpRequestStats stats = mock(ServerHttpRequestStats.class);
    context.successRequest(stats, 10.5);

    verify(stats).recordSuccessRequest();
    verify(stats).recordSuccessRequestLatency(10.5);
  }

  @Test
  public void testErrorRequest() {
    ServerStatsContext context = new ServerStatsContext(singleGetStats, multiGetStats, computeStats);

    ServerHttpRequestStats stats = mock(ServerHttpRequestStats.class);
    context.setRequestType(RequestType.SINGLE_GET);
    context.setMisroutedStoreVersion(true);
    context.errorRequest(stats, 12.3);

    verify(stats).recordErrorRequest();
    verify(stats).recordErrorRequestLatency(12.3);
    verify(stats).recordMisroutedStoreVersionRequest();

    context.errorRequest(null, 12.3);
    verify(singleGetStats).recordErrorRequest();
    verify(singleGetStats).recordErrorRequestLatency(12.3);
    verify(singleGetStats).recordMisroutedStoreVersionRequest();
  }

  @Test
  public void testRequestInfo() {
    RouterRequest routerRequest = mock(RouterRequest.class);
    doReturn("testStore").when(routerRequest).getStoreName();
    doReturn(RequestType.MULTI_GET).when(routerRequest).getRequestType();
    doReturn(123).when(routerRequest).getKeyCount();

    ServerStatsContext context = new ServerStatsContext(singleGetStats, multiGetStats, computeStats);
    context.setRequestInfo(routerRequest);

    assertEquals("testStore", context.getStoreName());
    assertEquals(multiGetStats, context.getCurrentStats());
    assertEquals(123, context.getRequestKeyCount());
  }

  @Test
  public void setRequestType() {
    ServerStatsContext context = new ServerStatsContext(singleGetStats, multiGetStats, computeStats);

    context.setRequestType(RequestType.SINGLE_GET);
    assertEquals(singleGetStats, context.getCurrentStats());

    context.setRequestType(RequestType.MULTI_GET);
    assertEquals(multiGetStats, context.getCurrentStats());

    context.setRequestType(RequestType.COMPUTE);
    assertEquals(computeStats, context.getCurrentStats());
  }

  @Test
  public void testRecordBasicMetrics() {
    ServerStatsContext context = new ServerStatsContext(singleGetStats, multiGetStats, computeStats);
    ServerHttpRequestStats stats = mock(ServerHttpRequestStats.class);
    context.setStoreName("testStore");
    context.setRequestType(RequestType.COMPUTE);

    context.setRequestKeyCount(105);
    context.setRequestSize(1000);
    context.incrementRequestPartCount();
    context.incrementRequestPartCount();

    ComputeResponseStats responseStats = new ComputeResponseStats();
    responseStats.setRecordCount(100);
    responseStats.addDatabaseLookupLatency(10.5);
    responseStats.setStorageExecutionSubmissionWaitTime(20.5);
    responseStats.incrementMultiChunkLargeValueCount();
    responseStats.addReadComputeLatency(1000);
    responseStats.addReadComputeDeserializationLatency(100);
    responseStats.addReadComputeSerializationLatency(200);
    responseStats.incrementDotProductCount(300);
    responseStats.incrementCosineSimilarityCount(13);
    responseStats.incrementHadamardProductCount(132);
    responseStats.incrementCountOperatorCount(432);
    context.setReadResponseStats(responseStats);

    context.recordBasicMetrics(stats);

    // verify that 14 interactions are recorded with the stats object, only 14 record metrics to the stats object
    MockingDetails details = org.mockito.Mockito.mockingDetails(stats);
    int invocations = details.getInvocations().size();
    assertEquals(invocations, 14);
  }
}
