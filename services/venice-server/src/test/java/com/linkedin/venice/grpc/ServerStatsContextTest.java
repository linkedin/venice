package com.linkedin.venice.grpc;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.listener.ServerStatsContext;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.response.MultiGetResponseWrapper;
import com.linkedin.venice.listener.response.ParallelMultiKeyResponseWrapper;
import com.linkedin.venice.listener.response.stats.ComputeResponseStats;
import com.linkedin.venice.listener.response.stats.ComputeResponseStatsWithSizeProfiling;
import com.linkedin.venice.listener.response.stats.MultiGetResponseStatsWithSizeProfiling;
import com.linkedin.venice.listener.response.stats.ReadResponseStatsRecorder;
import com.linkedin.venice.listener.response.stats.SingleGetResponseStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class ServerStatsContextTest {
  private AggServerHttpRequestStats singleGetStats;

  private AggServerHttpRequestStats multiGetStats;

  private AggServerHttpRequestStats computeStats;

  // Test dimension constants for HTTP 200 OK (success)
  private static final HttpResponseStatusEnum OK_HTTP_STATUS = HttpResponseStatusEnum.OK;
  private static final HttpResponseStatusCodeCategory OK_HTTP_STATUS_CATEGORY = HttpResponseStatusCodeCategory.SUCCESS;
  private static final VeniceResponseStatusCategory OK_VENICE_STATUS = VeniceResponseStatusCategory.SUCCESS;

  // Test dimension constants for HTTP 500 Internal Server Error (failure)
  private static final HttpResponseStatusEnum ERROR_HTTP_STATUS = HttpResponseStatusEnum.INTERNAL_SERVER_ERROR;
  private static final HttpResponseStatusCodeCategory ERROR_HTTP_STATUS_CATEGORY =
      HttpResponseStatusCodeCategory.SERVER_ERROR;
  private static final VeniceResponseStatusCategory ERROR_VENICE_STATUS = VeniceResponseStatusCategory.FAIL;

  @BeforeTest
  public void setUp() {
    singleGetStats = mock(AggServerHttpRequestStats.class);
    multiGetStats = mock(AggServerHttpRequestStats.class);
    computeStats = mock(AggServerHttpRequestStats.class);
  }

  private ServerStatsContext createContext(RequestType requestType, HttpResponseStatus responseStatus) {
    ServerStatsContext context = new ServerStatsContext(singleGetStats, multiGetStats, computeStats);
    context.setStoreName("testStore");
    context.setRequestType(requestType);
    context.setResponseStatus(responseStatus);
    return context;
  }

  private void verifyNoResponseSizeRecording(ServerHttpRequestStats stats) {
    verify(stats, never()).recordResponseSize(
        any(HttpResponseStatusEnum.class),
        any(HttpResponseStatusCodeCategory.class),
        any(VeniceResponseStatusCategory.class),
        anyInt());
  }

  private void verifyNoValueSizeRecording(ServerHttpRequestStats stats) {
    verify(stats, never()).recordValueSizeInByte(
        any(HttpResponseStatusEnum.class),
        any(HttpResponseStatusCodeCategory.class),
        any(VeniceResponseStatusCategory.class),
        anyInt());
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
    ServerStatsContext context = createContext(RequestType.SINGLE_GET, HttpResponseStatus.OK);
    ServerHttpRequestStats stats = mock(ServerHttpRequestStats.class);
    context.successRequest(stats, 10.5);

    verify(stats).recordSuccessRequest(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS);
    verify(stats).recordSuccessRequestLatency(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 10.5);
  }

  @Test
  public void testErrorRequest() {
    ServerStatsContext context = createContext(RequestType.SINGLE_GET, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    ServerHttpRequestStats stats = mock(ServerHttpRequestStats.class);
    context.setMisroutedStoreVersion(true);
    context.errorRequest(stats, 12.3);

    verify(stats).recordErrorRequest(ERROR_HTTP_STATUS, ERROR_HTTP_STATUS_CATEGORY, ERROR_VENICE_STATUS);
    verify(stats).recordErrorRequestLatency(ERROR_HTTP_STATUS, ERROR_HTTP_STATUS_CATEGORY, ERROR_VENICE_STATUS, 12.3);
    verify(stats).recordMisroutedStoreVersionRequest();

    context.errorRequest(null, 12.3);
    verify(singleGetStats).recordErrorRequest(ERROR_HTTP_STATUS, ERROR_HTTP_STATUS_CATEGORY, ERROR_VENICE_STATUS);
    verify(singleGetStats)
        .recordErrorRequestLatency(ERROR_HTTP_STATUS, ERROR_HTTP_STATUS_CATEGORY, ERROR_VENICE_STATUS, 12.3);
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

  /**
   * Verifies all metrics recorded by recordBasicMetrics for a compute request.
   */
  @Test
  public void testRecordBasicMetrics() {
    ServerStatsContext context = createContext(RequestType.COMPUTE, HttpResponseStatus.OK);
    ServerHttpRequestStats stats = mock(ServerHttpRequestStats.class);
    context.setRequestKeyCount(105);
    context.setRequestSize(1000);

    ComputeResponseStats responseStats = new ComputeResponseStats();
    responseStats.setRecordCount(100);
    responseStats.addDatabaseLookupLatency(10);
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

    // From AbstractReadResponseStats.recordMetrics (via responseStatsRecorder.recordMetrics)
    verify(stats).recordDatabaseLookupLatency(anyDouble(), /* assembledMultiChunk */ anyBoolean());
    verify(stats).recordMultiChunkLargeValueCount(1);
    verify(stats).recordStorageExecutionHandlerSubmissionWaitTime(20.5);

    // From ComputeResponseStats.recordMetrics
    verify(stats).recordDotProductCount(300);
    verify(stats).recordCosineSimilarityCount(13);
    verify(stats).recordHadamardProductCount(132);
    verify(stats).recordCountOperatorCount(432);
    verify(stats).recordReadComputeLatency(anyDouble(), /* assembledMultiChunk */ anyBoolean());
    verify(stats).recordReadComputeDeserializationLatency(anyDouble(), /* assembledMultiChunk */ anyBoolean());
    verify(stats).recordReadComputeSerializationLatency(anyDouble());

    // From ServerStatsContext.recordBasicMetrics directly
    verify(stats).recordRequestKeyCount(105);
    verify(stats).recordRequestSizeInBytes(1000);
  }

  /**
   * Verifies that recordBasicMetrics records unified response size and value size for a success request (OK).
   */
  @Test
  public void testRecordBasicMetricsRecordsSizeForSuccessRequest() {
    ServerStatsContext context = createContext(RequestType.SINGLE_GET, HttpResponseStatus.OK);
    context.setResponseSize(500);

    SingleGetResponseStats responseStats = new SingleGetResponseStats();
    responseStats.addValueSize(200);
    context.setReadResponseStats(responseStats);

    ServerHttpRequestStats stats = mock(ServerHttpRequestStats.class);
    context.recordBasicMetrics(stats);

    // Unified response size (both Tehuti and OTel)
    verify(stats).recordResponseSize(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 500);
    // Unified value size per-key via SingleGetResponseStats.recordMetrics (both Tehuti and OTel)
    verify(stats).recordValueSizeInByte(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 200);

    // successRequest only records count and latency (not size)
    context.successRequest(stats, 10.5);
    verify(stats).recordSuccessRequest(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS);
    verify(stats).recordSuccessRequestLatency(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 10.5);
  }

  /**
   * Verifies that recordBasicMetrics records unified response size and value size for an error request.
   */
  @Test
  public void testRecordBasicMetricsRecordsSizeForErrorRequest() {
    ServerStatsContext context = createContext(RequestType.SINGLE_GET, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    context.setResponseSize(100);

    SingleGetResponseStats responseStats = new SingleGetResponseStats();
    responseStats.addValueSize(50);
    context.setReadResponseStats(responseStats);

    ServerHttpRequestStats stats = mock(ServerHttpRequestStats.class);
    context.recordBasicMetrics(stats);

    // Unified response size (both Tehuti and OTel)
    verify(stats).recordResponseSize(ERROR_HTTP_STATUS, ERROR_HTTP_STATUS_CATEGORY, ERROR_VENICE_STATUS, 100);
    // Unified value size per-key (both Tehuti and OTel)
    verify(stats).recordValueSizeInByte(ERROR_HTTP_STATUS, ERROR_HTTP_STATUS_CATEGORY, ERROR_VENICE_STATUS, 50);

    // errorRequest only records count and latency (not size)
    context.errorRequest(stats, 15.0);
    verify(stats).recordErrorRequest(ERROR_HTTP_STATUS, ERROR_HTTP_STATUS_CATEGORY, ERROR_VENICE_STATUS);
    verify(stats).recordErrorRequestLatency(ERROR_HTTP_STATUS, ERROR_HTTP_STATUS_CATEGORY, ERROR_VENICE_STATUS, 15.0);
  }

  @Test
  public void testMultiGetWithSizeProfilingRecordsMetrics() {
    ServerStatsContext context = createContext(RequestType.MULTI_GET, HttpResponseStatus.OK);
    context.setResponseSize(800);
    ServerHttpRequestStats stats = mock(ServerHttpRequestStats.class);

    MultiGetResponseStatsWithSizeProfiling responseStats = new MultiGetResponseStatsWithSizeProfiling(3);
    responseStats.setRecordCount(3);
    responseStats.addKeySize(10);
    responseStats.addKeySize(20);
    responseStats.addKeySize(30);
    responseStats.addValueSize(100);
    responseStats.addValueSize(200);
    responseStats.addValueSize(300);
    context.setReadResponseStats(responseStats);

    // Verify getResponseValueSize returns aggregate
    assertEquals(responseStats.getResponseValueSize(), 600);

    context.recordBasicMetrics(stats);

    // Per-key sizes are recorded via recordUnmergedMetrics with unified recording (both Tehuti and OTel)
    verify(stats).recordKeySizeInByte(10);
    verify(stats).recordKeySizeInByte(20);
    verify(stats).recordKeySizeInByte(30);
    verify(stats).recordValueSizeInByte(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 100);
    verify(stats).recordValueSizeInByte(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 200);
    verify(stats).recordValueSizeInByte(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 300);

    // Unified response size (both Tehuti and OTel)
    verify(stats).recordResponseSize(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 800);
  }

  /**
   * 429 flow: recordBasicMetrics records both Tehuti and OTel via unified recording.
   * Neither successRequest nor errorRequest is called. OTel uses FAIL category (consistent with
   * the router's treatment of 429 as FAIL).
   */
  @Test
  public void test429FlowRecordsBothTehutiAndOtel() {
    ServerStatsContext context = createContext(RequestType.SINGLE_GET, HttpResponseStatus.TOO_MANY_REQUESTS);
    context.setResponseSize(500);
    ServerHttpRequestStats stats = mock(ServerHttpRequestStats.class);

    SingleGetResponseStats responseStats = new SingleGetResponseStats();
    responseStats.addValueSize(200);
    responseStats.addKeySize(50);
    context.setReadResponseStats(responseStats);

    // In the 429 flow, only recordBasicMetrics is called (StatsHandler.write does not call
    // successRequest or errorRequest for 429)
    context.recordBasicMetrics(stats);

    // Unified response size (both Tehuti and OTel)
    verify(stats).recordResponseSize(
        HttpResponseStatusEnum.TOO_MANY_REQUESTS,
        HttpResponseStatusCodeCategory.CLIENT_ERROR,
        VeniceResponseStatusCategory.FAIL,
        500);
    // Unified value size per-key via SingleGetResponseStats.recordMetrics (both Tehuti and OTel)
    verify(stats).recordValueSizeInByte(
        HttpResponseStatusEnum.TOO_MANY_REQUESTS,
        HttpResponseStatusCodeCategory.CLIENT_ERROR,
        VeniceResponseStatusCategory.FAIL,
        200);
    verify(stats).recordKeySizeInByte(50);

    // success/error count and latency are NOT recorded for 429
    verify(stats, never()).recordSuccessRequest(
        any(HttpResponseStatusEnum.class),
        any(HttpResponseStatusCodeCategory.class),
        any(VeniceResponseStatusCategory.class));
    verify(stats, never()).recordErrorRequest(
        any(HttpResponseStatusEnum.class),
        any(HttpResponseStatusCodeCategory.class),
        any(VeniceResponseStatusCategory.class));
  }

  /**
   * Verify flushLatency, earlyTermination, and responseSize are recorded in recordBasicMetrics.
   */
  @Test
  public void testRecordBasicMetricsFlushLatencyEarlyTerminationResponseSize() {
    ServerStatsContext context = createContext(RequestType.SINGLE_GET, HttpResponseStatus.OK);
    ServerHttpRequestStats stats = mock(ServerHttpRequestStats.class);
    context.setFlushLatency(25.5);
    context.setRequestTerminatedEarly();
    context.setResponseSize(1024);

    context.recordBasicMetrics(stats);

    verify(stats).recordFlushLatency(25.5);
    verify(stats).recordEarlyTerminatedEarlyRequest();
    verify(stats).recordResponseSize(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 1024);
  }

  /**
   * Verify recordBasicMetrics works when no responseStatsRecorder is set.
   */
  @Test
  public void testRecordBasicMetricsWithoutResponseStatsRecorder() {
    ServerStatsContext context = createContext(RequestType.SINGLE_GET, HttpResponseStatus.OK);
    ServerHttpRequestStats stats = mock(ServerHttpRequestStats.class);
    context.setRequestKeyCount(5);
    context.setRequestSize(256);

    // No responseStatsRecorder set — should still record request-level metrics without NPE
    context.recordBasicMetrics(stats);

    verify(stats).recordRequestKeyCount(5);
    verify(stats).recordRequestSizeInBytes(256);
    // No response stats interactions (no responseStatsRecorder means no recordMetrics call)
    verify(stats, never()).recordDatabaseLookupLatency(anyDouble(), anyBoolean());
  }

  /**
   * Response size recording is skipped when responseSize is negative (default).
   */
  @Test
  public void testRecordBasicMetricsSkipsSizeRecordingWhenNotSet() {
    ServerStatsContext context = createContext(RequestType.SINGLE_GET, HttpResponseStatus.OK);
    ServerHttpRequestStats stats = mock(ServerHttpRequestStats.class);
    // responseSize is left at default (-1), no responseStatsRecorder set

    context.recordBasicMetrics(stats);

    // Response size should NOT be called when responseSize < 0
    verifyNoResponseSizeRecording(stats);
  }

  /**
   * Value size recording is skipped when valueSize is zero (e.g., single get with no value found).
   */
  @Test
  public void testRecordBasicMetricsSkipsValueSizeWhenZero() {
    ServerStatsContext context = createContext(RequestType.SINGLE_GET, HttpResponseStatus.OK);
    context.setResponseSize(50);
    ServerHttpRequestStats stats = mock(ServerHttpRequestStats.class);

    // SingleGetResponseStats with no value added (valueSize defaults to 0)
    SingleGetResponseStats responseStats = new SingleGetResponseStats();
    context.setReadResponseStats(responseStats);
    assertEquals(responseStats.getResponseValueSize(), 0);

    context.recordBasicMetrics(stats);

    // Response size IS recorded (responseSize=50 > 0)
    verify(stats).recordResponseSize(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 50);
    // Value size is NOT recorded (valueSize=0, guard in SingleGetResponseStats: valueSize > 0)
    verifyNoValueSizeRecording(stats);
  }

  /**
   * Verify MultiGetResponseStatsWithSizeProfiling.merge() correctly merges totalValueSize.
   */
  @Test
  public void testMultiGetResponseStatsMergeTotalValueSize() {
    MultiGetResponseStatsWithSizeProfiling stats1 = new MultiGetResponseStatsWithSizeProfiling(3);
    stats1.setRecordCount(2);
    stats1.addValueSize(100);
    stats1.addValueSize(200);
    stats1.addKeySize(10);
    stats1.addKeySize(20);
    stats1.incrementMultiChunkLargeValueCount();

    MultiGetResponseStatsWithSizeProfiling stats2 = new MultiGetResponseStatsWithSizeProfiling(2);
    stats2.setRecordCount(1);
    stats2.addValueSize(300);
    stats2.addKeySize(30);

    assertEquals(stats1.getResponseValueSize(), 300);
    assertEquals(stats2.getResponseValueSize(), 300);

    stats1.merge(stats2);

    // totalValueSize should be merged: 300 + 300 = 600
    assertEquals(stats1.getResponseValueSize(), 600);

    // Verify merged stats record correctly
    ServerHttpRequestStats mockStats = mock(ServerHttpRequestStats.class);
    stats1.recordMetrics(mockStats, OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS);

    // multiChunkLargeValueCount: 1 from stats1 + 0 from stats2 = 1 (merged in super.merge)
    verify(mockStats).recordMultiChunkLargeValueCount(1);

    // Per-key K/V sizes are unmerged (recorded individually from stats1's lists only)
    verify(mockStats).recordKeySizeInByte(10);
    verify(mockStats).recordKeySizeInByte(20);
    verify(mockStats).recordValueSizeInByte(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 100);
    verify(mockStats).recordValueSizeInByte(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 200);
  }

  /**
   * Verify ComputeResponseStats.merge() correctly merges totalValueSize and compute-specific fields.
   */
  @Test
  public void testComputeResponseStatsMergeTotalValueSize() {
    ComputeResponseStats stats1 = new ComputeResponseStats();
    stats1.setRecordCount(2);
    stats1.addValueSize(100);
    stats1.addValueSize(200);
    stats1.incrementDotProductCount(10);

    ComputeResponseStats stats2 = new ComputeResponseStats();
    stats2.setRecordCount(1);
    stats2.addValueSize(400);
    stats2.incrementDotProductCount(5);

    assertEquals(stats1.getResponseValueSize(), 300);
    assertEquals(stats2.getResponseValueSize(), 400);

    stats1.merge(stats2);

    // totalValueSize should be merged: 300 + 400 = 700
    assertEquals(stats1.getResponseValueSize(), 700);

    // Verify merged compute stats record correctly
    ServerHttpRequestStats mockStats = mock(ServerHttpRequestStats.class);
    stats1.recordMetrics(mockStats, OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS);

    // dotProductCount: 10 + 5 = 15
    verify(mockStats).recordDotProductCount(15);
  }

  /**
   * Verify that CompositeReadResponseStatsRecorder (created by ParallelMultiKeyResponseWrapper)
   * correctly merges stats across chunks and records them via recordMetrics.
   */
  @Test
  public void testCompositeRecorderMergesAndRecordsAcrossChunks() {
    ParallelMultiKeyResponseWrapper<MultiGetResponseWrapper> wrapper = ParallelMultiKeyResponseWrapper.multiGet(
        3,
        10,
        chunkSize -> new MultiGetResponseWrapper(chunkSize, new MultiGetResponseStatsWithSizeProfiling(chunkSize)));

    // Accumulate stats in each chunk
    MultiGetResponseStatsWithSizeProfiling chunk0Stats =
        (MultiGetResponseStatsWithSizeProfiling) wrapper.getChunk(0).getStatsRecorder();
    chunk0Stats.addDatabaseLookupLatency(chunk0Stats.getCurrentTimeInNanos());
    chunk0Stats.addValueSize(100);
    chunk0Stats.addValueSize(200);
    chunk0Stats.addKeySize(10);
    chunk0Stats.addKeySize(20);
    chunk0Stats.incrementMultiChunkLargeValueCount();

    MultiGetResponseStatsWithSizeProfiling chunk1Stats =
        (MultiGetResponseStatsWithSizeProfiling) wrapper.getChunk(1).getStatsRecorder();
    chunk1Stats.addValueSize(300);
    chunk1Stats.addKeySize(30);

    MultiGetResponseStatsWithSizeProfiling chunk2Stats =
        (MultiGetResponseStatsWithSizeProfiling) wrapper.getChunk(2).getStatsRecorder();
    chunk2Stats.addValueSize(400);
    chunk2Stats.addKeySize(40);

    // getStatsRecorder() creates the CompositeReadResponseStatsRecorder which merges chunks
    ReadResponseStatsRecorder compositeRecorder = wrapper.getStatsRecorder();
    ServerHttpRequestStats mockStats = mock(ServerHttpRequestStats.class);
    compositeRecorder.recordMetrics(mockStats, OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS);

    // Merged metrics: multiChunkLargeValueCount=1 (from chunk0), databaseLookupLatency recorded
    verify(mockStats).recordMultiChunkLargeValueCount(1);
    verify(mockStats).recordDatabaseLookupLatency(anyDouble(), anyBoolean());

    // Per-key sizes from all chunks (unmerged — recorded individually)
    verify(mockStats).recordKeySizeInByte(10);
    verify(mockStats).recordKeySizeInByte(20);
    verify(mockStats).recordKeySizeInByte(30);
    verify(mockStats).recordKeySizeInByte(40);
    verify(mockStats).recordValueSizeInByte(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 100);
    verify(mockStats).recordValueSizeInByte(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 200);
    verify(mockStats).recordValueSizeInByte(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 300);
    verify(mockStats).recordValueSizeInByte(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 400);
  }

  /**
   * Verify size recording is skipped when responseSize=-1 and valueSize=0 for error requests.
   */
  @Test
  public void testErrorRequestBoundarySkipsSizeRecording() {
    ServerStatsContext context = createContext(RequestType.SINGLE_GET, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    ServerHttpRequestStats stats = mock(ServerHttpRequestStats.class);
    // responseSize defaults to -1 (not set)

    // SingleGetResponseStats with no value added (valueSize defaults to 0)
    SingleGetResponseStats responseStats = new SingleGetResponseStats();
    context.setReadResponseStats(responseStats);
    assertEquals(responseStats.getResponseValueSize(), 0);

    // recordBasicMetrics should skip size recording when sizes aren't set
    context.recordBasicMetrics(stats);

    // Response size NOT recorded (responseSize=-1, guard: responseSize >= 0)
    verifyNoResponseSizeRecording(stats);
    // Value size NOT recorded (valueSize=0, guard in SingleGetResponseStats: valueSize > 0)
    verifyNoValueSizeRecording(stats);

    // Error count and latency are still recorded in errorRequest
    context.errorRequest(stats, 8.0);
    verify(stats).recordErrorRequest(ERROR_HTTP_STATUS, ERROR_HTTP_STATUS_CATEGORY, ERROR_VENICE_STATUS);
    verify(stats).recordErrorRequestLatency(ERROR_HTTP_STATUS, ERROR_HTTP_STATUS_CATEGORY, ERROR_VENICE_STATUS, 8.0);
  }

  /**
   * Verify ComputeResponseStatsWithSizeProfiling.merge() correctly merges compute fields
   * and totalValueSize from the parent ComputeResponseStats.
   */
  @Test
  public void testComputeResponseStatsWithSizeProfilingMerge() {
    ComputeResponseStatsWithSizeProfiling stats1 = new ComputeResponseStatsWithSizeProfiling(3);
    stats1.setRecordCount(2);
    stats1.addValueSize(100);
    stats1.addValueSize(200);
    stats1.addKeySize(10);
    stats1.addKeySize(20);
    stats1.incrementDotProductCount(5);
    stats1.incrementCosineSimilarityCount(3);

    ComputeResponseStatsWithSizeProfiling stats2 = new ComputeResponseStatsWithSizeProfiling(2);
    stats2.setRecordCount(1);
    stats2.addValueSize(400);
    stats2.addKeySize(30);
    stats2.incrementDotProductCount(10);
    stats2.incrementHadamardProductCount(7);

    assertEquals(stats1.getResponseValueSize(), 300);
    assertEquals(stats2.getResponseValueSize(), 400);

    stats1.merge(stats2);

    // totalValueSize should be merged: 300 + 400 = 700
    assertEquals(stats1.getResponseValueSize(), 700);

    // Verify merged stats record correctly — recordMetrics calls recordUnmergedMetrics internally,
    // so per-key sizes from stats1 are also recorded here.
    ServerHttpRequestStats mockStats = mock(ServerHttpRequestStats.class);
    stats1.recordMetrics(mockStats, OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS);

    // dotProductCount: 5 + 10 = 15
    verify(mockStats).recordDotProductCount(15);
    // cosineSimilarityCount: 3 + 0 = 3
    verify(mockStats).recordCosineSimilarityCount(3);
    // hadamardProductCount: 0 + 7 = 7
    verify(mockStats).recordHadamardProductCount(7);

    // Per-key sizes from stats1 are recorded via recordUnmergedMetrics (called by recordMetrics)
    verify(mockStats).recordKeySizeInByte(10);
    verify(mockStats).recordKeySizeInByte(20);
    verify(mockStats).recordValueSizeInByte(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 100);
    verify(mockStats).recordValueSizeInByte(OK_HTTP_STATUS, OK_HTTP_STATUS_CATEGORY, OK_VENICE_STATUS, 200);
  }
}
