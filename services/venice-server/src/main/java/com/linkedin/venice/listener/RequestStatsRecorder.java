package com.linkedin.venice.listener;

import static com.linkedin.venice.listener.response.stats.ResponseStatsUtil.consumeDoubleIfAbove;
import static com.linkedin.venice.listener.response.stats.ResponseStatsUtil.consumeIntIfAbove;
import static com.linkedin.venice.response.VeniceReadResponseStatus.MISROUTED_STORE_VERSION;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.response.stats.ReadResponseStatsRecorder;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.utils.LatencyUtils;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * We need to be able to record server side statistics for gRPC requests. The current StatsHandler in the Netty
 * Pipeline maintains instance variables per channel, and guarantees that each request will be handled by the same
 * thread, thus we cannot augment StatsHandler in the same way as other handlers for gRPC requests. We create a
 * new StatsContext for each gRPC request, so we can maintain per request stats which will be aggregated on request
 * completion using references to the proper Metrics Repository in AggServerHttpRequestStats. This class is almost a
 * direct copy of StatsHandler, without Netty Channel Read/Write logic.
 */
public class RequestStatsRecorder {
  private static final Logger LOGGER = LogManager.getLogger(RequestStatsRecorder.class);

  private ReadResponseStatsRecorder responseStatsRecorder = null;
  private long startTimeInNS = System.nanoTime();
  private VeniceReadResponseStatus responseStatus = null;
  private String storeName = null;
  private boolean isMetadataRequest = false;
  private int requestKeyCount = -1;
  private int requestSizeInBytes = -1;
  private boolean isRequestTerminatedEarly = false;
  private final AggServerHttpRequestStats singleGetStats;
  private final AggServerHttpRequestStats multiGetStats;
  private final AggServerHttpRequestStats computeStats;
  private AggServerHttpRequestStats currentStats;

  // a flag that indicates if this is a new HttpRequest. Netty is TCP-based, so a HttpRequest is chunked into packages.
  // Set the startTimeInNS in ChannelRead if it is the first package within a HttpRequest.
  private boolean newRequest = true;

  /**
   * To indicate whether the stat callback has been triggered or not for a given request.
   * This is mostly to bypass the issue that stat callback could be triggered multiple times for one single request.
   */
  private boolean statCallbackExecuted = false;

  /**
   * For Netty pipeline:
   * Normally, one multi-get request will be split into two parts, and it means
   * {@link StatsHandler#channelRead(ChannelHandlerContext, Object)} will be invoked twice.
   *
   * 'firstPartLatency' will measure the time took by:
   * {@link StatsHandler}
   * {@link io.netty.handler.codec.http.HttpServerCodec}
   * {@link io.netty.handler.codec.http.HttpObjectAggregator}
   *
   * 'partsInvokeDelayLatency' will measure the delay between the invocation of part1
   * and the invocation of part2;
   *
   * 'secondPartLatency' will measure the time took by:
   * {@link StatsHandler}
   * {@link io.netty.handler.codec.http.HttpServerCodec}
   * {@link io.netty.handler.codec.http.HttpObjectAggregator}
   * {@link VerifySslHandler}
   * {@link ServerAclHandler}
   * {@link RouterRequestHttpHandler}
   * {@link StorageReadRequestHandler}
   *
   * For gRPC pipeline: Request is not split into parts, so part latency is not applicable.
   */
  private double firstPartLatency = -1;
  private double secondPartLatency = -1;
  private double partsInvokeDelayLatency = -1;
  private int requestPartCount = 1;
  private boolean isMisroutedStoreVersion = false;

  /**
   * TODO: We need to figure out how to record the flush latency for gRPC requests.
   */
  private double flushLatency = -1;
  private int responseSize = -1;

  public boolean isNewRequest() {
    return newRequest;
  }

  public RequestStatsRecorder setSecondPartLatency(double secondPartLatency) {
    this.secondPartLatency = secondPartLatency;
    return this;
  }

  public RequestStatsRecorder setPartsInvokeDelayLatency(double partsInvokeDelayLatency) {
    this.partsInvokeDelayLatency = partsInvokeDelayLatency;
    return this;
  }

  public RequestStatsRecorder incrementRequestPartCount() {
    this.requestPartCount++;
    return this;
  }

  public RequestStatsRecorder(
      AggServerHttpRequestStats singleGetStats,
      AggServerHttpRequestStats multiGetStats,
      AggServerHttpRequestStats computeStats) {
    this.singleGetStats = singleGetStats;
    this.multiGetStats = multiGetStats;
    this.computeStats = computeStats;
    // default to current stats
    this.currentStats = singleGetStats;
  }

  public void resetContext() {
    this.responseStatsRecorder = null;
    storeName = null;
    startTimeInNS = System.nanoTime();
    partsInvokeDelayLatency = -1;
    secondPartLatency = -1;
    requestPartCount = 1;
    isMetadataRequest = false;
    responseStatus = null;
    statCallbackExecuted = false;
    requestKeyCount = -1;
    requestSizeInBytes = -1;
    isRequestTerminatedEarly = false;
    isMisroutedStoreVersion = false;
    flushLatency = -1;
    responseSize = -1;
    newRequest = false; // set to false after the first package of a HttpRequest
  }

  public RequestStatsRecorder setReadResponseStats(ReadResponseStatsRecorder responseStatsRecorder) {
    this.responseStatsRecorder = responseStatsRecorder;
    return this;
  }

  public RequestStatsRecorder setFirstPartLatency(double firstPartLatency) {
    this.firstPartLatency = firstPartLatency;
    return this;
  }

  public RequestStatsRecorder setNewRequest() {
    this.newRequest = true;
    return this;
  }

  public boolean isMetadataRequest() {
    return isMetadataRequest;
  }

  public boolean isStatCallBackExecuted() {
    return statCallbackExecuted;
  }

  public void setStatCallBackExecuted(boolean statCallbackExecuted) {
    this.statCallbackExecuted = statCallbackExecuted;
  }

  public RequestStatsRecorder setResponseStatus(VeniceReadResponseStatus status) {
    this.responseStatus = status;
    return this;
  }

  public String getStoreName() {
    return storeName;
  }

  public RequestStatsRecorder setStoreName(String name) {
    this.storeName = name;
    return this;
  }

  public RequestStatsRecorder setMetadataRequest(boolean metadataRequest) {
    this.isMetadataRequest = metadataRequest;
    return this;
  }

  public RequestStatsRecorder setRequestTerminatedEarly() {
    this.isRequestTerminatedEarly = true;
    return this;
  }

  public VeniceReadResponseStatus getVeniceReadResponseStatus() {
    return responseStatus;
  }

  public RequestStatsRecorder setRequestType(RequestType requestType) {
    switch (requestType) {
      case MULTI_GET:
      case MULTI_GET_STREAMING:
        currentStats = multiGetStats;
        break;
      case COMPUTE:
      case COMPUTE_STREAMING:
        currentStats = computeStats;
        break;
      default:
        currentStats = singleGetStats;
    }
    return this;
  }

  public RequestStatsRecorder setRequestKeyCount(int keyCount) {
    this.requestKeyCount = keyCount;
    return this;
  }

  public AggServerHttpRequestStats getCurrentStats() {
    return currentStats;
  }

  public RequestStatsRecorder setRequestInfo(RouterRequest request) {
    setStoreName(request.getStoreName());
    setRequestType(request.getRequestType());
    setRequestKeyCount(request.getKeyCount());
    return this;
  }

  public RequestStatsRecorder setRequestSize(int requestSizeInBytes) {
    this.requestSizeInBytes = requestSizeInBytes;
    return this;
  }

  public long getRequestStartTimeInNS() {
    return this.startTimeInNS;
  }

  public RequestStatsRecorder setFlushLatency(double latency) {
    this.flushLatency = latency;
    return this;
  }

  public void setResponseSize(int size) {
    this.responseSize = size;
  }

  public RequestStatsRecorder recordBasicMetrics(ServerHttpRequestStats serverHttpRequestStats) {
    if (serverHttpRequestStats == null) {
      return this;
    }

    if (this.responseStatsRecorder != null) {
      this.responseStatsRecorder.recordMetrics(serverHttpRequestStats);
    }

    consumeIntIfAbove(serverHttpRequestStats::recordRequestKeyCount, this.requestKeyCount, 0);
    consumeIntIfAbove(serverHttpRequestStats::recordRequestSizeInBytes, this.requestSizeInBytes, 0);
    consumeDoubleIfAbove(serverHttpRequestStats::recordRequestFirstPartLatency, this.firstPartLatency, 0);
    consumeDoubleIfAbove(serverHttpRequestStats::recordRequestPartsInvokeDelayLatency, this.partsInvokeDelayLatency, 0);
    consumeDoubleIfAbove(serverHttpRequestStats::recordRequestSecondPartLatency, this.secondPartLatency, 0);
    consumeIntIfAbove(serverHttpRequestStats::recordRequestPartCount, this.requestPartCount, 0);

    if (this.isRequestTerminatedEarly) {
      serverHttpRequestStats.recordEarlyTerminatedEarlyRequest();
    }
    if (flushLatency >= 0) {
      serverHttpRequestStats.recordFlushLatency(flushLatency);
    }
    if (responseSize >= 0) {
      serverHttpRequestStats.recordResponseSize(responseSize);
    }

    return this;
  }

  /**
   * Records a successful request in the statistics.
   * This method is called when the {@link responseStatus} is either OK or KEY_NOT_FOUND.
   *
   * Note: Synchronization is not required here, as operations in Tehuti are already synchronized.
   * However, reconsider potential race conditions if new logic is introduced.
   */
  public RequestStatsRecorder successRequest(ServerHttpRequestStats stats, double elapsedTime) {
    if (stats != null) {
      stats.recordSuccessRequest();
      stats.recordSuccessRequestLatency(elapsedTime);
    }
    return this;
  }

  /**
   * Records an error request in the statistics.
   * This method is called when the {@link responseStatus} is neither OK, KEY_NOT_FOUND, nor TOO_MANY_REQUESTS.
   */
  public RequestStatsRecorder errorRequest(ServerHttpRequestStats stats, double elapsedTime) {
    if (stats == null) {
      currentStats.recordErrorRequest();
      currentStats.recordErrorRequestLatency(elapsedTime);
      if (isMisroutedStoreVersion) {
        currentStats.recordMisroutedStoreVersionRequest();
      }
    } else {
      stats.recordErrorRequest();
      stats.recordErrorRequestLatency(elapsedTime);
      if (isMisroutedStoreVersion) {
        stats.recordMisroutedStoreVersionRequest();
      }
    }

    return this;
  }

  public int getRequestKeyCount() {
    return requestKeyCount;
  }

  public void setMisroutedStoreVersion(boolean misroutedStoreVersion) {
    isMisroutedStoreVersion = misroutedStoreVersion;
  }

  /**
   * Records the completion of a request in the statistics.
   * This method should be called at the end of the request processing.
   */
  public static void recordRequestCompletionStats(
      RequestStatsRecorder requestStatsRecorder,
      boolean isSuccess,
      long flushLatencyNs) {
    VeniceReadResponseStatus readResponseStatus = requestStatsRecorder.getVeniceReadResponseStatus();
    if (readResponseStatus == null) {
      LOGGER.error(
          "Failed to record request stats: response status of request cannot be null",
          new VeniceException("response status of request cannot be null"));
      return;
    }

    String storeName = requestStatsRecorder.getStoreName();
    if (storeName == null) {
      LOGGER.error(
          "Failed to record request stats: store name of request cannot be null",
          new VeniceException("store name of request cannot be null"));
      return;
    }

    if (readResponseStatus == VeniceRequestEarlyTerminationException.getResponseStatusCode()) {
      requestStatsRecorder.setRequestTerminatedEarly();
    }
    if (readResponseStatus == MISROUTED_STORE_VERSION) {
      requestStatsRecorder.setMisroutedStoreVersion(true);
    }

    // Calculate the latency
    double requestLatency = LatencyUtils.getElapsedTimeFromNSToMS(requestStatsRecorder.getRequestStartTimeInNS());

    // Optionally include flush latency if available
    if (flushLatencyNs > 0) {
      requestStatsRecorder.setFlushLatency(LatencyUtils.getElapsedTimeFromNSToMS(flushLatencyNs));
    }

    ServerHttpRequestStats serverHttpRequestStats = requestStatsRecorder.getCurrentStats().getStoreStats(storeName);
    requestStatsRecorder.recordBasicMetrics(serverHttpRequestStats);

    // Record success or error based on the response status
    if (isSuccess && (readResponseStatus == VeniceReadResponseStatus.OK
        || readResponseStatus == VeniceReadResponseStatus.KEY_NOT_FOUND)) {
      requestStatsRecorder.successRequest(serverHttpRequestStats, requestLatency);
    } else if (readResponseStatus != VeniceReadResponseStatus.TOO_MANY_REQUESTS) {
      requestStatsRecorder.errorRequest(serverHttpRequestStats, requestLatency);
    }

    // Mark the callback as executed
    requestStatsRecorder.setStatCallBackExecuted(true);
  }
}
