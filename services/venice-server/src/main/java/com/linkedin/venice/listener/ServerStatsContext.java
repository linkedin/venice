package com.linkedin.venice.listener;

import static com.linkedin.venice.listener.response.stats.ResponseStatsUtil.consumeIntIfAbove;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.response.stats.ReadResponseStatsRecorder;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import io.netty.handler.codec.http.HttpResponseStatus;


/**
 * We need to be able to record server side statistics for gRPC requests. The current StatsHandler in the Netty
 * Pipeline maintains instance variables per channel, and guarantees that each request will be handled by the same
 * thread, thus we cannot augment StatsHandler in the same way as other handlers for gRPC requests. We create a
 * new StatsContext for each gRPC request, so we can maintain per request stats which will be aggregated on request
 * completion using references to the proper Metrics Repository in AggServerHttpRequestStats. This class is almost a
 * direct copy of StatsHandler, without Netty Channel Read/Write logic.
 */
public class ServerStatsContext {
  private ReadResponseStatsRecorder responseStatsRecorder;
  private long startTimeInNS;
  private HttpResponseStatus responseStatus;
  private String storeName = null;
  private boolean isMetadataRequest;
  private int requestKeyCount = -1;
  private int requestSizeInBytes = -1;
  private boolean isRequestTerminatedEarly = false;
  private final AggServerHttpRequestStats singleGetStats;
  private final AggServerHttpRequestStats multiGetStats;
  private final AggServerHttpRequestStats computeStats;
  private AggServerHttpRequestStats currentStats;
  private RequestType requestType = RequestType.SINGLE_GET;

  // a flag that indicates if this is a new HttpRequest. Netty is TCP-based, so a HttpRequest is chunked into packages.
  // Set the startTimeInNS in ChannelRead if it is the first package within a HttpRequest.
  private boolean newRequest = true;
  /**
   * To indicate whether the stat callback has been triggered or not for a given request.
   * This is mostly to bypass the issue that stat callback could be triggered multiple times for one single request.
   */
  private boolean statCallbackExecuted = false;

  private boolean isMisroutedStoreVersion = false;
  private double flushLatency = -1;
  private int responseSize = -1;

  public boolean isNewRequest() {
    return newRequest;
  }

  public ServerStatsContext(
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
    isMetadataRequest = false;
    responseStatus = null;
    statCallbackExecuted = false;
    requestKeyCount = -1;
    requestSizeInBytes = -1;
    isRequestTerminatedEarly = false;
    isMisroutedStoreVersion = false;
    flushLatency = -1;
    responseSize = -1;

    newRequest = false;
  }

  public void setReadResponseStats(ReadResponseStatsRecorder responseStatsRecorder) {
    this.responseStatsRecorder = responseStatsRecorder;
  }

  public void setNewRequest() {
    this.newRequest = true;
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

  public void setResponseStatus(HttpResponseStatus status) {
    this.responseStatus = status;
  }

  public String getStoreName() {
    return storeName;
  }

  public void setStoreName(String name) {
    this.storeName = name;
  }

  public void setMetadataRequest(boolean metadataRequest) {
    this.isMetadataRequest = metadataRequest;
  }

  public void setRequestTerminatedEarly() {
    this.isRequestTerminatedEarly = true;
  }

  public HttpResponseStatus getResponseStatus() {
    return responseStatus;
  }

  public void setRequestType(RequestType requestType) {
    this.requestType = requestType;
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
  }

  public RequestType getRequestType() {
    return requestType;
  }

  public void setRequestKeyCount(int keyCount) {
    this.requestKeyCount = keyCount;
  }

  public AggServerHttpRequestStats getCurrentStats() {
    return currentStats;
  }

  public void setRequestInfo(RouterRequest request) {
    setStoreName(request.getStoreName());
    setRequestType(request.getRequestType());
    setRequestKeyCount(request.getKeyCount());
  }

  public void setRequestSize(int requestSizeInBytes) {
    this.requestSizeInBytes = requestSizeInBytes;
  }

  public long getRequestStartTimeInNS() {
    return this.startTimeInNS;
  }

  public void setFlushLatency(double latency) {
    this.flushLatency = latency;
  }

  public void setResponseSize(int size) {
    this.responseSize = size;
  }

  public void recordBasicMetrics(ServerHttpRequestStats serverHttpRequestStats) {
    if (serverHttpRequestStats != null && responseStatus != null) {
      // Compute HTTP status dimensions once, used by recordMetrics (for value size) and recordResponseSize.
      int statusCode = responseStatus.code();
      HttpResponseStatusEnum statusEnum = HttpResponseStatusEnum.transformIntToHttpResponseStatusEnum(statusCode);
      HttpResponseStatusCodeCategory statusCategory =
          HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory(statusCode);
      VeniceResponseStatusCategory veniceCategory =
          (responseStatus.equals(HttpResponseStatus.OK) || responseStatus.equals(HttpResponseStatus.NOT_FOUND))
              ? VeniceResponseStatusCategory.SUCCESS
              : VeniceResponseStatusCategory.FAIL;

      if (this.responseStatsRecorder != null) {
        this.responseStatsRecorder.recordMetrics(serverHttpRequestStats, statusEnum, statusCategory, veniceCategory);
      }

      consumeIntIfAbove(serverHttpRequestStats::recordRequestKeyCount, this.requestKeyCount, 0);
      consumeIntIfAbove(serverHttpRequestStats::recordRequestSizeInBytes, this.requestSizeInBytes, 0);

      if (this.isRequestTerminatedEarly) {
        serverHttpRequestStats.recordEarlyTerminatedEarlyRequest();
      }
      if (flushLatency >= 0) {
        serverHttpRequestStats.recordFlushLatency(flushLatency);
      }
      if (responseSize >= 0) {
        serverHttpRequestStats.recordResponseSize(statusEnum, statusCategory, veniceCategory, responseSize);
      }
    }
  }

  // This method does not have to be synchronized since Tehuti Sensor.record() is internally synchronized
  // and OTel SDK recording methods are thread-safe. Please re-consider if new logic is added.
  public void successRequest(ServerHttpRequestStats stats, double elapsedTime) {
    if (stats == null) {
      throw new VeniceException("store name could not be null if request succeeded");
    }

    int statusCode = responseStatus != null ? responseStatus.code() : HttpResponseStatus.OK.code();
    HttpResponseStatusEnum statusEnum = HttpResponseStatusEnum.transformIntToHttpResponseStatusEnum(statusCode);
    HttpResponseStatusCodeCategory statusCategory =
        HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory(statusCode);
    VeniceResponseStatusCategory veniceCategory = VeniceResponseStatusCategory.SUCCESS;

    stats.recordSuccessRequest(statusEnum, statusCategory, veniceCategory);
    stats.recordSuccessRequestLatency(statusEnum, statusCategory, veniceCategory, elapsedTime);
  }

  public void errorRequest(ServerHttpRequestStats stats, double elapsedTime) {
    int statusCode = responseStatus != null ? responseStatus.code() : 500;
    HttpResponseStatusEnum statusEnum = HttpResponseStatusEnum.transformIntToHttpResponseStatusEnum(statusCode);
    HttpResponseStatusCodeCategory statusCategory =
        HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory(statusCode);
    VeniceResponseStatusCategory veniceCategory = VeniceResponseStatusCategory.FAIL;

    if (stats == null) {
      currentStats.recordErrorRequest(statusEnum, statusCategory, veniceCategory);
      currentStats.recordErrorRequestLatency(statusEnum, statusCategory, veniceCategory, elapsedTime);
      if (isMisroutedStoreVersion) {
        currentStats.recordMisroutedStoreVersionRequest();
      }
    } else {
      stats.recordErrorRequest(statusEnum, statusCategory, veniceCategory);
      stats.recordErrorRequestLatency(statusEnum, statusCategory, veniceCategory, elapsedTime);
      if (isMisroutedStoreVersion) {
        stats.recordMisroutedStoreVersionRequest();
      }
    }
  }

  public int getRequestKeyCount() {
    return requestKeyCount;
  }

  public void setMisroutedStoreVersion(boolean misroutedStoreVersion) {
    isMisroutedStoreVersion = misroutedStoreVersion;
  }
}
