package com.linkedin.venice.listener;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import it.unimi.dsi.fastutil.ints.IntList;


/**
 * We need to be able to record server side statistics for gRPC requests. The current StatsHandler in the Netty
 * Pipeline maintains instance variables per channel, and guarantees that each request will be handled by the same
 * thread, thus we cannot augment StatsHandler in the same way as other handlers for gRPC requests. We create a
 * new StatsContext for each gRPC request, so we can maintain per request stats which will be aggregated on request
 * completion using references to the proper Metrics Repository in AggServerHttpRequestStats. This class is almost a
 * direct copy of StatsHandler, without Netty Channel Read/Write logic.
 */
public class ServerStatsContext {
  private long startTimeInNS;
  private HttpResponseStatus responseStatus;
  private String storeName = null;
  private boolean isMetadataRequest;
  private double databaseLookupLatency = -1;
  private int multiChunkLargeValueCount = -1;
  private int requestKeyCount = -1;
  private int successRequestKeyCount = -1;
  private int requestSizeInBytes = -1;
  private double readComputeLatency = -1;
  private double readComputeDeserializationLatency = -1;
  private double readComputeSerializationLatency = -1;
  private int dotProductCount = 0;
  private int cosineSimilarityCount = 0;
  private int hadamardProductCount = 0;
  private int countOperatorCount = 0;
  private boolean isRequestTerminatedEarly = false;
  private long responseWriteAndFlushStartTimeNanos = -1;

  private IntList keySizeList;
  private IntList valueSizeList;

  private int valueSize = 0;
  private int readComputeOutputSize = 0;

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
  private int storageExecutionQueueLen;

  /**
   * Normally, one multi-get request will be split into two parts, and it means
   * {@link StatsHandler#channelRead(ChannelHandlerContext, Object)} will be invoked twice.
   *
   * 'firstPartLatency' will measure the time took by:
   * {@link StatsHandler}
   * {@link HttpServerCodec}
   * {@link HttpObjectAggregator}
   *
   * 'partsInvokeDelayLatency' will measure the delay between the invocation of part1
   * and the invocation of part2;
   *
   * 'secondPartLatency' will measure the time took by:
   * {@link StatsHandler}
   * {@link HttpServerCodec}
   * {@link HttpObjectAggregator}
   * {@link VerifySslHandler}
   * {@link ServerAclHandler}
   * {@link RouterRequestHttpHandler}
   * {@link StorageReadRequestHandler}
   *
   */
  private double firstPartLatency = -1;
  private double secondPartLatency = -1;
  private double partsInvokeDelayLatency = -1;
  private int requestPartCount = -1;
  private boolean isComplete;

  private boolean isMisroutedStoreVersion = false;

  public boolean isNewRequest() {
    return newRequest;
  }

  public double getSecondPartLatency() {
    return secondPartLatency;
  }

  public void setSecondPartLatency(double secondPartLatency) {
    this.secondPartLatency = secondPartLatency;
  }

  public double getPartsInvokeDelayLatency() {
    return partsInvokeDelayLatency;
  }

  public void setPartsInvokeDelayLatency(double partsInvokeDelayLatency) {
    this.partsInvokeDelayLatency = partsInvokeDelayLatency;
  }

  public int getRequestPartCount() {
    return requestPartCount;
  }

  public void setRequestPartCount(int requestPartCount) {
    this.requestPartCount = requestPartCount;
  }

  public void incrementRequestPartCount() {
    this.requestPartCount++;
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
    storeName = null;
    startTimeInNS = System.nanoTime();
    partsInvokeDelayLatency = -1;
    secondPartLatency = -1;
    requestPartCount = 1;
    isMetadataRequest = false;
    responseStatus = null;
    statCallbackExecuted = false;
    databaseLookupLatency = -1;
    storageExecutionQueueLen = -1;
    requestKeyCount = -1;
    successRequestKeyCount = -1;
    requestSizeInBytes = -1;
    multiChunkLargeValueCount = -1;
    readComputeLatency = -1;
    readComputeDeserializationLatency = -1;
    readComputeSerializationLatency = -1;
    dotProductCount = 0;
    cosineSimilarityCount = 0;
    hadamardProductCount = 0;
    isRequestTerminatedEarly = false;
    isComplete = false;
    isMisroutedStoreVersion = false;
    newRequest = false;
    responseWriteAndFlushStartTimeNanos = -1;
  }

  public void setFirstPartLatency(double firstPartLatency) {
    this.firstPartLatency = firstPartLatency;
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

  public void setSuccessRequestKeyCount(int successKeyCount) {
    this.successRequestKeyCount = successKeyCount;
  }

  public void setDatabaseLookupLatency(double latency) {
    this.databaseLookupLatency = latency;
  }

  public void setReadComputeLatency(double latency) {
    this.readComputeLatency = latency;
  }

  public void setReadComputeDeserializationLatency(double latency) {
    this.readComputeDeserializationLatency = latency;
  }

  public void setReadComputeSerializationLatency(double latency) {
    this.readComputeSerializationLatency = latency;
  }

  public void setDotProductCount(int count) {
    this.dotProductCount = count;
  }

  public void setCosineSimilarityCount(int count) {
    this.cosineSimilarityCount = count;
  }

  public void setHadamardProductCount(int count) {
    this.hadamardProductCount = count;
  }

  public void setCountOperatorCount(int count) {
    this.countOperatorCount = count;
  }

  public void setStorageExecutionQueueLen(int storageExecutionQueueLen) {
    this.storageExecutionQueueLen = storageExecutionQueueLen;
  }

  public boolean isAssembledMultiChunkLargeValue() {
    return multiChunkLargeValueCount > 0;
  }

  public void setMultiChunkLargeValueCount(int multiChunkLargeValueCount) {
    this.multiChunkLargeValueCount = multiChunkLargeValueCount;
  }

  public void setKeySizeList(IntList keySizeList) {
    this.keySizeList = keySizeList;
  }

  public void setValueSizeList(IntList valueSizeList) {
    this.valueSizeList = valueSizeList;
  }

  public long getRequestStartTimeInNS() {
    return this.startTimeInNS;
  }

  public void recordBasicMetrics(ServerHttpRequestStats serverHttpRequestStats) {
    if (serverHttpRequestStats != null) {
      if (databaseLookupLatency >= 0) {
        serverHttpRequestStats.recordDatabaseLookupLatency(databaseLookupLatency, isAssembledMultiChunkLargeValue());
      }
      if (storageExecutionQueueLen >= 0) {
        currentStats.recordStorageExecutionQueueLen(storageExecutionQueueLen);
      }
      if (multiChunkLargeValueCount > 0) {
        // We only record this metric for requests where large values occurred
        serverHttpRequestStats.recordMultiChunkLargeValueCount(multiChunkLargeValueCount);
      }
      if (requestKeyCount > 0) {
        serverHttpRequestStats.recordRequestKeyCount(requestKeyCount);
      }
      if (successRequestKeyCount > 0) {
        serverHttpRequestStats.recordSuccessRequestKeyCount(successRequestKeyCount);
      }
      if (requestSizeInBytes > 0) {
        serverHttpRequestStats.recordRequestSizeInBytes(requestSizeInBytes);
      }
      if (firstPartLatency > 0) {
        serverHttpRequestStats.recordRequestFirstPartLatency(firstPartLatency);
      }
      if (partsInvokeDelayLatency > 0) {
        serverHttpRequestStats.recordRequestPartsInvokeDelayLatency(partsInvokeDelayLatency);
      }
      if (secondPartLatency > 0) {
        serverHttpRequestStats.recordRequestSecondPartLatency(secondPartLatency);
      }
      if (requestPartCount > 0) {
        serverHttpRequestStats.recordRequestPartCount(requestPartCount);
      }
      if (readComputeLatency >= 0) {
        serverHttpRequestStats.recordReadComputeLatency(readComputeLatency, isAssembledMultiChunkLargeValue());
      }
      if (readComputeDeserializationLatency >= 0) {
        serverHttpRequestStats.recordReadComputeDeserializationLatency(
            readComputeDeserializationLatency,
            isAssembledMultiChunkLargeValue());
      }
      if (readComputeSerializationLatency >= 0) {
        serverHttpRequestStats
            .recordReadComputeSerializationLatency(readComputeSerializationLatency, isAssembledMultiChunkLargeValue());
      }
      if (dotProductCount > 0) {
        serverHttpRequestStats.recordDotProductCount(dotProductCount);
      }
      if (cosineSimilarityCount > 0) {
        serverHttpRequestStats.recordCosineSimilarityCount(cosineSimilarityCount);
      }
      if (hadamardProductCount > 0) {
        serverHttpRequestStats.recordHadamardProduct(hadamardProductCount);
      }
      if (countOperatorCount > 0) {
        serverHttpRequestStats.recordCountOperator(countOperatorCount);
      }
      if (isRequestTerminatedEarly) {
        serverHttpRequestStats.recordEarlyTerminatedEarlyRequest();
      }
      if (keySizeList != null) {
        for (int i = 0; i < keySizeList.size(); i++) {
          serverHttpRequestStats.recordKeySizeInByte(keySizeList.getInt(i));
        }
      }
      if (valueSizeList != null) {
        for (int i = 0; i < valueSizeList.size(); i++) {
          if (valueSizeList.getInt(i) != -1) {
            serverHttpRequestStats.recordValueSizeInByte(valueSizeList.getInt(i));
          }
        }
      }
      if (readComputeOutputSize > 0) {
        serverHttpRequestStats.recordReadComputeEfficiency((double) valueSize / readComputeOutputSize);
      }
    }
  }

  // This method does not have to be synchronized since operations in Tehuti are already synchronized.
  // Please re-consider the race condition if new logic is added.
  public void successRequest(ServerHttpRequestStats stats, double elapsedTime) {
    isComplete = true;
    if (stats != null) {
      stats.recordSuccessRequest();
      stats.recordSuccessRequestLatency(elapsedTime);
    } else {
      throw new VeniceException("store name could not be null if request succeeded");
    }
  }

  public void errorRequest(ServerHttpRequestStats stats, double elapsedTime) {
    isComplete = true;
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
  }

  public void setValueSize(int size) {
    this.valueSize = size;
  }

  public void setReadComputeOutputSize(int size) {
    this.readComputeOutputSize = size;
  }

  public int getRequestKeyCount() {
    return requestKeyCount;
  }

  public boolean isComplete() {
    return isComplete;
  }

  public void setMisroutedStoreVersion(boolean misroutedStoreVersion) {
    isMisroutedStoreVersion = misroutedStoreVersion;
  }

  public boolean isMisroutedStoreVersion() {
    return isMisroutedStoreVersion;
  }

  public void setResponseWriteAndFlushStartTimeNanos(long startTimeNanos) {
    responseWriteAndFlushStartTimeNanos = startTimeNanos;
  }

  public long getResponseWriteAndFlushStartTimeNanos() {
    return responseWriteAndFlushStartTimeNanos;
  }
}
