package com.linkedin.venice.listener.grpc;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.utils.LatencyUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import it.unimi.dsi.fastutil.ints.IntList;


public class GrpcStatsContext {
  /**
   * We need to be able to record server side statistics for gRPC requests. The current StatsHandler in the Netty
   * Pipeline maintains instance variables per channel, and guarantees that each request will be handled by the same
   * thread, thus we cannot augment StatsHandler in the same way as other handlers for gRPC requests. We create a
   * new StatsContext for each gRPC request, so we can maintain per request stats which will be aggregated on request
   * completion using references to the proper Metrics Repository in AggServerHttpRequestStats. This class is almost a
   * direct copy of StatsHandler, without Netty Channel Read/Write logic.
   */
  private long startTimeInNS;
  private HttpResponseStatus responseStatus;
  private String storeName = null;
  private boolean isHealthCheck;
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

  private IntList keySizeList;
  private IntList valueSizeList;

  private int valueSize = 0;
  private int readComputeOutputSize = 0;

  private final AggServerHttpRequestStats singleGetStats;
  private final AggServerHttpRequestStats multiGetStats;
  private final AggServerHttpRequestStats computeStats;
  private AggServerHttpRequestStats currentStats;

  private boolean newRequest = true;
  private boolean statCallbackExecuted = false;
  private double storageExecutionSubmissionWaitTime;
  private int storageExecutionQueueLen;
  private double firstPartLatency = -1;
  private double secondPartLatency = -1;
  private double partsInvokeDelayLatency = -1;
  private int requestPartCount = -1;
  private boolean isComplete;

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

  public GrpcStatsContext(
      AggServerHttpRequestStats singleGetStats,
      AggServerHttpRequestStats multiGetStats,
      AggServerHttpRequestStats computeStats) {
    this.singleGetStats = singleGetStats;
    this.multiGetStats = multiGetStats;
    this.computeStats = computeStats;

    storeName = null;
    startTimeInNS = System.nanoTime();
    partsInvokeDelayLatency = -1;
    secondPartLatency = -1;
    requestPartCount = 1;
    isHealthCheck = false;
    responseStatus = null;
    statCallbackExecuted = false;
    databaseLookupLatency = -1;
    storageExecutionSubmissionWaitTime = -1;
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

    newRequest = false;
    firstPartLatency = LatencyUtils.getLatencyInMS(startTimeInNS);
    secondPartLatency = LatencyUtils.getLatencyInMS(startTimeInNS);
    partsInvokeDelayLatency = 0;
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

  public void setHealthCheck(boolean healthCheck) {
    this.isHealthCheck = healthCheck;
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
        currentStats = multiGetStats;
        break;
      case COMPUTE:
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

  public void setStorageExecutionHandlerSubmissionWaitTime(double storageExecutionSubmissionWaitTime) {
    this.storageExecutionSubmissionWaitTime = storageExecutionSubmissionWaitTime;
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
    if (storeName != null) {
      if (databaseLookupLatency >= 0) {
        serverHttpRequestStats.recordDatabaseLookupLatency(databaseLookupLatency, isAssembledMultiChunkLargeValue());
      }
      if (storageExecutionSubmissionWaitTime >= 0) {
        currentStats.recordStorageExecutionHandlerSubmissionWaitTime(storageExecutionSubmissionWaitTime);
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
          if (valueSizeList.getInt(i) != -1)
            serverHttpRequestStats.recordValueSizeInByte(valueSizeList.getInt(i));
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
    if (storeName != null) {
      stats.recordSuccessRequest();
      stats.recordSuccessRequestLatency(elapsedTime);
    } else {
      throw new VeniceException("store name could not be null if request succeeded");
    }
  }

  public void errorRequest(ServerHttpRequestStats stats, double elapsedTime) {
    isComplete = true;
    if (storeName == null) {
      currentStats.recordErrorRequest();
      currentStats.recordErrorRequestLatency(elapsedTime);
    } else {
      stats.recordErrorRequest();
      stats.recordErrorRequestLatency(elapsedTime);
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
}
