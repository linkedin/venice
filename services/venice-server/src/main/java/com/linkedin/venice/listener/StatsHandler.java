package com.linkedin.venice.listener;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import com.linkedin.venice.utils.LatencyUtils;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import java.util.List;
import java.util.Optional;


public class StatsHandler extends ChannelDuplexHandler {
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

  private Optional<List<Integer>> optionalKeySizeList = Optional.empty();
  private Optional<List<Integer>> optionalValueSizeList = Optional.empty();

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
  private double storageExecutionSubmissionWaitTime;
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
   * {@link StorageReadRequestsHandler}
   *
   */
  private double firstPartLatency = -1;
  private double secondPartLatency = -1;
  private double partsInvokeDelayLatency = -1;
  private int requestPartCount = -1;

  public void setResponseStatus(HttpResponseStatus status) {
    this.responseStatus = status;
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

  public void setOptionalKeySizeList(Optional<List<Integer>> optionalKeySizeList) {
    this.optionalKeySizeList = optionalKeySizeList;
  }

  public void setOptionalValueSizeList(Optional<List<Integer>> optionalValueSizeList) {
    this.optionalValueSizeList = optionalValueSizeList;
  }

  public StatsHandler(
      AggServerHttpRequestStats singleGetStats,
      AggServerHttpRequestStats multiGetStats,
      AggServerHttpRequestStats computeStats) {
    this.singleGetStats = singleGetStats;
    this.multiGetStats = multiGetStats;
    this.computeStats = computeStats;
    // default to use single-get
    this.currentStats = singleGetStats;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (newRequest) {
      // Reset for every request
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

      optionalKeySizeList = Optional.empty();
      optionalValueSizeList = Optional.empty();

      /**
       * For a single 'channelRead' invocation, Netty will guarantee all the following 'channelRead' functions
       * registered by the pipeline to be executed in the same thread.
       */
      newRequest = false;
      ctx.fireChannelRead(msg);
      firstPartLatency = LatencyUtils.getLatencyInMS(startTimeInNS);
    } else {
      // Only works for multi-get request.
      long startTimeOfPart2InNS = System.nanoTime();
      partsInvokeDelayLatency = LatencyUtils.convertLatencyFromNSToMS(startTimeOfPart2InNS - startTimeInNS);
      ctx.fireChannelRead(msg);
      secondPartLatency = LatencyUtils.getLatencyInMS(startTimeOfPart2InNS);
      ++requestPartCount;
    }
  }

  public long getRequestStartTimeInNS() {
    return this.startTimeInNS;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws VeniceException {
    ChannelFuture future = ctx.writeAndFlush(msg);
    future.addListener((result) -> {
      // reset the StatsHandler for the new request. This is necessary since instances are channel-based
      // and channels are ready for the future requests as soon as the current has been handled.
      newRequest = true;

      if (responseStatus == null) {
        throw new VeniceException("request status could not be null");
      }

      // we don't record if it is a health check request
      if (isHealthCheck) {
        return;
      }

      /**
       * TODO: Need to do more investigation to figure out why this callback could be triggered
       * multiple times for a single request
       */
      if (!statCallbackExecuted) {
        recordBasicMetrics();

        optionalKeySizeList.ifPresent(
            keySizeList -> keySizeList.forEach(keySize -> currentStats.recordKeySizeInByte(storeName, keySize)));
        optionalValueSizeList.ifPresent(
            valueSizeList -> valueSizeList
                .forEach(valueSize -> currentStats.recordValueSizeInByte(storeName, valueSize)));

        double elapsedTime = LatencyUtils.getLatencyInMS(startTimeInNS);
        // if ResponseStatus is either OK or NOT_FOUND and the channel write is succeed,
        // records a successRequest in stats. Otherwise, records a errorRequest in stats;
        if (result.isSuccess() && (responseStatus.equals(OK) || responseStatus.equals(NOT_FOUND))) {
          successRequest(elapsedTime);
        } else {
          errorRequest(elapsedTime);
        }
        statCallbackExecuted = true;
      }
    });
  }

  private void recordBasicMetrics() {
    if (storeName != null) {
      if (databaseLookupLatency >= 0) {
        currentStats.recordDatabaseLookupLatency(storeName, databaseLookupLatency, isAssembledMultiChunkLargeValue());
      }
      if (storageExecutionSubmissionWaitTime >= 0) {
        currentStats.recordStorageExecutionHandlerSubmissionWaitTime(storageExecutionSubmissionWaitTime);
      }
      if (storageExecutionQueueLen >= 0) {
        currentStats.recordStorageExecutionQueueLen(storageExecutionQueueLen);
      }
      if (multiChunkLargeValueCount > 0) {
        // We only record this metric for requests where large values occurred
        currentStats.recordMultiChunkLargeValueCount(storeName, multiChunkLargeValueCount);
      }
      if (requestKeyCount > 0) {
        currentStats.recordRequestKeyCount(storeName, requestKeyCount);
      }
      if (successRequestKeyCount > 0) {
        currentStats.recordSuccessRequestKeyCount(storeName, successRequestKeyCount);
      }
      if (requestSizeInBytes > 0) {
        currentStats.recordRequestSizeInBytes(storeName, requestSizeInBytes);
      }
      if (firstPartLatency > 0) {
        currentStats.recordRequestFirstPartLatency(storeName, firstPartLatency);
      }
      if (partsInvokeDelayLatency > 0) {
        currentStats.recordRequestPartsInvokeDelayLatency(storeName, partsInvokeDelayLatency);
      }
      if (secondPartLatency > 0) {
        currentStats.recordRequestSecondPartLatency(storeName, secondPartLatency);
      }
      if (requestPartCount > 0) {
        currentStats.recordRequestPartCount(storeName, requestPartCount);
      }
      if (readComputeLatency >= 0) {
        currentStats.recordReadComputeLatency(storeName, readComputeLatency, isAssembledMultiChunkLargeValue());
      }
      if (readComputeDeserializationLatency >= 0) {
        currentStats.recordReadComputeDeserializationLatency(
            storeName,
            readComputeDeserializationLatency,
            isAssembledMultiChunkLargeValue());
      }
      if (readComputeSerializationLatency >= 0) {
        currentStats.recordReadComputeSerializationLatency(
            storeName,
            readComputeSerializationLatency,
            isAssembledMultiChunkLargeValue());
      }
      if (dotProductCount > 0) {
        currentStats.recordDotProductCount(storeName, dotProductCount);
      }
      if (cosineSimilarityCount > 0) {
        currentStats.recordCosineSimilarityCount(storeName, cosineSimilarityCount);
      }
      if (hadamardProductCount > 0) {
        currentStats.recordHadamardProductCount(storeName, hadamardProductCount);
      }
      if (countOperatorCount > 0) {
        currentStats.recordCountOperatorCount(storeName, countOperatorCount);
      }
      if (isRequestTerminatedEarly) {
        currentStats.recordEarlyTerminatedEarlyRequest(storeName);
      }
    }
  }

  // This method does not have to be synchronised since operations in Tehuti are already synchronised.
  // Please re-consider the race condition if new logic is added.
  private void successRequest(double elapsedTime) {
    if (storeName != null) {
      currentStats.recordSuccessRequest(storeName);
      currentStats.recordSuccessRequestLatency(storeName, elapsedTime);
    } else {
      throw new VeniceException("store name could not be null if request succeeded");
    }
  }

  private void errorRequest(double elapsedTime) {
    if (storeName == null) {
      currentStats.recordErrorRequest();
      currentStats.recordErrorRequestLatency(elapsedTime);
    } else {
      currentStats.recordErrorRequest(storeName);
      currentStats.recordErrorRequestLatency(storeName, elapsedTime);
    }
  }
}
