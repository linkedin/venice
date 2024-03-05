package com.linkedin.venice.listener;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.utils.LatencyUtils;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpResponseStatus;
import it.unimi.dsi.fastutil.ints.IntList;


public class StatsHandler extends ChannelDuplexHandler {
  private final ServerStatsContext serverStatsContext;
  private final AggServerHttpRequestStats singleGetStats;
  private final AggServerHttpRequestStats multiGetStats;
  private final AggServerHttpRequestStats computeStats;

  public StatsHandler(
      AggServerHttpRequestStats singleGetStats,
      AggServerHttpRequestStats multiGetStats,
      AggServerHttpRequestStats computeStats) {
    this.singleGetStats = singleGetStats;
    this.multiGetStats = multiGetStats;
    this.computeStats = computeStats;

    this.serverStatsContext = new ServerStatsContext(singleGetStats, multiGetStats, computeStats);
  }

  public ServerStatsContext getNewStatsContext() {
    return new ServerStatsContext(singleGetStats, multiGetStats, computeStats);
  }

  public void setResponseStatus(HttpResponseStatus status) {
    serverStatsContext.setResponseStatus(status);
  }

  public void setStoreName(String name) {
    serverStatsContext.setStoreName(name);
  }

  public void setHealthCheck(boolean healthCheck) {
    serverStatsContext.setHealthCheck(healthCheck);
  }

  public void setRequestTerminatedEarly() {
    serverStatsContext.setRequestTerminatedEarly();
  }

  public void setRequestType(RequestType requestType) {
    serverStatsContext.setRequestType(requestType);
  }

  public void setRequestKeyCount(int keyCount) {
    serverStatsContext.setRequestKeyCount(keyCount);
  }

  public void setRequestInfo(RouterRequest request) {
    serverStatsContext.setRequestInfo(request);
  }

  public void setRequestSize(int requestSizeInBytes) {
    serverStatsContext.setRequestSize(requestSizeInBytes);
  }

  public void setSuccessRequestKeyCount(int successKeyCount) {
    serverStatsContext.setSuccessRequestKeyCount(successKeyCount);
  }

  public void setDatabaseLookupLatency(double latency) {
    serverStatsContext.setDatabaseLookupLatency(latency);
  }

  public void setReadComputeLatency(double latency) {
    serverStatsContext.setReadComputeLatency(latency);
  }

  public void setReadComputeDeserializationLatency(double latency) {
    serverStatsContext.setReadComputeDeserializationLatency(latency);
  }

  public void setReadComputeSerializationLatency(double latency) {
    serverStatsContext.setReadComputeSerializationLatency(latency);
  }

  public void setDotProductCount(int count) {
    serverStatsContext.setDotProductCount(count);
  }

  public void setCosineSimilarityCount(int count) {
    serverStatsContext.setCosineSimilarityCount(count);
  }

  public void setHadamardProductCount(int count) {
    serverStatsContext.setHadamardProductCount(count);
  }

  public void setCountOperatorCount(int count) {
    serverStatsContext.setCountOperatorCount(count);
  }

  public void setStorageExecutionHandlerSubmissionWaitTime(double storageExecutionSubmissionWaitTime) {
    serverStatsContext.setStorageExecutionHandlerSubmissionWaitTime(storageExecutionSubmissionWaitTime);
  }

  public void setStorageExecutionQueueLen(int storageExecutionQueueLen) {
    serverStatsContext.setStorageExecutionQueueLen(storageExecutionQueueLen);
  }

  public boolean isAssembledMultiChunkLargeValue() {
    return serverStatsContext.isAssembledMultiChunkLargeValue();
  }

  public void setMultiChunkLargeValueCount(int multiChunkLargeValueCount) {
    serverStatsContext.setMultiChunkLargeValueCount(multiChunkLargeValueCount);
  }

  public void setKeySizeList(IntList keySizeList) {
    serverStatsContext.setKeySizeList(keySizeList);
  }

  public void setValueSizeList(IntList valueSizeList) {
    serverStatsContext.setValueSizeList(valueSizeList);
  }

  public void setValueSize(int valueSize) {
    serverStatsContext.setValueSize(valueSize);
  }

  public void setReadComputeOutputSize(int readComputeOutputSize) {
    serverStatsContext.setReadComputeOutputSize(readComputeOutputSize);
  }

  public long getRequestStartTimeInNS() {
    return serverStatsContext.getRequestStartTimeInNS();
  }

  public ServerStatsContext getServerStatsContext() {
    return serverStatsContext;
  }

  public void setMisroutedStoreVersionRequest(boolean misroutedStoreVersionRequest) {
    serverStatsContext.setMisroutedStoreVersion(misroutedStoreVersionRequest);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (serverStatsContext.isNewRequest()) {
      // Reset for every request
      serverStatsContext.resetContext();
      /**
       * For a single 'channelRead' invocation, Netty will guarantee all the following 'channelRead' functions
       * registered by the pipeline to be executed in the same thread.
       */
      ctx.fireChannelRead(msg);
      double firstPartLatency = LatencyUtils.getLatencyInMS(serverStatsContext.getRequestStartTimeInNS());
      serverStatsContext.setFirstPartLatency(firstPartLatency);
    } else {
      // Only works for multi-get request.
      long startTimeOfPart2InNS = System.nanoTime();
      long startTimeInNS = serverStatsContext.getRequestStartTimeInNS();

      serverStatsContext
          .setPartsInvokeDelayLatency(LatencyUtils.convertLatencyFromNSToMS(startTimeOfPart2InNS - startTimeInNS));

      ctx.fireChannelRead(msg);

      serverStatsContext.setSecondPartLatency(LatencyUtils.getLatencyInMS(startTimeOfPart2InNS));
      serverStatsContext.incrementRequestPartCount();
    }
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws VeniceException {
    ChannelFuture future = ctx.writeAndFlush(msg);
    future.addListener((result) -> {
      // reset the StatsHandler for the new request. This is necessary since instances are channel-based
      // and channels are ready for the future requests as soon as the current has been handled.
      serverStatsContext.setNewRequest();

      if (serverStatsContext.getResponseStatus() == null) {
        throw new VeniceException("request status could not be null");
      }

      // we don't record if it is a health check request
      if (serverStatsContext.isHealthCheck()) {
        return;
      }

      /**
       * TODO: Need to do more investigation to figure out why this callback could be triggered
       * multiple times for a single request
       */
      if (!serverStatsContext.isStatCallBackExecuted()) {
        ServerHttpRequestStats serverHttpRequestStats = serverStatsContext.getStoreName() == null
            ? null
            : serverStatsContext.getCurrentStats().getStoreStats(serverStatsContext.getStoreName());
        serverStatsContext.recordBasicMetrics(serverHttpRequestStats);
        double elapsedTime = LatencyUtils.getLatencyInMS(serverStatsContext.getRequestStartTimeInNS());
        // if ResponseStatus is either OK or NOT_FOUND and the channel write is succeed,
        // records a successRequest in stats. Otherwise, records a errorRequest in stats
        // For TOO_MANY_REQUESTS do not record either success or error. Recording as success would give out
        // wrong interpretation of latency, recording error would give out impression that server failed to serve
        if (result.isSuccess() && (serverStatsContext.getResponseStatus().equals(OK)
            || serverStatsContext.getResponseStatus().equals(NOT_FOUND))) {
          serverStatsContext.successRequest(serverHttpRequestStats, elapsedTime);
        } else if (!serverStatsContext.getResponseStatus().equals(TOO_MANY_REQUESTS)) {
          serverStatsContext.errorRequest(serverHttpRequestStats, elapsedTime);
        }
        serverStatsContext.setStatCallBackExecuted(true);
      }
    });
  }
}
