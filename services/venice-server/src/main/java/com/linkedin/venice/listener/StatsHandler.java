package com.linkedin.venice.listener;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.utils.LatencyUtils;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpResponseStatus;


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

  public void setMetadataRequest(boolean metadataRequest) {
    serverStatsContext.setMetadataRequest(metadataRequest);
  }

  public void setRequestTerminatedEarly() {
    serverStatsContext.setRequestTerminatedEarly();
  }

  public void setRequestInfo(RouterRequest request) {
    serverStatsContext.setRequestInfo(request);
  }

  public void setRequestSize(int requestSizeInBytes) {
    serverStatsContext.setRequestSize(requestSizeInBytes);
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
      double firstPartLatency = LatencyUtils.getElapsedTimeFromNSToMS(serverStatsContext.getRequestStartTimeInNS());
      serverStatsContext.setFirstPartLatency(firstPartLatency);
    } else {
      // Only works for multi-get request.
      long startTimeOfPart2InNS = System.nanoTime();
      long startTimeInNS = serverStatsContext.getRequestStartTimeInNS();

      serverStatsContext.setPartsInvokeDelayLatency(LatencyUtils.convertNSToMS(startTimeOfPart2InNS - startTimeInNS));

      ctx.fireChannelRead(msg);

      serverStatsContext.setSecondPartLatency(LatencyUtils.getElapsedTimeFromNSToMS(startTimeOfPart2InNS));
      serverStatsContext.incrementRequestPartCount();
    }
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws VeniceException {
    ChannelFuture future = ctx.writeAndFlush(msg);
    long beforeFlushTimestampNs = System.nanoTime();
    future.addListener((result) -> {
      // reset the StatsHandler for the new request. This is necessary since instances are channel-based
      // and channels are ready for the future requests as soon as the current has been handled.
      serverStatsContext.setNewRequest();

      if (serverStatsContext.getResponseStatus() == null) {
        throw new VeniceException("request status could not be null");
      }

      // we don't record if it is a metatadata request
      if (serverStatsContext.isMetadataRequest()) {
        return;
      }

      /**
       * TODO: Need to do more investigation to figure out why this callback could be triggered
       * multiple times for a single request
       */
      if (!serverStatsContext.isStatCallBackExecuted()) {
        serverStatsContext.setFlushLatency(LatencyUtils.getElapsedTimeFromNSToMS(beforeFlushTimestampNs));
        ServerHttpRequestStats serverHttpRequestStats = serverStatsContext.getStoreName() == null
            ? null
            : serverStatsContext.getCurrentStats().getStoreStats(serverStatsContext.getStoreName());
        serverStatsContext.recordBasicMetrics(serverHttpRequestStats);
        double elapsedTime = LatencyUtils.getElapsedTimeFromNSToMS(serverStatsContext.getRequestStartTimeInNS());
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
