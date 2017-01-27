package com.linkedin.venice.listener;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpResponseStatus;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class StatsHandler extends ChannelDuplexHandler {

  private long startTime;
  private HttpResponseStatus responseStatus;
  private String storeName;
  private boolean isHealthCheck;
  private final AggServerHttpRequestStats stats;

  //a flag that indicates if this is a new HttpRequest. Netty is TCP-based, so a HttpRequest is chunked into packages.
  //Set the startTime in ChannelRead if it is the first package within a HttpRequest.
  private boolean newRequest = true;

  public void setResponseStatus(HttpResponseStatus status) {
    this.responseStatus = status;
  }

  public void setStoreName(String name) {
    this.storeName = name;
  }

  public void setHealthCheck(boolean healthCheck) {
    this.isHealthCheck = healthCheck;
  }

  public StatsHandler(AggServerHttpRequestStats stats) {
    this.stats = stats;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (newRequest) {
      startTime = System.currentTimeMillis();
      isHealthCheck = false;
      responseStatus = null;

      newRequest = false;
    }
    ctx.fireChannelRead(msg);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws VeniceException {
    ChannelFuture future = ctx.writeAndFlush(msg);
    future.addListener((result) -> {
      if (responseStatus == null) {
        throw new VeniceException("request status could not be null");
      }

      //we don't record if it is a health check request
      if (isHealthCheck) {
        return;
      }

      long elapsedTime = System.currentTimeMillis() - startTime;

      //if ResponseStatus is either OK or NOT_FOUND and the channel write is succeed,
      //records a successRequest in stats. Otherwise, records a errorRequest in stats;
      if (result.isSuccess()
        && (responseStatus == OK || responseStatus == NOT_FOUND)) {
        successRequest(elapsedTime);
      } else {
        errorRequest(elapsedTime);
      }

      //reset the StatsHandler for the new request. This is necessary since instances are channel-based
      // and channels are ready for the future requests as soon as the current has been handled.
      newRequest = true;
    });
  }

  //This method does not have to be synchronised since operations in Tehuti are already synchronised.
  //Please re-consider the race condition if new logic is added.
  private void successRequest(long elapsedTime) {
    if (storeName != null) {
      stats.recordSuccessRequest(storeName);
      stats.recordSuccessRequestLatency(storeName, elapsedTime);
    } else {
      throw new VeniceException("store name could not be null if request succeeded");
    }
  }

  private void errorRequest(long elapsedTime) {
    if (storeName == null) {
      stats.recordErrorRequest();
      stats.recordErrorRequestLatency(elapsedTime);
    } else {
      stats.recordErrorRequest(storeName);
      stats.recordErrorRequestLatency(storeName, elapsedTime);
    }
  }
}