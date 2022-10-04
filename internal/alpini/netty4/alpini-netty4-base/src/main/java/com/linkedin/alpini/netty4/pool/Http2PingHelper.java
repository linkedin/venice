package com.linkedin.alpini.netty4.pool;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.monitoring.CallCompletion;
import com.linkedin.alpini.base.monitoring.CallTracker;
import com.linkedin.alpini.netty4.handlers.Http2PingSendHandler;
import io.netty.channel.Channel;
import io.netty.handler.codec.http2.DefaultHttp2PingFrame;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2PingFrame;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/*
* This class is designed to provide the HTTP/2 ping related functions.
* Each host has an instance of Http2PingHelper to handle its ping calls,
* accessed via the instance of ChannelPoolManagerImpl.Pool.
*
* Two ping call trackers are used:
*
* 1. A normal call tracker: CallTracker _pingCallTracker.
* This is used to track all the ping call info and expose it to ingraph
* for monitoring purposes.
*
* 2. A customized call tracker: LatestPingCallTimeTracker _latestPingCallTimeTracker.
* This is designed and used to keep the response time of latest ping calls.
* This metric will be used as SN health metric in terms of connection health.
*
* @author Binbing Hou <bhou@linkedin.com>
* */
public class Http2PingHelper implements Http2PingCallListener {
  private final static Logger LOG = LogManager.getLogger(Http2PingHelper.class);

  private static final int NUM_OF_LATEST_PINGS_TO_TRACK = 5;

  private final CallTracker _pingCallTracker;
  private CallCompletion _callCompletion;
  private long _lastPingId;

  private final LatestPingCallTimeTracker _latestPingCallTimeTracker;
  private final Http2PingSendHandler _http2PingSendHandler;

  public Http2PingHelper() {
    _pingCallTracker = createPingCallTracker();
    _latestPingCallTimeTracker = createLatestPingCallTimeTracker();
    _http2PingSendHandler = createHttp2PingSendHandler();
  }

  public CallTracker createPingCallTracker() {
    return CallTracker.create();
  }

  public LatestPingCallTimeTracker createLatestPingCallTimeTracker() {
    return new LatestPingCallTimeTracker();
  }

  public Http2PingSendHandler createHttp2PingSendHandler() {
    return new Http2PingSendHandler(this);
  }

  public void sendPing(@Nonnull Channel channel) {
    if (channel.pipeline().get(Http2ConnectionHandler.class) == null) {
      LOG.debug(
          "Channel={} does not have a Http2ConnectionHandler in the pipeline. It is possible that "
              + "it is an HTTP/1 channel or an HTTP/2 channel in the middle of setup/teardown",
          channel);
      return;
    }
    // take the current nanoseconds as the id of the ping frame
    Http2PingFrame defaultHttp2PingFrame = new DefaultHttp2PingFrame(Time.nanoTime());
    channel.writeAndFlush(defaultHttp2PingFrame).addListener(writeFuture -> {
      if (!writeFuture.isSuccess()) {
        LOG.error(
            "Failed to write the PING frame={} to the HTTP/2 Channel={}",
            defaultHttp2PingFrame,
            channel,
            writeFuture.cause());
      } else {
        LOG.debug("Succeed to write PING frame={} to the HTTP/2 Channel={}", defaultHttp2PingFrame, channel);
      }
    });
  }

  public CallTracker pingCallTracker() {
    return _pingCallTracker;
  }

  public double getAvgResponseTimeOfLatestPings() {
    return _latestPingCallTimeTracker.getAvgResponseTimeOfLatestPings();
  }

  public Http2PingSendHandler getHttp2PingSendHandler() {
    return _http2PingSendHandler;
  }

  @Override
  public synchronized void callStart(long pingId, long timeNanos) {
    if (_callCompletion != null) {
      // Generally, _callCompletion is not possible to be null.
      // In case any issue could make _callCompletion != null, close the call with callCloseWithError.
      LOG.warn(
          "_callCompletion is not null, but is expected to be null "
              + "when a new ping call id={} starts; last ping id={}",
          pingId,
          _lastPingId);
      callCloseWithError(_lastPingId, Time.nanoTime());
    }
    _lastPingId = pingId;
    _callCompletion = _pingCallTracker.startCall(timeNanos);
    _latestPingCallTimeTracker.callStart(timeNanos);
  }

  @Override
  public synchronized void callClose(long pingId, long timeNanos) {
    if (_callCompletion != null) {
      if (pingId != _lastPingId) {
        LOG.warn(
            "callClose is trying to close the ping call with id={}, " + "but the call was started with id={}",
            pingId,
            _lastPingId);
        return;
      }
      _callCompletion.close(timeNanos);
      _latestPingCallTimeTracker.callClose(timeNanos);
      _callCompletion = null;
    } else {
      LOG.warn(
          "callClose is trying to close a ping call id={} " + "that has not been started; last ping call id={}",
          pingId,
          _lastPingId);
    }
  }

  @Override
  public synchronized void callCloseWithError(long pingId, long timeNanos) {
    if (_callCompletion != null) {
      if (pingId != _lastPingId) {
        LOG.warn(
            "callCloseWithError is trying to close the ping call with id={}, "
                + "but the last ping call was started with id={}",
            pingId,
            _lastPingId);
        return;
      }
      _callCompletion.close(timeNanos);
      _latestPingCallTimeTracker.callClose(timeNanos);
      _callCompletion = null;
    } else {
      LOG.warn(
          "callCloseWithError is trying to close a ping call id={} "
              + "that has not been started; last ping call id={}",
          pingId,
          _lastPingId);
    }
  }

  @Override
  public synchronized boolean isCallComplete(long pingId) {
    if (_lastPingId != pingId) {
      LOG.warn("isCallComplete is checking pingId={}, but the last ping id ={}", pingId, _lastPingId);
    }
    return _callCompletion == null;
  }

  private static class LatestPingCallTimeTracker {

    // The latest response times are stored in a circular array for low overhead.
    // Each host has an instance of Http2PingHelper and only updates the latest
    // ping response time every ping interval. Therefore, there are no concurrent issues.
    private final long[] _circularArray = new long[NUM_OF_LATEST_PINGS_TO_TRACK];
    private int _currentPingPos = -1;
    public long _lastPingSendTime;

    LatestPingCallTimeTracker() {
    }

    private void callStart(long timeNanos) {
      _lastPingSendTime = timeNanos;
    }

    private void callClose(long timeNanos) {
      long responseTime = timeNanos - _lastPingSendTime;
      updateLatestPingResponseTime(responseTime);
    }

    private void updateLatestPingResponseTime(long pingResponseTimeNanos) {
      _currentPingPos = (_currentPingPos + 1) % NUM_OF_LATEST_PINGS_TO_TRACK;
      _circularArray[_currentPingPos] = pingResponseTimeNanos;
    }

    private double getAvgResponseTimeOfLatestPings() {
      // if no enough ping call times, return 0
      if (_circularArray[NUM_OF_LATEST_PINGS_TO_TRACK - 1] == 0) {
        return 0;
      }
      double totalTime = 0;
      for (int i = 0; i < NUM_OF_LATEST_PINGS_TO_TRACK; i++) {
        totalTime += _circularArray[i];
      }
      return totalTime / NUM_OF_LATEST_PINGS_TO_TRACK;
    }
  }

  public static double nanoToMillis(double nanos) {
    return nanos / 1000000.0;
  }
}
