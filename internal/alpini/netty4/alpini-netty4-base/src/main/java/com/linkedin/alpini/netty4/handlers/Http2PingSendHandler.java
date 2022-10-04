package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.netty4.pool.Http2PingCallListener;
import com.linkedin.alpini.netty4.pool.Http2PingHelper;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.Http2PingFrame;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/*
* This handler is designed to handle the http2 ping and update the ping call listener.
* It is sharable since there is only one connection handling http2 ping for each host every ping interval.
*
* @author Binbing Hou <bhou@linkedin.com>
* */
@ChannelHandler.Sharable
public class Http2PingSendHandler extends ChannelDuplexHandler {
  private final static Logger LOG = LogManager.getLogger(Http2PingSendHandler.class);

  private long _lastPingSendTime;
  private long _lastPingAckTime;
  private long _lastPingId;
  private Channel _lastPingChannel;

  private final Http2PingCallListener _http2PingCallListener;

  public Http2PingSendHandler(@Nonnull Http2PingCallListener http2PingCallListener) {
    _http2PingCallListener = http2PingCallListener;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof Http2PingFrame) {
      long currentTimeNanos = getCurrentTimeNanos();
      Http2PingFrame http2PingFrame = (Http2PingFrame) msg;

      // When writing the ping frame, we check whether the ack of the previous ping is received.
      // If not, we consider this as a timeout and take the elapsed time as the ping response time.
      // The ack of the previous ping (identified as ping id) will be ignored even if being received later.
      if (!_http2PingCallListener.isCallComplete(_lastPingId)) {
        _http2PingCallListener.callCloseWithError(_lastPingId, currentTimeNanos);

        LOG.warn(
            "Failed to receive PING ACK from last ping send channel={} with elapsed time={} millisecond(s) "
                + "ping send handler={} since the last ping, of which sent nano timestamp={}, id={}",
            _lastPingChannel,
            Http2PingHelper.nanoToMillis(currentTimeNanos - _lastPingSendTime),
            this,
            _lastPingSendTime,
            _lastPingId);
      }

      _lastPingId = http2PingFrame.content();
      _lastPingSendTime = currentTimeNanos;
      _lastPingAckTime = 0;
      _lastPingChannel = ctx.channel();

      _http2PingCallListener.callStart(_lastPingId, currentTimeNanos);

      LOG.debug(
          "Sent PING frame={} to Channel={}, current nano timestamp={}",
          http2PingFrame,
          ctx.channel(),
          _lastPingSendTime);
    }

    super.write(ctx, msg, promise);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof Http2PingFrame) {
      Http2PingFrame http2PingFrame = (Http2PingFrame) msg;
      if (http2PingFrame.ack() && http2PingFrame.content() == _lastPingId) {
        long currentTimeNanos = getCurrentTimeNanos();
        _http2PingCallListener.callClose(http2PingFrame.content(), currentTimeNanos);
        _lastPingAckTime = currentTimeNanos;

        LOG.debug(
            "Received PING ACK frame={} from Channel={}, response time={} nanosecond(s), current nano timestamp={}",
            http2PingFrame,
            ctx.channel(),
            currentTimeNanos - _lastPingSendTime,
            currentTimeNanos);
      }
    } else {
      super.channelRead(ctx, msg);
    }
  }

  public long getCurrentTimeNanos() {
    return Time.nanoTime();
  }

  /*package-private for test only*/ long getLastPingSendTime() {
    return _lastPingSendTime;
  }

  /*package-private for test only*/ long getLastPingAckTime() {
    return _lastPingAckTime;
  }

  /*package-private for test only*/ long getLastPingId() {
    return _lastPingId;
  }

  /*package-private for test only*/ Channel getLastPingChannel() {
    return _lastPingChannel;
  }
}
