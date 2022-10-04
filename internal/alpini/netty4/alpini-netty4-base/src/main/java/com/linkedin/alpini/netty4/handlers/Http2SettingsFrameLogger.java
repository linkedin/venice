package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.netty4.misc.Http2Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Flags;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.logging.LogLevel;
import java.util.concurrent.atomic.LongAdder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Logs the HTTP/2 Settings Frames.
 *  1) This handler logs the settings that are exchanged during the connection creation.
 *  2) RST frames - If it contains an error code other than NO_ERROR, CANCEL
 *  3) GO_AWAY frames - If it contains an error code other than NO_ERROR, CANCEL
 *
 * @author Abhishek Andhavarapu
 */
@ChannelHandler.Sharable
public class Http2SettingsFrameLogger extends Http2FrameLogger {
  private static final Logger LOG = LogManager.getLogger(Http2SettingsFrameLogger.class);
  private static final int BUFFER_LENGTH_THRESHOLD = 64;
  private final LongAdder _errorCount = new LongAdder();
  private boolean _logInboundRst = false;

  // For unit tests
  Logger _frameLogger;

  public Http2SettingsFrameLogger(LogLevel level) {
    super(level);
    _frameLogger = LOG;
  }

  public Http2SettingsFrameLogger(LogLevel level, String name) {
    super(level, name);
    _frameLogger = LOG;
  }

  public Http2SettingsFrameLogger(LogLevel level, Class<?> clazz) {
    super(level, clazz);
    _frameLogger = LOG;
  }

  // For unit tests.
  Logger getLogger() {
    return _frameLogger;
  }

  public long getErrorCount() {
    return _errorCount.longValue();
  }

  public void setLogInboundRst(boolean logInboundRst) {
    _logInboundRst = logInboundRst;
  }

  @Override
  public void logData(
      Direction direction,
      ChannelHandlerContext ctx,
      int streamId,
      ByteBuf data,
      int padding,
      boolean endStream) {
    // super.logData(direction, ctx, streamId, data, padding, endStream);
  }

  @Override
  public void logHeaders(
      Direction direction,
      ChannelHandlerContext ctx,
      int streamId,
      Http2Headers headers,
      int padding,
      boolean endStream) {
    // super.logHeaders(direction, ctx, streamId, headers, padding, endStream);
  }

  @Override
  public void logHeaders(
      Direction direction,
      ChannelHandlerContext ctx,
      int streamId,
      Http2Headers headers,
      int streamDependency,
      short weight,
      boolean exclusive,
      int padding,
      boolean endStream) {
    // super.logHeaders(direction, ctx, streamId, headers, streamDependency, weight, exclusive, padding, endStream);
  }

  @Override
  public void logPriority(
      Direction direction,
      ChannelHandlerContext ctx,
      int streamId,
      int streamDependency,
      short weight,
      boolean exclusive) {
    // super.logPriority(direction, ctx, streamId, streamDependency, weight, exclusive);
  }

  @Override
  public void logRstStream(Direction direction, ChannelHandlerContext ctx, int streamId, long errorCode) {
    // Log RST Frame if there is an error. RST frame is used to terminate a stream.
    Http2Error errorEnum = Http2Error.valueOf(errorCode);
    // If it is a known error or UNKNOWN error code.
    if (Http2Utils.isUnexpectedError(errorEnum, _logInboundRst && direction.equals(Direction.INBOUND))) {
      _errorCount.increment();
      getLogger().error(
          "{} {} RST_STREAM: streamId={} error={}({})",
          ctx.channel(),
          direction.name(),
          streamId,
          errorEnum,
          errorCode);
    }
  }

  @Override
  public void logPing(Direction direction, ChannelHandlerContext ctx, long data) {
    // super.logPing(direction, ctx, data);
  }

  @Override
  public void logPingAck(Direction direction, ChannelHandlerContext ctx, long data) {
    // super.logPingAck(direction, ctx, data);
  }

  @Override
  public void logPushPromise(
      Direction direction,
      ChannelHandlerContext ctx,
      int streamId,
      int promisedStreamId,
      Http2Headers headers,
      int padding) {
    // super.logPushPromise(direction, ctx, streamId, promisedStreamId, headers, padding);
  }

  @Override
  public void logGoAway(
      Direction direction,
      ChannelHandlerContext ctx,
      int lastStreamId,
      long errorCode,
      ByteBuf debugData) {
    // Log GO_AWAY Frame if there is an error. GO_AWAY frame is used to terminate a connection.
    Http2Error errorEnum = Http2Error.valueOf(errorCode);
    // If it is an known error or UNKNOWN error code.
    if (Http2Utils.isUnexpectedError(errorEnum)) {
      getLogger().error(
          "{} {} GO_AWAY: lastStreamId={} errorCode={}({}) length={} bytes={}",
          ctx.channel(),
          direction.name(),
          lastStreamId,
          errorEnum,
          errorCode,
          debugData != null ? debugData.readableBytes() : 0,
          debugData != null ? toString(debugData) : "NULL");
    }
  }

  @Override
  public void logWindowsUpdate(Direction direction, ChannelHandlerContext ctx, int streamId, int windowSizeIncrement) {
    // super.logWindowsUpdate(direction, ctx, streamId, windowSizeIncrement);
  }

  @Override
  public void logUnknownFrame(
      Direction direction,
      ChannelHandlerContext ctx,
      byte frameType,
      int streamId,
      Http2Flags flags,
      ByteBuf data) {
    // super.logUnknownFrame(direction, ctx, frameType, streamId, flags, data);
  }

  private String toString(ByteBuf buf) {
    if (LOG.isTraceEnabled() || buf.readableBytes() <= BUFFER_LENGTH_THRESHOLD) {
      // Log the entire buffer.
      return ByteBufUtil.prettyHexDump(buf);
    }

    // Otherwise just log the first 64 bytes.
    int length = Math.min(buf.readableBytes(), BUFFER_LENGTH_THRESHOLD);
    return ByteBufUtil.prettyHexDump(buf, buf.readerIndex(), length) + "...";
  }
}
