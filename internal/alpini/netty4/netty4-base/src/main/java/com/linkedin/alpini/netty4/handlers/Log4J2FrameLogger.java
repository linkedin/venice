package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.Msg;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2Flags;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.logging.LogLevel;
import io.netty.util.internal.StringUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by acurtis on 4/19/18.
 */
public class Log4J2FrameLogger extends Http2FrameLogger {
  private static final int BUFFER_LENGTH_THRESHOLD = 64;
  private final Logger _logger;
  private final Level _level;

  public Log4J2FrameLogger(Level level) {
    this(level, Http2FrameLogger.class);
  }

  public Log4J2FrameLogger(Level level, String name) {
    this(level, LogManager.getLogger(name));
  }

  public Log4J2FrameLogger(Level level, Class<?> clazz) {
    this(level, LogManager.getLogger(clazz));
  }

  private Log4J2FrameLogger(Level level, Logger logger) {
    super(LogLevel.ERROR);
    _logger = setupLogger(logger);
    _level = level;
  }

  Logger setupLogger(Logger logger) {
    return logger;
  }

  public void logData(
      Direction direction,
      ChannelHandlerContext ctx,
      int streamId,
      ByteBuf data,
      int padding,
      boolean endStream) {
    _logger.log(
        _level,
        "{} {} DATA: streamId={} padding={} endStream={} length={} bytes={}",
        ctx.channel(),
        direction.name(),
        streamId,
        padding,
        endStream,
        data.readableBytes(),
        toMsg(data));
  }

  public void logHeaders(
      Direction direction,
      ChannelHandlerContext ctx,
      int streamId,
      Http2Headers headers,
      int padding,
      boolean endStream) {
    _logger.log(
        _level,
        "{} {} HEADERS: streamId={} headers={} padding={} endStream={}",
        ctx.channel(),
        direction.name(),
        streamId,
        headers,
        padding,
        endStream);
  }

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
    _logger.log(
        _level,
        "{} {} HEADERS: streamId={} headers={} streamDependency={} weight={} exclusive={} " + "padding={} endStream={}",
        ctx.channel(),
        direction.name(),
        streamId,
        headers,
        streamDependency,
        weight,
        exclusive,
        padding,
        endStream);
  }

  public void logPriority(
      Direction direction,
      ChannelHandlerContext ctx,
      int streamId,
      int streamDependency,
      short weight,
      boolean exclusive) {
    _logger.log(
        _level,
        "{} {} PRIORITY: streamId={} streamDependency={} weight={} exclusive={}",
        ctx.channel(),
        direction.name(),
        streamId,
        streamDependency,
        weight,
        exclusive);
  }

  public void logRstStream(Direction direction, ChannelHandlerContext ctx, int streamId, long errorCode) {
    _logger.log(
        _level,
        "{} {} RST_STREAM: streamId={} errorCode={}",
        ctx.channel(),
        direction.name(),
        streamId,
        errorCode);
  }

  public void logSettingsAck(Direction direction, ChannelHandlerContext ctx) {
    _logger.log(_level, "{} {} SETTINGS: ack=true", ctx.channel(), direction.name());
  }

  public void logSettings(Direction direction, ChannelHandlerContext ctx, Http2Settings settings) {
    _logger.log(_level, "{} {} SETTINGS: ack=false settings={}", ctx.channel(), direction.name(), settings);
  }

  public void logPing(Direction direction, ChannelHandlerContext ctx, long data) {
    _logger.log(_level, "{} {} PING: ack=false bytes={}", ctx.channel(), direction.name(), data);
  }

  public void logPingAck(Direction direction, ChannelHandlerContext ctx, long data) {
    _logger.log(_level, "{} {} PING: ack=true bytes={}", ctx.channel(), direction.name(), data);
  }

  public void logPushPromise(
      Direction direction,
      ChannelHandlerContext ctx,
      int streamId,
      int promisedStreamId,
      Http2Headers headers,
      int padding) {
    _logger.log(
        _level,
        "{} {} PUSH_PROMISE: streamId={} promisedStreamId={} headers={} padding={}",
        ctx.channel(),
        direction.name(),
        streamId,
        promisedStreamId,
        headers,
        padding);
  }

  public void logGoAway(
      Direction direction,
      ChannelHandlerContext ctx,
      int lastStreamId,
      long errorCode,
      ByteBuf debugData) {
    _logger.log(
        _level,
        "{} {} GO_AWAY: lastStreamId={} errorCode={} length={} bytes={}",
        ctx.channel(),
        direction.name(),
        lastStreamId,
        errorCode,
        debugData.readableBytes(),
        toMsg(debugData));
  }

  public void logWindowsUpdate(Direction direction, ChannelHandlerContext ctx, int streamId, int windowSizeIncrement) {
    _logger.log(
        _level,
        "{} {} WINDOW_UPDATE: streamId={} windowSizeIncrement={}",
        ctx.channel(),
        direction.name(),
        streamId,
        windowSizeIncrement);
  }

  public void logUnknownFrame(
      Direction direction,
      ChannelHandlerContext ctx,
      byte frameType,
      int streamId,
      Http2Flags flags,
      ByteBuf data) {
    _logger.log(
        _level,
        "{} {} UNKNOWN: frameType={} streamId={} flags={} length={} bytes={}",
        ctx.channel(),
        direction.name(),
        frameType & 0xFF,
        streamId,
        flags.value(),
        data.readableBytes(),
        toMsg(data));
  }

  private Object toMsg(ByteBuf buf) {
    if (!_logger.isEnabled(_level)) {
      return StringUtil.EMPTY_STRING;
    }

    if (_level == Level.TRACE || buf.readableBytes() <= BUFFER_LENGTH_THRESHOLD) {
      // Log the entire buffer.
      return Msg.make(buf, ByteBufUtil::hexDump);
    }

    // Otherwise just log the first 64 bytes.
    return Msg.make(buf, Log4J2FrameLogger::hexDumpInitial);
  }

  private static String hexDumpInitial(ByteBuf buf) {
    int length = Math.min(buf.readableBytes(), BUFFER_LENGTH_THRESHOLD);
    return ByteBufUtil.hexDump(buf, buf.readerIndex(), length) + "...";
  }
}
