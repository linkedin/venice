package com.linkedin.alpini.netty4.handlers;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.logging.LogLevel;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestHttp2SettingsFrameLogger {
  private Logger _mockLog = mock(Logger.class);

  private Http2SettingsFrameLogger constructWithMockLoggers() {
    Http2SettingsFrameLogger handler = new Http2SettingsFrameLogger(LogLevel.INFO);
    handler._frameLogger = _mockLog;
    return handler;
  }

  @BeforeMethod(groups = "unit")
  public void beforeMethod() {
    reset(_mockLog);
  }

  @Test(groups = "unit")
  public void testRSTFrameLogsWithError() {
    Http2SettingsFrameLogger handler = constructWithMockLoggers();
    handler.logRstStream(
        Http2FrameLogger.Direction.INBOUND,
        mock(ChannelHandlerContext.class),
        3,
        Http2Error.INTERNAL_ERROR.code());
    verify(_mockLog, times(1)).error(anyString(), any(), any(), any(), any(), any());
    Assert.assertEquals(1L, handler.getErrorCount());
  }

  @Test(groups = "unit")
  public void testRSTFrameLogsWithNoError() {
    Http2SettingsFrameLogger handler = constructWithMockLoggers();
    handler.logRstStream(
        Http2FrameLogger.Direction.INBOUND,
        mock(ChannelHandlerContext.class),
        3,
        Http2Error.NO_ERROR.code());
    verify(_mockLog, never()).error(anyString(), any(), any(), any(), any(), any());
    Assert.assertEquals(0L, handler.getErrorCount());
  }

  @Test(groups = "unit")
  public void testRSTFrameLogsWithUnknownErrorCode() {
    Http2SettingsFrameLogger handler = constructWithMockLoggers();
    handler.logRstStream(Http2FrameLogger.Direction.INBOUND, mock(ChannelHandlerContext.class), 3, 999999);
    verify(_mockLog, times(1)).error(anyString(), any(), any(), any(), any(), any());
    Assert.assertEquals(1L, handler.getErrorCount());
  }

  @Test(groups = "unit")
  public void shouldLogRSTFrameWithCancelFromRemote() {
    Http2SettingsFrameLogger handler = constructWithMockLoggers();
    handler.setLogInboundRst(true);
    handler.logRstStream(
        Http2FrameLogger.Direction.INBOUND,
        mock(ChannelHandlerContext.class),
        3,
        Http2Error.CANCEL.code());
    verify(_mockLog, times(1)).error(anyString(), any(), any(), any(), any(), any());
    Assert.assertEquals(1L, handler.getErrorCount());
  }

  @Test(groups = "unit")
  public void shouldNotLogRSTFrameWithCancelFromRemote() {
    Http2SettingsFrameLogger handler = constructWithMockLoggers();
    handler.logRstStream(
        Http2FrameLogger.Direction.INBOUND,
        mock(ChannelHandlerContext.class),
        3,
        Http2Error.CANCEL.code());
    verify(_mockLog, never()).error(anyString(), any(), any(), any(), any(), any());
    Assert.assertEquals(0L, handler.getErrorCount(), "Should not count when logInboundRst is false");
  }

  @Test(groups = "unit")
  public void testGoAwayFrameLogsWithError() {
    Http2SettingsFrameLogger handler = constructWithMockLoggers();
    handler.logGoAway(
        Http2FrameLogger.Direction.INBOUND,
        mock(ChannelHandlerContext.class),
        3,
        Http2Error.INTERNAL_ERROR.code(),
        null);
    verify(_mockLog, times(1)).error(anyString(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test(groups = "unit")
  public void testGoAwayFrameLogsWithErrorNonNullDebugData() {
    Http2SettingsFrameLogger handler = constructWithMockLoggers();
    handler.logGoAway(
        Http2FrameLogger.Direction.INBOUND,
        mock(ChannelHandlerContext.class),
        3,
        Http2Error.INTERNAL_ERROR.code(),
        ByteBufUtil.writeUtf8(UnpooledByteBufAllocator.DEFAULT, "ERROR"));
    verify(_mockLog, times(1)).error(anyString(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test(groups = "unit")
  public void testGoAwayFrameLogsWithNoError() {
    Http2SettingsFrameLogger handler = constructWithMockLoggers();
    handler.logGoAway(
        Http2FrameLogger.Direction.INBOUND,
        mock(ChannelHandlerContext.class),
        3,
        Http2Error.NO_ERROR.code(),
        null);
    verify(_mockLog, never()).error(anyString(), any(), any(), any(), any(), any(), any(), any());
    Assert.assertEquals(0L, handler.getErrorCount());
  }

}
