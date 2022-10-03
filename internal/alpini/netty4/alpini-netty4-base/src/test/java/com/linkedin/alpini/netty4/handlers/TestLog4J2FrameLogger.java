package com.linkedin.alpini.netty4.handlers;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2Flags;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import java.nio.charset.StandardCharsets;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 4/23/18.
 */
public class TestLog4J2FrameLogger {
  private final Logger _mockLogger = Mockito.mock(Logger.class);

  @Test(groups = "unit")
  public void testConstruct() throws Exception {
    Assert.assertNotNull(new Log4J2FrameLogger(Level.DEBUG));
  }

  @Test(groups = "unit")
  public void testLogData() {
    Mockito.reset(_mockLogger);
    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    class Harness extends Log4J2FrameLogger {
      private Harness() {
        super(Level.DEBUG);
      }

      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Log4J2FrameLogger handler = new Harness();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    ByteBuf data = Unpooled.copiedBuffer("Hello world", StandardCharsets.US_ASCII);

    handler.logData(Http2FrameLogger.Direction.INBOUND, ctx, 1, data, 42, true);

    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    Mockito.verify(_mockLogger)
        .log(
            Mockito.eq(Level.DEBUG),
            Mockito.eq("{} {} DATA: streamId={} padding={} endStream={} length={} bytes={}"),
            Mockito.same(ch),
            Mockito.eq("INBOUND"),
            Mockito.eq(1),
            Mockito.eq(42),
            Mockito.eq(true),
            Mockito.eq(11),
            message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "48656c6c6f20776f726c64");
    Mockito.verify(ctx).channel();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testLogHeaders1() {
    Mockito.reset(_mockLogger);
    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    class Harness extends Log4J2FrameLogger {
      private Harness() {
        super(Level.DEBUG);
      }

      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Log4J2FrameLogger handler = new Harness();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    Http2Headers data = Mockito.mock(Http2Headers.class, "headers");

    handler.logHeaders(Http2FrameLogger.Direction.INBOUND, ctx, 1, data, 42, true);

    Mockito.verify(_mockLogger)
        .log(
            Mockito.eq(Level.DEBUG),
            Mockito.eq("{} {} HEADERS: streamId={} headers={} padding={} endStream={}"),
            Mockito.same(ch),
            Mockito.eq("INBOUND"),
            Mockito.eq(1),
            Mockito.same(data),
            Mockito.eq(42),
            Mockito.eq(true));
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Mockito.verify(ctx).channel();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testLogHeaders2() {
    Mockito.reset(_mockLogger);
    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    class Harness extends Log4J2FrameLogger {
      private Harness() {
        super(Level.DEBUG);
      }

      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Log4J2FrameLogger handler = new Harness();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    Http2Headers data = Mockito.mock(Http2Headers.class, "headers");

    handler.logHeaders(Http2FrameLogger.Direction.INBOUND, ctx, 5, data, 1, (short) 100, false, 42, true);

    Mockito.verify(_mockLogger)
        .log(
            Mockito.eq(Level.DEBUG),
            Mockito.eq(
                "{} {} HEADERS: streamId={} headers={} streamDependency={} weight={} exclusive={} padding={} endStream={}"),
            Mockito.same(ch),
            Mockito.eq("INBOUND"),
            Mockito.eq(5),
            Mockito.same(data),
            Mockito.eq(1),
            Mockito.eq((short) 100),
            Mockito.eq(false),
            Mockito.eq(42),
            Mockito.eq(true));
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Mockito.verify(ctx).channel();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testLogPriority() {
    Mockito.reset(_mockLogger);
    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    class Harness extends Log4J2FrameLogger {
      private Harness() {
        super(Level.DEBUG);
      }

      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Log4J2FrameLogger handler = new Harness();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.when(ctx.channel()).thenReturn(ch);

    handler.logPriority(Http2FrameLogger.Direction.INBOUND, ctx, 5, 0, (short) 100, false);

    Mockito.verify(_mockLogger)
        .log(
            Mockito.eq(Level.DEBUG),
            Mockito.eq("{} {} PRIORITY: streamId={} streamDependency={} weight={} exclusive={}"),
            Mockito.same(ch),
            Mockito.eq("INBOUND"),
            Mockito.eq(5),
            Mockito.eq(0),
            Mockito.eq((short) 100),
            Mockito.eq(false));
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Mockito.verify(ctx).channel();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testLogRstStream() {
    Mockito.reset(_mockLogger);
    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    class Harness extends Log4J2FrameLogger {
      private Harness() {
        super(Level.DEBUG);
      }

      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Log4J2FrameLogger handler = new Harness();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.when(ctx.channel()).thenReturn(ch);

    handler.logRstStream(Http2FrameLogger.Direction.INBOUND, ctx, 5, 0);

    Mockito.verify(_mockLogger)
        .log(
            Mockito.eq(Level.DEBUG),
            Mockito.eq("{} {} RST_STREAM: streamId={} errorCode={}"),
            Mockito.same(ch),
            Mockito.eq("INBOUND"),
            Mockito.eq(5),
            Mockito.eq(0L));
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Mockito.verify(ctx).channel();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testLogSettingsAck() {
    Mockito.reset(_mockLogger);
    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    class Harness extends Log4J2FrameLogger {
      private Harness() {
        super(Level.DEBUG);
      }

      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Log4J2FrameLogger handler = new Harness();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.when(ctx.channel()).thenReturn(ch);

    handler.logSettingsAck(Http2FrameLogger.Direction.INBOUND, ctx);

    Mockito.verify(_mockLogger)
        .log(Mockito.eq(Level.DEBUG), Mockito.eq("{} {} SETTINGS: ack=true"), Mockito.same(ch), Mockito.eq("INBOUND"));
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Mockito.verify(ctx).channel();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testLogSettings() {
    Mockito.reset(_mockLogger);
    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    class Harness extends Log4J2FrameLogger {
      private Harness() {
        super(Level.DEBUG);
      }

      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Log4J2FrameLogger handler = new Harness();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    Http2Settings settings = new Http2Settings();

    handler.logSettings(Http2FrameLogger.Direction.INBOUND, ctx, settings);

    Mockito.verify(_mockLogger)
        .log(
            Mockito.eq(Level.DEBUG),
            Mockito.eq("{} {} SETTINGS: ack=false settings={}"),
            Mockito.same(ch),
            Mockito.eq("INBOUND"),
            Mockito.same(settings));
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Mockito.verify(ctx).channel();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testLogPing() {
    Mockito.reset(_mockLogger);
    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    class Harness extends Log4J2FrameLogger {
      private Harness() {
        super(Level.DEBUG);
      }

      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Log4J2FrameLogger handler = new Harness();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    long data = 0xdeadbeefL;

    handler.logPing(Http2FrameLogger.Direction.INBOUND, ctx, data);

    Mockito.verify(_mockLogger)
        .log(
            Mockito.eq(Level.DEBUG),
            Mockito.eq("{} {} PING: ack=false bytes={}"),
            Mockito.same(ch),
            Mockito.eq("INBOUND"),
            Mockito.eq(data));
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Mockito.verify(ctx).channel();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testLogPingAck() {
    Mockito.reset(_mockLogger);
    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    class Harness extends Log4J2FrameLogger {
      private Harness() {
        super(Level.DEBUG);
      }

      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Log4J2FrameLogger handler = new Harness();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    long data = 0xdeadbeefL;

    handler.logPingAck(Http2FrameLogger.Direction.INBOUND, ctx, data);

    Mockito.verify(_mockLogger)
        .log(
            Mockito.eq(Level.DEBUG),
            Mockito.eq("{} {} PING: ack=true bytes={}"),
            Mockito.same(ch),
            Mockito.eq("INBOUND"),
            Mockito.eq(data));
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Mockito.verify(ctx).channel();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testLogPushPromise() {
    Mockito.reset(_mockLogger);
    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    class Harness extends Log4J2FrameLogger {
      private Harness() {
        super(Level.DEBUG);
      }

      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Log4J2FrameLogger handler = new Harness();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    Http2Headers headers = Mockito.mock(Http2Headers.class, "headers");

    handler.logPushPromise(Http2FrameLogger.Direction.INBOUND, ctx, 5, 8, headers, 42);

    Mockito.verify(_mockLogger)
        .log(
            Mockito.eq(Level.DEBUG),
            Mockito.eq("{} {} PUSH_PROMISE: streamId={} promisedStreamId={} headers={} padding={}"),
            Mockito.same(ch),
            Mockito.eq("INBOUND"),
            Mockito.eq(5),
            Mockito.eq(8),
            Mockito.same(headers),
            Mockito.eq(42));
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Mockito.verify(ctx).channel();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testLogGoAway() {
    Mockito.reset(_mockLogger);
    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    class Harness extends Log4J2FrameLogger {
      private Harness() {
        super(Level.DEBUG);
      }

      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Log4J2FrameLogger handler = new Harness();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    ByteBuf debug = Unpooled.copiedBuffer("Hello world", StandardCharsets.US_ASCII);

    handler.logGoAway(Http2FrameLogger.Direction.INBOUND, ctx, 5, 0, debug);
    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    Mockito.verify(_mockLogger)
        .log(
            Mockito.eq(Level.DEBUG),
            Mockito.eq("{} {} GO_AWAY: lastStreamId={} errorCode={} length={} bytes={}"),
            Mockito.same(ch),
            Mockito.eq("INBOUND"),
            Mockito.eq(5),
            Mockito.eq(0L),
            Mockito.eq(11),
            captor.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(captor.getValue().toString(), "48656c6c6f20776f726c64");

    Mockito.verify(ctx).channel();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testLogWindowUpdate() {
    Mockito.reset(_mockLogger);
    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    class Harness extends Log4J2FrameLogger {
      private Harness() {
        super(Level.DEBUG);
      }

      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Log4J2FrameLogger handler = new Harness();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    Http2Headers headers = Mockito.mock(Http2Headers.class, "headers");

    handler.logWindowsUpdate(Http2FrameLogger.Direction.INBOUND, ctx, 5, 10000);

    Mockito.verify(_mockLogger)
        .log(
            Mockito.eq(Level.DEBUG),
            Mockito.eq("{} {} WINDOW_UPDATE: streamId={} windowSizeIncrement={}"),
            Mockito.same(ch),
            Mockito.eq("INBOUND"),
            Mockito.eq(5),
            Mockito.eq(10000));
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Mockito.verify(ctx).channel();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testLogUnknownFrame() {
    Mockito.reset(_mockLogger);
    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    class Harness extends Log4J2FrameLogger {
      private Harness() {
        super(Level.DEBUG, "foo");
      }

      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Log4J2FrameLogger handler = new Harness();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    ByteBuf debug = Unpooled.copiedBuffer(
        "Hello world! " + "This is a longer string which will exceed 64 bytes so it will be truncated",
        StandardCharsets.US_ASCII);
    Http2Flags flags = new Http2Flags((short) 1234);

    handler.logUnknownFrame(Http2FrameLogger.Direction.INBOUND, ctx, (byte) 100, 5, flags, debug);
    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    Mockito.verify(_mockLogger)
        .log(
            Mockito.eq(Level.DEBUG),
            Mockito.eq("{} {} UNKNOWN: frameType={} streamId={} flags={} length={} bytes={}"),
            Mockito.same(ch),
            Mockito.eq("INBOUND"),
            Mockito.eq(100),
            Mockito.eq(5),
            Mockito.eq((short) 1234),
            Mockito.eq(87),
            captor.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(
        captor.getValue().toString(),
        "48656c6c6f20776f726c642120546869732069732061206c6f6e67657220737472696e672077686963682077696c6c2065786365656420363420627974657320...");

    Mockito.verify(ctx).channel();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testLogUnknownFrameNoDebug() {
    Mockito.reset(_mockLogger);
    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(false);

    class Harness extends Log4J2FrameLogger {
      private Harness() {
        super(Level.DEBUG, "foo");
      }

      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Log4J2FrameLogger handler = new Harness();
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    ByteBuf debug = Unpooled.copiedBuffer(
        "Hello world! " + "This is a longer string which will exceed 64 bytes so it will be truncated",
        StandardCharsets.US_ASCII);
    Http2Flags flags = new Http2Flags((short) 1234);

    handler.logUnknownFrame(Http2FrameLogger.Direction.INBOUND, ctx, (byte) 100, 5, flags, debug);
    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    Mockito.verify(_mockLogger)
        .log(
            Mockito.eq(Level.DEBUG),
            Mockito.eq("{} {} UNKNOWN: frameType={} streamId={} flags={} length={} bytes={}"),
            Mockito.same(ch),
            Mockito.eq("INBOUND"),
            Mockito.eq(100),
            Mockito.eq(5),
            Mockito.eq((short) 1234),
            Mockito.eq(87),
            captor.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(captor.getValue().toString(), "");

    Mockito.verify(ctx).channel();
    Mockito.verifyNoMoreInteractions(ctx);
  }
}
