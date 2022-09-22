package io.netty.handler.codec.http2;

import static io.netty.handler.codec.http2.Http2TestUtil.anyChannelPromise;
import static io.netty.handler.codec.http2.Http2TestUtil.anyHttp2Settings;
import static io.netty.handler.codec.http2.Http2TestUtil.assertEqualsAndRelease;
import static io.netty.handler.codec.http2.Http2TestUtil.bb;
import static io.netty.util.ReferenceCountUtil.release;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyShort;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http2.Http2Exception.StreamException;
import io.netty.handler.codec.http2.LastInboundHandler.Consumer;
import io.netty.util.AsciiString;
import io.netty.util.AttributeKey;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 *  Forked from Netty's Http2MultiplexTest
 */
public abstract class TestHttp2Multiplex<C extends Http2FrameCodec> {
  private final Http2Headers request = new DefaultHttp2Headers().method(HttpMethod.GET.asciiName())
      .scheme(HttpScheme.HTTPS.name())
      .authority(new AsciiString("example.org"))
      .path(new AsciiString("/foo"));

  private EmbeddedChannel parentChannel;
  private Http2FrameWriter frameWriter;
  private Http2FrameInboundWriter frameInboundWriter;
  private TestChannelInitializer childChannelInitializer;
  private EspressoHttp2MultiplexHandler multiplexHandler;
  private C codec;

  private static final int initialRemoteStreamWindow = 1024;

  protected abstract C newCodec(TestChannelInitializer childChannelInitializer, Http2FrameWriter frameWriter);

  protected abstract EspressoHttp2MultiplexHandler newMultiplexer(TestChannelInitializer childChannelInitializer);

  @BeforeMethod(groups = "unit")
  public void setUp() {
    childChannelInitializer = new TestChannelInitializer();
    parentChannel = new EmbeddedChannel();
    frameInboundWriter = new Http2FrameInboundWriter(parentChannel);
    parentChannel.connect(new InetSocketAddress(0));
    frameWriter = Http2TestUtil.mockedFrameWriter();
    codec = newCodec(childChannelInitializer, frameWriter);
    parentChannel.pipeline().addLast(codec);
    EspressoHttp2MultiplexHandler multiplexer = newMultiplexer(childChannelInitializer);
    if (multiplexer != null) {
      this.multiplexHandler = multiplexer;
      parentChannel.pipeline().addLast(multiplexer);
    }

    parentChannel.runPendingTasks();
    parentChannel.pipeline().fireChannelActive();

    parentChannel.writeInbound(Http2CodecUtil.connectionPrefaceBuf());

    Http2Settings settings = new Http2Settings().initialWindowSize(initialRemoteStreamWindow);
    frameInboundWriter.writeInboundSettings(settings);

    verify(frameWriter).writeSettingsAck(eqCodecCtx(), anyChannelPromise());

    frameInboundWriter.writeInboundSettingsAck();

    Http2SettingsFrame settingsFrame = parentChannel.readInbound();
    assertNotNull(settingsFrame);
    Http2SettingsAckFrame settingsAckFrame = parentChannel.readInbound();
    assertNotNull(settingsAckFrame);

    // Handshake
    verify(frameWriter).writeSettings(eqCodecCtx(), anyHttp2Settings(), anyChannelPromise());
  }

  private ChannelHandlerContext eqCodecCtx() {
    return eq(codec.ctx);
  }

  @AfterMethod(groups = "unit")
  public void tearDown() throws Exception {
    if (childChannelInitializer.handler instanceof LastInboundHandler) {
      ((LastInboundHandler) childChannelInitializer.handler).finishAndReleaseAll();
    }
    parentChannel.finishAndReleaseAll();
    codec = null;
  }

  // TODO(buchgr): Flush from child channel
  // TODO(buchgr): ChildChannel.childReadComplete()
  // TODO(buchgr): GOAWAY Logic
  // TODO(buchgr): Test ChannelConfig.setMaxMessagesPerRead

  @Test(groups = "unit")
  public void writeUnknownFrame() {
    Http2StreamChannel childChannel = newOutboundStream(new ChannelInboundHandlerAdapter() {
      @Override
      public void channelActive(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers()));
        ctx.writeAndFlush(new DefaultHttp2UnknownFrame((byte) 99, new Http2Flags()));
        ctx.fireChannelActive();
      }
    });
    assertTrue(childChannel.isActive());

    parentChannel.runPendingTasks();

    verify(frameWriter).writeFrame(
        eq(codec.ctx),
        eq((byte) 99),
        eqStreamId(childChannel),
        any(Http2Flags.class),
        any(ByteBuf.class),
        any(ChannelPromise.class));
  }

  private Http2StreamChannel newInboundStream(int streamId, boolean endStream, final ChannelHandler childHandler) {
    return newInboundStream(streamId, endStream, null, childHandler);
  }

  private Http2StreamChannel newInboundStream(
      int streamId,
      boolean endStream,
      AtomicInteger maxReads,
      final ChannelHandler childHandler) {
    final AtomicReference<Http2StreamChannel> streamChannelRef = new AtomicReference<Http2StreamChannel>();
    childChannelInitializer.maxReads = maxReads;
    childChannelInitializer.handler = new ChannelInboundHandlerAdapter() {
      @Override
      public void channelRegistered(ChannelHandlerContext ctx) {
        assertNull(streamChannelRef.get());
        streamChannelRef.set((Http2StreamChannel) ctx.channel());
        ctx.pipeline().addLast(childHandler);
        ctx.fireChannelRegistered();
      }
    };

    frameInboundWriter.writeInboundHeaders(streamId, request, 0, endStream);
    parentChannel.runPendingTasks();
    Http2StreamChannel channel = streamChannelRef.get();
    assertEquals(streamId, channel.stream().id());
    return channel;
  }

  @Test(groups = "unit")
  public void readUnkownFrame() {
    LastInboundHandler handler = new LastInboundHandler();

    Http2StreamChannel channel = newInboundStream(3, true, handler);
    frameInboundWriter.writeInboundFrame((byte) 99, channel.stream().id(), new Http2Flags(), Unpooled.EMPTY_BUFFER);

    // header frame and unknown frame
    verifyFramesMultiplexedToCorrectChannel(channel, handler, 2);

    Channel childChannel = newOutboundStream(new ChannelInboundHandlerAdapter());
    assertTrue(childChannel.isActive());
    // TODO : Verify we can recyle child channels
    /*if(reuseChildChannels) {
      assertEquals(1, ((EspressoHttp2MultiplexHandler) multiplexer).getChildChannelPool().size());
    }*/
  }

  @Test(groups = "unit")
  public void headerAndDataFramesShouldBeDelivered() {
    LastInboundHandler inboundHandler = new LastInboundHandler();

    Http2StreamChannel channel = newInboundStream(3, false, inboundHandler);
    Http2HeadersFrame headersFrame = new DefaultHttp2HeadersFrame(request).stream(channel.stream());
    Http2DataFrame dataFrame1 = new DefaultHttp2DataFrame(bb("hello")).stream(channel.stream());
    Http2DataFrame dataFrame2 = new DefaultHttp2DataFrame(bb("world")).stream(channel.stream());

    assertTrue(inboundHandler.isChannelActive());
    frameInboundWriter.writeInboundData(channel.stream().id(), bb("hello"), 0, false);
    frameInboundWriter.writeInboundData(channel.stream().id(), bb("world"), 0, false);

    assertEquals(headersFrame, inboundHandler.readInbound());

    assertEqualsAndRelease(dataFrame1, inboundHandler.readInbound());
    assertEqualsAndRelease(dataFrame2, inboundHandler.readInbound());

    assertNull(inboundHandler.readInbound());
  }

  @Test(groups = "unit")
  public void framesShouldBeMultiplexed() {
    LastInboundHandler handler1 = new LastInboundHandler();
    Http2StreamChannel channel1 = newInboundStream(3, false, handler1);
    LastInboundHandler handler2 = new LastInboundHandler();
    Http2StreamChannel channel2 = newInboundStream(5, false, handler2);
    LastInboundHandler handler3 = new LastInboundHandler();
    Http2StreamChannel channel3 = newInboundStream(11, false, handler3);

    verifyFramesMultiplexedToCorrectChannel(channel1, handler1, 1);
    verifyFramesMultiplexedToCorrectChannel(channel2, handler2, 1);
    verifyFramesMultiplexedToCorrectChannel(channel3, handler3, 1);

    frameInboundWriter.writeInboundData(channel2.stream().id(), bb("hello"), 0, false);
    frameInboundWriter.writeInboundData(channel1.stream().id(), bb("foo"), 0, true);
    frameInboundWriter.writeInboundData(channel2.stream().id(), bb("world"), 0, true);
    frameInboundWriter.writeInboundData(channel3.stream().id(), bb("bar"), 0, true);

    verifyFramesMultiplexedToCorrectChannel(channel1, handler1, 1);
    verifyFramesMultiplexedToCorrectChannel(channel2, handler2, 2);
    verifyFramesMultiplexedToCorrectChannel(channel3, handler3, 1);
  }

  @Test(groups = "unit")
  public void inboundDataFrameShouldUpdateLocalFlowController() throws Http2Exception {
    Http2LocalFlowController flowController = Mockito.mock(Http2LocalFlowController.class);
    codec.connection().local().flowController(flowController);

    LastInboundHandler handler = new LastInboundHandler();
    final Http2StreamChannel channel = newInboundStream(3, false, handler);

    ByteBuf tenBytes = bb("0123456789");

    frameInboundWriter.writeInboundData(channel.stream().id(), tenBytes, 0, true);

    // Verify we marked the bytes as consumed
    verify(flowController).consumeBytes(argThat(new ArgumentMatcher<Http2Stream>() {
      @Override
      public boolean matches(Http2Stream http2Stream) {
        return http2Stream.id() == channel.stream().id();
      }
    }), eq(10));

    // headers and data frame
    verifyFramesMultiplexedToCorrectChannel(channel, handler, 2);
  }

  @Test(groups = "unit")
  public void unhandledHttp2FramesShouldBePropagated() {
    Http2PingFrame pingFrame = new DefaultHttp2PingFrame(0);
    frameInboundWriter.writeInboundPing(false, 0);
    assertEquals(parentChannel.readInbound(), pingFrame);

    DefaultHttp2GoAwayFrame goAwayFrame = new DefaultHttp2GoAwayFrame(1, parentChannel.alloc().buffer().writeLong(8));
    frameInboundWriter.writeInboundGoAway(0, goAwayFrame.errorCode(), goAwayFrame.content().retainedDuplicate());

    Http2GoAwayFrame frame = parentChannel.readInbound();
    assertEqualsAndRelease(frame, goAwayFrame);
  }

  @Test(groups = "unit")
  public void channelReadShouldRespectAutoRead() {
    LastInboundHandler inboundHandler = new LastInboundHandler();
    Http2StreamChannel childChannel = newInboundStream(3, false, inboundHandler);
    assertTrue(childChannel.config().isAutoRead());
    Http2HeadersFrame headersFrame = inboundHandler.readInbound();
    assertNotNull(headersFrame);

    childChannel.config().setAutoRead(false);

    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("hello world"), 0, false);
    Http2DataFrame dataFrame0 = inboundHandler.readInbound();
    assertNotNull(dataFrame0);
    release(dataFrame0);

    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("foo"), 0, false);
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("bar"), 0, false);

    assertNull(inboundHandler.readInbound());

    childChannel.config().setAutoRead(true);
    verifyFramesMultiplexedToCorrectChannel(childChannel, inboundHandler, 2);
  }

  @Test(groups = "unit")
  public void channelReadShouldRespectAutoReadAndNotProduceNPE() throws Exception {
    LastInboundHandler inboundHandler = new LastInboundHandler();
    Http2StreamChannel childChannel = newInboundStream(3, false, inboundHandler);
    Http2FrameStream stream = childChannel.stream();
    assertTrue(childChannel.config().isAutoRead());
    Http2HeadersFrame headersFrame = inboundHandler.readInbound();
    assertNotNull(headersFrame);

    childChannel.config().setAutoRead(false);
    childChannel.pipeline().addFirst(new ChannelInboundHandlerAdapter() {
      private int count;

      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ctx.fireChannelRead(msg);
        // Close channel after 2 reads so there is still something in the inboundBuffer when the close happens.
        if (++count == 2) {
          ctx.close();
        }
      }
    });
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("hello world"), 0, false);
    Http2DataFrame dataFrame0 = inboundHandler.readInbound();
    assertNotNull(dataFrame0);
    release(dataFrame0);

    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("foo"), 0, false);
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("bar"), 0, false);
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("bar"), 0, false);

    assertNull(inboundHandler.readInbound());

    childChannel.config().setAutoRead(true);
    verifyFramesMultiplexedToCorrectChannel(stream, inboundHandler, 3);
    inboundHandler.checkException();
  }

  @Test(groups = "unit")
  public void readInChannelReadWithoutAutoRead() {
    useReadWithoutAutoRead(false);
  }

  @Test(groups = "unit")
  public void readInChannelReadCompleteWithoutAutoRead() {
    useReadWithoutAutoRead(true);
  }

  private void useReadWithoutAutoRead(final boolean readComplete) {
    LastInboundHandler inboundHandler = new LastInboundHandler();
    Http2StreamChannel childChannel = newInboundStream(3, false, inboundHandler);
    assertTrue(childChannel.config().isAutoRead());
    childChannel.config().setAutoRead(false);
    assertFalse(childChannel.config().isAutoRead());

    Http2HeadersFrame headersFrame = inboundHandler.readInbound();
    assertNotNull(headersFrame);

    // Add a handler which will request reads.
    childChannel.pipeline().addFirst(new ChannelInboundHandlerAdapter() {
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ctx.fireChannelRead(msg);
        if (!readComplete) {
          ctx.read();
        }
      }

      @Override
      public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.fireChannelReadComplete();
        if (readComplete) {
          ctx.read();
        }
      }
    });

    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("hello world"), 0, false);
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("foo"), 0, false);
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("bar"), 0, false);
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("hello world"), 0, false);
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("foo"), 0, false);
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("bar"), 0, true);

    verifyFramesMultiplexedToCorrectChannel(childChannel, inboundHandler, 6);
  }

  private Http2StreamChannel newOutboundStream(ChannelHandler handler) {
    return new EspressoHttp2StreamChannelBootstrap(parentChannel).handler(handler)
        .open()
        .syncUninterruptibly()
        .getNow();
  }

  /**
   * A child channel for an HTTP/2 stream in IDLE state (that is no headers sent or received),
   * should not emit a RST_STREAM frame on close, as this is a connection error of type protocol error.
   */
  @Test(groups = "unit")
  public void idleOutboundStreamShouldNotWriteResetFrameOnClose() {
    LastInboundHandler handler = new LastInboundHandler();

    Channel childChannel = newOutboundStream(handler);
    assertTrue(childChannel.isActive());

    childChannel.close();
    parentChannel.runPendingTasks();

    assertFalse(childChannel.isOpen());
    assertFalse(childChannel.isActive());
    assertNull(parentChannel.readOutbound());
  }

  @Test(groups = "unit")
  public void outboundStreamShouldWriteResetFrameOnClose_headersSent() {
    ChannelHandler handler = new ChannelInboundHandlerAdapter() {
      @Override
      public void channelActive(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers()));
        ctx.fireChannelActive();
      }
    };

    Http2StreamChannel childChannel = newOutboundStream(handler);
    Http2FrameStream stream = childChannel.stream();
    assertTrue(childChannel.isActive());

    childChannel.close();
    verify(frameWriter)
        .writeRstStream(eqCodecCtx(), eqStreamId(stream), eq(Http2Error.CANCEL.code()), anyChannelPromise());
  }

  // TODO: Fix it after updating the Mockito version
  @Test(groups = "unit", enabled = false)
  public void outboundStreamShouldNotWriteResetFrameOnClose_IfStreamDidntExist() {
    when(
        frameWriter.writeHeaders(
            any(ChannelHandlerContext.class),
            anyInt(),
            any(Http2Headers.class),
            anyInt(),
            anyShort(),
            anyBoolean(),
            anyInt(),
            anyBoolean(),
            any(ChannelPromise.class))).thenAnswer(new Answer<ChannelFuture>() {
              private boolean headersWritten;

              @Override
              public ChannelFuture answer(InvocationOnMock invocationOnMock) {
                // We want to fail to write the first headers frame. This is what happens if the connection
                // refuses to allocate a new stream due to having received a GOAWAY.
                if (!headersWritten) {
                  headersWritten = true;
                  return ((ChannelPromise) invocationOnMock.getArguments()[8]).setFailure(new Exception("boom"));
                }
                return ((ChannelPromise) invocationOnMock.getArguments()[8]).setSuccess();
              }
            });

    final Http2FrameStream[] stream = new Http2FrameStream[1];

    Http2StreamChannel childChannel = newOutboundStream(new ChannelInboundHandlerAdapter() {
      @Override
      public void channelActive(ChannelHandlerContext ctx) {
        stream[0] = ((Http2StreamChannel) ctx.channel()).stream();
        ctx.writeAndFlush(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers()));
        ctx.fireChannelActive();
      }
    });

    assertFalse(childChannel.isActive());

    childChannel.close();
    parentChannel.runPendingTasks();
    // The channel was never active so we should not generate a RST frame.
    verify(frameWriter, never()).writeRstStream(eqCodecCtx(), eqStreamId(stream[0]), anyLong(), anyChannelPromise());

    assertTrue(parentChannel.outboundMessages().isEmpty());
  }

  @Test(groups = "unit")
  public void inboundRstStreamFireChannelInactive() {
    LastInboundHandler inboundHandler = new LastInboundHandler();
    Http2StreamChannel channel = newInboundStream(3, false, inboundHandler);
    Http2FrameStream stream = channel.stream();
    assertTrue(inboundHandler.isChannelActive());
    frameInboundWriter.writeInboundRstStream(channel.stream().id(), Http2Error.INTERNAL_ERROR.code());

    assertFalse(inboundHandler.isChannelActive());
    // Channels are not reused
    assertTrue(multiplexHandler.getChildChannelPool().isEmpty());

    // A RST_STREAM frame should NOT be emitted, as we received a RST_STREAM.
    verify(frameWriter, Mockito.never())
        .writeRstStream(eqCodecCtx(), eqStreamId(stream), anyLong(), anyChannelPromise());
  }

  @Test(groups = "unit")
  public void inboundRstStreamNonErrorShouldKeepChannelActive() {
    LastInboundHandler inboundHandler = new LastInboundHandler();
    Http2StreamChannel channel = newInboundStream(3, false, inboundHandler);
    Http2FrameStream stream = channel.stream();
    assertTrue(inboundHandler.isChannelActive());
    // There is no error for this RST_STREAM, so we should reuse the channel if possible
    frameInboundWriter.writeInboundRstStream(channel.stream().id(), Http2Error.NO_ERROR.code());

    if (multiplexHandler.reuseChildChannelsEnabled()) {
      assertTrue(inboundHandler.isChannelActive());
    } else {
      assertFalse(inboundHandler.isChannelActive());
    }

    // A RST_STREAM frame should NOT be emitted, as we received a RST_STREAM.
    verify(frameWriter, Mockito.never())
        .writeRstStream(eqCodecCtx(), eqStreamId(stream), anyLong(), anyChannelPromise());
  }

  @Test(groups = "unit", expectedExceptions = StreamException.class)
  public void streamExceptionTriggersChildChannelExceptionAndClose() throws Exception {
    LastInboundHandler inboundHandler = new LastInboundHandler();
    Http2StreamChannel channel = newInboundStream(3, false, inboundHandler);
    assertTrue(channel.isActive());
    StreamException cause = new StreamException(channel.stream().id(), Http2Error.PROTOCOL_ERROR, "baaam!");
    parentChannel.pipeline().fireExceptionCaught(cause);

    assertFalse(channel.isActive());
    inboundHandler.checkException();
  }

  // TODO: Fix it after updating the Mockito version
  @Test(groups = "unit", expectedExceptions = ClosedChannelException.class, enabled = false)
  public void streamClosedErrorTranslatedToClosedChannelExceptionOnWrites() throws Exception {
    LastInboundHandler inboundHandler = new LastInboundHandler();

    final Http2StreamChannel childChannel = newOutboundStream(inboundHandler);
    assertTrue(childChannel.isActive());

    Http2Headers headers = new DefaultHttp2Headers();
    when(
        frameWriter.writeHeaders(
            any(ChannelHandlerContext.class),
            anyInt(),
            eq(headers),
            anyInt(),
            anyShort(),
            anyBoolean(),
            anyInt(),
            anyBoolean(),
            any(ChannelPromise.class))).thenAnswer(new Answer<ChannelFuture>() {
              @Override
              public ChannelFuture answer(InvocationOnMock invocationOnMock) {
                return ((ChannelPromise) invocationOnMock.getArguments()[8]).setFailure(
                    new StreamException(childChannel.stream().id(), Http2Error.STREAM_CLOSED, "Stream Closed"));
              }
            });
    ChannelFuture future = childChannel.writeAndFlush(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers()));

    parentChannel.flush();

    assertFalse(childChannel.isActive());
    assertFalse(childChannel.isOpen());

    inboundHandler.checkException();

    future.syncUninterruptibly();
  }

  @Test(groups = "unit")
  public void creatingWritingReadingAndClosingOutboundStreamShouldWork() {
    LastInboundHandler inboundHandler = new LastInboundHandler();
    Http2StreamChannel childChannel = newOutboundStream(inboundHandler);
    Http2FrameStream stream = childChannel.stream();
    assertTrue(childChannel.isActive());
    assertTrue(inboundHandler.isChannelActive());

    // Write to the child channel
    Http2Headers headers = new DefaultHttp2Headers().scheme("https").method("GET").path("/foo.txt");
    childChannel.writeAndFlush(new DefaultHttp2HeadersFrame(headers));

    // Read from the child channel
    frameInboundWriter.writeInboundHeaders(childChannel.stream().id(), headers, 0, false);

    Http2HeadersFrame headersFrame = inboundHandler.readInbound();
    assertNotNull(headersFrame);
    // TODO : Fix
    // assertEquals(headers, headersFrame.headers());

    // Close the child channel.
    childChannel.close();

    parentChannel.runPendingTasks();
    // An active outbound stream should emit a RST_STREAM frame.
    verify(frameWriter).writeRstStream(eqCodecCtx(), eqStreamId(stream), anyLong(), anyChannelPromise());

    assertFalse(childChannel.isOpen());
    assertFalse(childChannel.isActive());
    assertFalse(inboundHandler.isChannelActive());
  }

  // Test failing the promise of the first headers frame of an outbound stream. In practice this error case would most
  // likely happen due to the max concurrent streams limit being hit or the channel running out of stream identifiers.
  //
  // TODO: Fix it after updating the Mockito version
  @Test(groups = "unit", expectedExceptions = Http2NoMoreStreamIdsException.class, enabled = false)
  public void failedOutboundStreamCreationThrowsAndClosesChannel() throws Exception {
    LastInboundHandler handler = new LastInboundHandler();
    Http2StreamChannel childChannel = newOutboundStream(handler);
    assertTrue(childChannel.isActive());

    Http2Headers headers = new DefaultHttp2Headers();
    when(
        frameWriter.writeHeaders(
            any(ChannelHandlerContext.class),
            anyInt(),
            eq(headers),
            anyInt(),
            anyShort(),
            anyBoolean(),
            anyInt(),
            anyBoolean(),
            any(ChannelPromise.class))).thenAnswer(new Answer<ChannelFuture>() {
              @Override
              public ChannelFuture answer(InvocationOnMock invocationOnMock) {
                return ((ChannelPromise) invocationOnMock.getArguments()[8])
                    .setFailure(new Http2NoMoreStreamIdsException());
              }
            });

    ChannelFuture future = childChannel.writeAndFlush(new DefaultHttp2HeadersFrame(headers));
    parentChannel.flush();

    assertFalse(childChannel.isActive());
    assertFalse(childChannel.isOpen());

    handler.checkException();

    future.syncUninterruptibly();
  }

  @Test(groups = "unit")
  public void channelClosedWhenCloseListenerCompletes() {
    LastInboundHandler inboundHandler = new LastInboundHandler();
    Http2StreamChannel childChannel = newInboundStream(3, false, inboundHandler);

    assertTrue(childChannel.isOpen());
    assertTrue(childChannel.isActive());

    final AtomicBoolean channelOpen = new AtomicBoolean(true);
    final AtomicBoolean channelActive = new AtomicBoolean(true);

    // Create a promise before actually doing the close, because otherwise we would be adding a listener to a future
    // that is already completed because we are using EmbeddedChannel which executes code in the JUnit thread.
    ChannelPromise p = childChannel.newPromise();
    p.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) {
        channelOpen.set(future.channel().isOpen());
        channelActive.set(future.channel().isActive());
      }
    });
    childChannel.close(p).syncUninterruptibly();

    assertFalse(channelOpen.get());
    assertFalse(channelActive.get());
    assertFalse(childChannel.isActive());
  }

  @Test(groups = "unit")
  public void channelClosedWhenChannelClosePromiseCompletes() {
    LastInboundHandler inboundHandler = new LastInboundHandler();
    Http2StreamChannel childChannel = newInboundStream(3, false, inboundHandler);

    assertTrue(childChannel.isOpen());
    assertTrue(childChannel.isActive());

    final AtomicBoolean channelOpen = new AtomicBoolean(true);
    final AtomicBoolean channelActive = new AtomicBoolean(true);

    childChannel.closeFuture().addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) {
        channelOpen.set(future.channel().isOpen());
        channelActive.set(future.channel().isActive());
      }
    });
    childChannel.close().syncUninterruptibly();

    assertFalse(channelOpen.get());
    assertFalse(channelActive.get());
    assertFalse(childChannel.isActive());
  }

  // TODO: Fix it after updating the Mockito version
  @Test(groups = "unit", enabled = false)
  public void channelClosedWhenWriteFutureFails() {
    final Queue<ChannelPromise> writePromises = new ArrayDeque<ChannelPromise>();

    LastInboundHandler inboundHandler = new LastInboundHandler();
    Http2StreamChannel childChannel = newInboundStream(3, false, inboundHandler);

    assertTrue(childChannel.isOpen());
    assertTrue(childChannel.isActive());

    final AtomicBoolean channelOpen = new AtomicBoolean(true);
    final AtomicBoolean channelActive = new AtomicBoolean(true);

    Http2Headers headers = new DefaultHttp2Headers();
    when(
        frameWriter.writeHeaders(
            any(ChannelHandlerContext.class),
            anyInt(),
            eq(headers),
            anyInt(),
            anyShort(),
            anyBoolean(),
            anyInt(),
            anyBoolean(),
            any(ChannelPromise.class))).thenAnswer(new Answer<ChannelFuture>() {
              @Override
              public ChannelFuture answer(InvocationOnMock invocationOnMock) {
                ChannelPromise promise = (ChannelPromise) invocationOnMock.getArguments()[8];
                writePromises.offer(promise);
                return promise;
              }
            });

    ChannelFuture f = childChannel.writeAndFlush(new DefaultHttp2HeadersFrame(headers));
    assertFalse(f.isDone());
    f.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        channelOpen.set(future.channel().isOpen());
        channelActive.set(future.channel().isActive());
      }
    });

    ChannelPromise first = writePromises.poll();
    first.setFailure(new ClosedChannelException());
    f.awaitUninterruptibly();

    assertFalse(channelOpen.get());
    assertFalse(channelActive.get());
    assertFalse(childChannel.isActive());
  }

  @Test(groups = "unit")
  public void channelClosedTwiceMarksPromiseAsSuccessful() {
    LastInboundHandler inboundHandler = new LastInboundHandler();
    Http2StreamChannel childChannel = newInboundStream(3, false, inboundHandler);

    assertTrue(childChannel.isOpen());
    assertTrue(childChannel.isActive());
    childChannel.close().syncUninterruptibly();
    childChannel.close().syncUninterruptibly();

    assertFalse(childChannel.isOpen());
    assertFalse(childChannel.isActive());
  }

  @Test(groups = "unit")
  public void settingChannelOptsAndAttrs() {
    AttributeKey<String> key = AttributeKey.newInstance(UUID.randomUUID().toString());

    Channel childChannel = newOutboundStream(new ChannelInboundHandlerAdapter());
    childChannel.config().setAutoRead(false).setWriteSpinCount(1000);
    childChannel.attr(key).set("bar");
    assertFalse(childChannel.config().isAutoRead());
    assertEquals(1000, childChannel.config().getWriteSpinCount());
    assertEquals("bar", childChannel.attr(key).get());
  }

  @Test(groups = "unit")
  public void outboundFlowControlWritability() {
    Http2StreamChannel childChannel = newOutboundStream(new ChannelInboundHandlerAdapter());
    assertTrue(childChannel.isActive());

    assertTrue(childChannel.isWritable());
    childChannel.writeAndFlush(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers()));
    parentChannel.flush();

    // Test for initial window size
    assertTrue(initialRemoteStreamWindow < childChannel.config().getWriteBufferHighWaterMark());

    assertTrue(childChannel.isWritable());
    childChannel.write(new DefaultHttp2DataFrame(Unpooled.buffer().writeZero(16 * 1024 * 1024)));
    assertEquals(0, childChannel.bytesBeforeUnwritable());
    assertFalse(childChannel.isWritable());
  }

  @Test(groups = "unit")
  public void writabilityOfParentIsRespected() {
    Http2StreamChannel childChannel = newOutboundStream(new ChannelInboundHandlerAdapter());
    childChannel.config().setWriteBufferWaterMark(new WriteBufferWaterMark(2048, 4096));
    parentChannel.config().setWriteBufferWaterMark(new WriteBufferWaterMark(256, 512));
    assertTrue(childChannel.isWritable());
    assertTrue(parentChannel.isActive());

    childChannel.writeAndFlush(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers()));
    parentChannel.flush();

    assertTrue(childChannel.isWritable());
    childChannel.write(new DefaultHttp2DataFrame(Unpooled.buffer().writeZero(256)));
    assertTrue(childChannel.isWritable());
    childChannel.writeAndFlush(new DefaultHttp2DataFrame(Unpooled.buffer().writeZero(512)));

    long bytesBeforeUnwritable = childChannel.bytesBeforeUnwritable();
    assertNotEquals(0, bytesBeforeUnwritable);
    // Add something to the ChannelOutboundBuffer of the parent to simulate queuing in the parents channel buffer
    // and verify that this only affect the writability of the parent channel while the child stays writable
    // until it used all of its credits.
    parentChannel.unsafe()
        .outboundBuffer()
        .addMessage(Unpooled.buffer().writeZero(800), 800, parentChannel.voidPromise());
    assertFalse(parentChannel.isWritable());

    assertTrue(childChannel.isWritable());
    assertEquals(4096, childChannel.bytesBeforeUnwritable());

    // Flush everything which simulate writing everything to the socket.
    parentChannel.flush();
    assertTrue(parentChannel.isWritable());
    assertTrue(childChannel.isWritable());
    assertEquals(bytesBeforeUnwritable, childChannel.bytesBeforeUnwritable());

    ChannelFuture future =
        childChannel.writeAndFlush(new DefaultHttp2DataFrame(Unpooled.buffer().writeZero((int) bytesBeforeUnwritable)));
    assertFalse(childChannel.isWritable());
    assertTrue(parentChannel.isWritable());

    parentChannel.flush();
    assertFalse(future.isDone());
    assertTrue(parentChannel.isWritable());
    assertFalse(childChannel.isWritable());

    // Now write an window update frame for the stream which then should ensure we will flush the bytes that were
    // queued in the RemoteFlowController before for the stream.
    frameInboundWriter.writeInboundWindowUpdate(childChannel.stream().id(), (int) bytesBeforeUnwritable);
    assertTrue(childChannel.isWritable());
    assertTrue(future.isDone());
  }

  @Test(groups = "unit")
  public void channelClosedWhenInactiveFired() {
    LastInboundHandler inboundHandler = new LastInboundHandler();
    Http2StreamChannel childChannel = newInboundStream(3, false, inboundHandler);

    final AtomicBoolean channelOpen = new AtomicBoolean(false);
    final AtomicBoolean channelActive = new AtomicBoolean(false);
    assertTrue(childChannel.isOpen());
    assertTrue(childChannel.isActive());

    childChannel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
      @Override
      public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        channelOpen.set(ctx.channel().isOpen());
        channelActive.set(ctx.channel().isActive());

        super.channelInactive(ctx);
      }
    });

    childChannel.close().syncUninterruptibly();
    assertFalse(channelOpen.get());
    assertFalse(channelActive.get());
  }

  @Test(groups = "unit")
  public void channelInactiveHappensAfterExceptionCaughtEvents() throws Exception {
    final AtomicInteger count = new AtomicInteger(0);
    final AtomicInteger exceptionCaught = new AtomicInteger(-1);
    final AtomicInteger channelInactive = new AtomicInteger(-1);
    final AtomicInteger channelUnregistered = new AtomicInteger(-1);
    Http2StreamChannel childChannel = newOutboundStream(new ChannelInboundHandlerAdapter() {
      @Override
      public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        ctx.close();
        throw new Exception("exception");
      }
    });

    childChannel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
      @Override
      public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        channelInactive.set(count.getAndIncrement());
        super.channelInactive(ctx);
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        exceptionCaught.set(count.getAndIncrement());
        super.exceptionCaught(ctx, cause);
      }

      @Override
      public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        channelUnregistered.set(count.getAndIncrement());
        super.channelUnregistered(ctx);
      }
    });

    childChannel.pipeline().fireUserEventTriggered(new Object());
    parentChannel.runPendingTasks();

    // The events should have happened in this order because the inactive and deregistration events
    // get deferred as they do in the AbstractChannel.
    assertEquals(0, exceptionCaught.get());
    assertEquals(1, channelInactive.get());
    assertEquals(2, channelUnregistered.get());
  }

  @Test(groups = "unit")
  public void callUnsafeCloseMultipleTimes() {
    LastInboundHandler inboundHandler = new LastInboundHandler();
    Http2StreamChannel childChannel = newInboundStream(3, false, inboundHandler);
    childChannel.unsafe().close(childChannel.voidPromise());

    ChannelPromise promise = childChannel.newPromise();
    childChannel.unsafe().close(promise);
    promise.syncUninterruptibly();
    childChannel.closeFuture().syncUninterruptibly();
  }

  @Test(groups = "unit")
  public void endOfStreamDoesNotDiscardData() {
    AtomicInteger numReads = new AtomicInteger(1);
    final AtomicBoolean shouldDisableAutoRead = new AtomicBoolean();
    Consumer<ChannelHandlerContext> ctxConsumer = new Consumer<ChannelHandlerContext>() {
      @Override
      public void accept(ChannelHandlerContext obj) {
        if (shouldDisableAutoRead.get()) {
          obj.channel().config().setAutoRead(false);
        }
      }
    };
    LastInboundHandler inboundHandler = new LastInboundHandler(ctxConsumer);
    Http2StreamChannel childChannel = newInboundStream(3, false, numReads, inboundHandler);
    Http2FrameStream stream = childChannel.stream();
    childChannel.config().setAutoRead(false);

    Http2DataFrame dataFrame1 = new DefaultHttp2DataFrame(bb("1")).stream(childChannel.stream());
    Http2DataFrame dataFrame2 = new DefaultHttp2DataFrame(bb("2")).stream(childChannel.stream());
    Http2DataFrame dataFrame3 = new DefaultHttp2DataFrame(bb("3")).stream(childChannel.stream());
    Http2DataFrame dataFrame4 = new DefaultHttp2DataFrame(bb("4")).stream(childChannel.stream());

    assertEquals(new DefaultHttp2HeadersFrame(request).stream(childChannel.stream()), inboundHandler.readInbound());

    ChannelHandler readCompleteSupressHandler = new ChannelInboundHandlerAdapter() {
      @Override
      public void channelReadComplete(ChannelHandlerContext ctx) {
        // We want to simulate the parent channel calling channelRead and delay calling channelReadComplete.
      }
    };

    parentChannel.pipeline().addFirst(readCompleteSupressHandler);
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("1"), 0, false);

    assertEqualsAndRelease(dataFrame1, inboundHandler.<Http2DataFrame>readInbound());

    // Deliver frames, and then a stream closed while read is inactive.
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("2"), 0, false);
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("3"), 0, false);
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("4"), 0, false);

    shouldDisableAutoRead.set(true);
    childChannel.config().setAutoRead(true);
    numReads.set(1);

    frameInboundWriter.writeInboundRstStream(childChannel.stream().id(), Http2Error.NO_ERROR.code());

    // Detecting EOS should flush all pending data regardless of read calls.
    assertEqualsAndRelease(dataFrame2, inboundHandler.<Http2DataFrame>readInbound());
    assertNull(inboundHandler.readInbound());

    // As we limited the number to 1 we also need to call read() again.
    childChannel.read();

    assertEqualsAndRelease(dataFrame3, inboundHandler.<Http2DataFrame>readInbound());
    assertEqualsAndRelease(dataFrame4, inboundHandler.<Http2DataFrame>readInbound());

    Http2ResetFrame resetFrame =
        useUserEventForResetFrame() ? inboundHandler.readUserEvent() : inboundHandler.readInbound();

    assertEquals(stream, resetFrame.stream());
    assertEquals(Http2Error.NO_ERROR.code(), resetFrame.errorCode());

    assertNull(inboundHandler.readInbound());

    // Now we want to call channelReadComplete and simulate the end of the read loop.
    parentChannel.pipeline().remove(readCompleteSupressHandler);
    parentChannel.flushInbound();

    if (multiplexHandler.reuseChildChannelsEnabled()) {
      // If channel reuse is enabled, the channel is not closed.
      childChannel.close().syncUninterruptibly();
    } else {
      childChannel.closeFuture().syncUninterruptibly();
    }
  }

  protected abstract boolean useUserEventForResetFrame();

  protected abstract boolean ignoreWindowUpdateFrames();

  @Test(groups = "unit")
  public void windowUpdateFrames() {
    AtomicInteger numReads = new AtomicInteger(1);
    LastInboundHandler inboundHandler = new LastInboundHandler();
    Http2StreamChannel childChannel = newInboundStream(3, false, numReads, inboundHandler);

    assertEquals(new DefaultHttp2HeadersFrame(request).stream(childChannel.stream()), inboundHandler.readInbound());

    frameInboundWriter.writeInboundWindowUpdate(childChannel.stream().id(), 4);

    Http2WindowUpdateFrame updateFrame = inboundHandler.readInbound();
    if (ignoreWindowUpdateFrames()) {
      assertNull(updateFrame);
    } else {
      assertEquals(new DefaultHttp2WindowUpdateFrame(4).stream(childChannel.stream()), updateFrame);
    }

    frameInboundWriter.writeInboundWindowUpdate(Http2CodecUtil.CONNECTION_STREAM_ID, 6);

    assertNull(parentChannel.readInbound());
    childChannel.close().syncUninterruptibly();
  }

  @Test(groups = "unit")
  public void childQueueIsDrainedAndNewDataIsDispatchedInParentReadLoopAutoRead() {
    AtomicInteger numReads = new AtomicInteger(1);
    final AtomicInteger channelReadCompleteCount = new AtomicInteger(0);
    final AtomicBoolean shouldDisableAutoRead = new AtomicBoolean();
    Consumer<ChannelHandlerContext> ctxConsumer = new Consumer<ChannelHandlerContext>() {
      @Override
      public void accept(ChannelHandlerContext obj) {
        channelReadCompleteCount.incrementAndGet();
        if (shouldDisableAutoRead.get()) {
          obj.channel().config().setAutoRead(false);
        }
      }
    };
    LastInboundHandler inboundHandler = new LastInboundHandler(ctxConsumer);
    Http2StreamChannel childChannel = newInboundStream(3, false, numReads, inboundHandler);
    childChannel.config().setAutoRead(false);

    Http2DataFrame dataFrame1 = new DefaultHttp2DataFrame(bb("1")).stream(childChannel.stream());
    Http2DataFrame dataFrame2 = new DefaultHttp2DataFrame(bb("2")).stream(childChannel.stream());
    Http2DataFrame dataFrame3 = new DefaultHttp2DataFrame(bb("3")).stream(childChannel.stream());
    Http2DataFrame dataFrame4 = new DefaultHttp2DataFrame(bb("4")).stream(childChannel.stream());

    assertEquals(new DefaultHttp2HeadersFrame(request).stream(childChannel.stream()), inboundHandler.readInbound());

    ChannelHandler readCompleteSupressHandler = new ChannelInboundHandlerAdapter() {
      @Override
      public void channelReadComplete(ChannelHandlerContext ctx) {
        // We want to simulate the parent channel calling channelRead and delay calling channelReadComplete.
      }
    };
    parentChannel.pipeline().addFirst(readCompleteSupressHandler);

    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("1"), 0, false);

    assertEqualsAndRelease(dataFrame1, inboundHandler.<Http2DataFrame>readInbound());

    // We want one item to be in the queue, and allow the numReads to be larger than 1. This will ensure that
    // when beginRead() is called the child channel is added to the readPending queue of the parent channel.
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("2"), 0, false);

    numReads.set(10);
    shouldDisableAutoRead.set(true);
    childChannel.config().setAutoRead(true);

    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("3"), 0, false);
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("4"), 0, false);

    // Detecting EOS should flush all pending data regardless of read calls.
    assertEqualsAndRelease(dataFrame2, inboundHandler.<Http2DataFrame>readInbound());
    assertEqualsAndRelease(dataFrame3, inboundHandler.<Http2DataFrame>readInbound());
    assertEqualsAndRelease(dataFrame4, inboundHandler.<Http2DataFrame>readInbound());

    assertNull(inboundHandler.readInbound());

    // Now we want to call channelReadComplete and simulate the end of the read loop.
    parentChannel.pipeline().remove(readCompleteSupressHandler);
    parentChannel.flushInbound();

    // 3 = 1 for initialization + 1 for read when auto read was off + 1 for when auto read was back on
    assertEquals(3, channelReadCompleteCount.get());
  }

  @Test(groups = "unit")
  public void childQueueIsDrainedAndNewDataIsDispatchedInParentReadLoopNoAutoRead() {
    final AtomicInteger numReads = new AtomicInteger(1);
    final AtomicInteger channelReadCompleteCount = new AtomicInteger(0);
    final AtomicBoolean shouldDisableAutoRead = new AtomicBoolean();
    Consumer<ChannelHandlerContext> ctxConsumer = new Consumer<ChannelHandlerContext>() {
      @Override
      public void accept(ChannelHandlerContext obj) {
        channelReadCompleteCount.incrementAndGet();
        if (shouldDisableAutoRead.get()) {
          obj.channel().config().setAutoRead(false);
        }
      }
    };
    final LastInboundHandler inboundHandler = new LastInboundHandler(ctxConsumer);
    Http2StreamChannel childChannel = newInboundStream(3, false, numReads, inboundHandler);
    childChannel.config().setAutoRead(false);

    Http2DataFrame dataFrame1 = new DefaultHttp2DataFrame(bb("1")).stream(childChannel.stream());
    Http2DataFrame dataFrame2 = new DefaultHttp2DataFrame(bb("2")).stream(childChannel.stream());
    Http2DataFrame dataFrame3 = new DefaultHttp2DataFrame(bb("3")).stream(childChannel.stream());
    Http2DataFrame dataFrame4 = new DefaultHttp2DataFrame(bb("4")).stream(childChannel.stream());

    assertEquals(new DefaultHttp2HeadersFrame(request).stream(childChannel.stream()), inboundHandler.readInbound());

    ChannelHandler readCompleteSupressHandler = new ChannelInboundHandlerAdapter() {
      @Override
      public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // We want to simulate the parent channel calling channelRead and delay calling channelReadComplete.
      }
    };
    parentChannel.pipeline().addFirst(readCompleteSupressHandler);

    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("1"), 0, false);

    assertEqualsAndRelease(dataFrame1, inboundHandler.readInbound());

    // We want one item to be in the queue, and allow the numReads to be larger than 1. This will ensure that
    // when beginRead() is called the child channel is added to the readPending queue of the parent channel.
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("2"), 0, false);

    numReads.set(2);
    childChannel.read();

    assertEqualsAndRelease(dataFrame2, inboundHandler.readInbound());

    assertNull(inboundHandler.readInbound());

    // This is the second item that was read, this should be the last until we call read() again. This should also
    // notify of readComplete().
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("3"), 0, false);

    assertEqualsAndRelease(dataFrame3, inboundHandler.readInbound());

    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("4"), 0, false);
    assertNull(inboundHandler.readInbound());

    childChannel.read();

    assertEqualsAndRelease(dataFrame4, inboundHandler.readInbound());

    assertNull(inboundHandler.readInbound());

    // Now we want to call channelReadComplete and simulate the end of the read loop.
    parentChannel.pipeline().remove(readCompleteSupressHandler);
    parentChannel.flushInbound();

    // 3 = 1 for initialization + 1 for first read of 2 items + 1 for second read of 2 items +
    // 1 for parent channel readComplete
    assertEquals(4, channelReadCompleteCount.get());
  }

  @Test(groups = "unit")
  public void useReadWithoutAutoReadInRead() {
    useReadWithoutAutoReadBuffered(false);
  }

  @Test(groups = "unit")
  public void useReadWithoutAutoReadInReadComplete() {
    useReadWithoutAutoReadBuffered(true);
  }

  private void useReadWithoutAutoReadBuffered(final boolean triggerOnReadComplete) {
    LastInboundHandler inboundHandler = new LastInboundHandler();
    Http2StreamChannel childChannel = newInboundStream(3, false, inboundHandler);
    assertTrue(childChannel.config().isAutoRead());
    childChannel.config().setAutoRead(false);
    assertFalse(childChannel.config().isAutoRead());

    Http2HeadersFrame headersFrame = inboundHandler.readInbound();
    assertNotNull(headersFrame);

    // Write some bytes to get the channel into the idle state with buffered data and also verify we
    // do not dispatch it until we receive a read() call.
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("hello world"), 0, false);
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("foo"), 0, false);
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("bar"), 0, false);

    // Add a handler which will request reads.
    childChannel.pipeline().addFirst(new ChannelInboundHandlerAdapter() {
      @Override
      public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        if (triggerOnReadComplete) {
          ctx.read();
          ctx.read();
        }
      }

      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ctx.fireChannelRead(msg);
        if (!triggerOnReadComplete) {
          ctx.read();
          ctx.read();
        }
      }
    });

    inboundHandler.channel().read();

    verifyFramesMultiplexedToCorrectChannel(childChannel, inboundHandler, 3);

    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("hello world2"), 0, false);
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("foo2"), 0, false);
    frameInboundWriter.writeInboundData(childChannel.stream().id(), bb("bar2"), 0, true);

    verifyFramesMultiplexedToCorrectChannel(childChannel, inboundHandler, 3);
  }

  private static void verifyFramesMultiplexedToCorrectChannel(
      Http2StreamChannel streamChannel,
      LastInboundHandler inboundHandler,
      int numFrames) {
    for (int i = 0; i < numFrames; i++) {
      Http2StreamFrame frame = inboundHandler.readInbound();
      assertNotNull(frame, i + " out of " + numFrames + " received");
      assertEquals(streamChannel.stream(), frame.stream());
      release(frame);
    }
    assertNull(inboundHandler.readInbound());
  }

  private static void verifyFramesMultiplexedToCorrectChannel(
      Http2FrameStream stream,
      LastInboundHandler inboundHandler,
      int numFrames) {
    for (int i = 0; i < numFrames; i++) {
      Http2StreamFrame frame = inboundHandler.readInbound();
      assertNotNull(frame, i + " out of " + numFrames + " received");
      assertEquals(stream, frame.stream());
      release(frame);
    }
    assertNull(inboundHandler.readInbound());
  }

  private static int eqStreamId(Http2StreamChannel channel) {
    return eq(channel.stream().id());
  }

  private static int eqStreamId(Http2FrameStream stream) {
    return eq(stream.id());
  }
}
