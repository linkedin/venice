package com.linkedin.alpini.netty4.http2;

import com.linkedin.alpini.base.misc.DateUtils;
import com.linkedin.alpini.base.misc.Pair;
import com.linkedin.alpini.netty4.handlers.AbstractLeakDetect;
import com.linkedin.alpini.netty4.handlers.HttpClientResponseHandler.ResponseConsumer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.PrematureChannelClosureException;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DefaultHttp2ConnectionDecoder;
import io.netty.handler.codec.http2.DefaultHttp2ConnectionEncoder;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2LocalFlowController;
import io.netty.handler.codec.http2.DefaultHttp2RemoteFlowController;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameCodec;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2FrameReader;
import io.netty.handler.codec.http2.Http2FrameStream;
import io.netty.handler.codec.http2.Http2FrameWriter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2LocalFlowController;
import io.netty.handler.codec.http2.Http2OutboundFrameLogger;
import io.netty.handler.codec.http2.Http2RemoteFlowController;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.handler.logging.LogLevel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.internal.PlatformDependent;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * @author acurtis
 */
@Test(groups = { "unit", "unit-leak", "leak" }, singleThreaded = true)
public final class TestHttp2ClientResponseHandler extends AbstractLeakDetect {
  private static final Logger LOG = LogManager.getLogger(TestHttp2ClientResponseHandler.class);

  Http2Connection connection;
  Http2ConnectionEncoder encoder;
  Http2ConnectionDecoder decoder;
  final Http2FrameWriter frameWriter =
      Mockito.mock(Http2FrameWriter.class, Mockito.withSettings().defaultAnswer(this::defaultFrameWriter));
  final Http2FrameReader frameReader =
      Mockito.mock(Http2FrameReader.class, Mockito.withSettings().defaultAnswer(this::defaultFrameReader));
  Http2RemoteFlowController remoteFlowController;
  Http2LocalFlowController localFlowController;

  private Object defaultFrameWriter(InvocationOnMock invocation) throws Throwable {
    LOG.info("defaultFrameWriter {}", invocation.getMethod().getName());
    return Mockito.CALLS_REAL_METHODS.answer(invocation);
  }

  private Object defaultFrameReader(InvocationOnMock invocation) throws Throwable {
    LOG.info("defaultFrameReader {}", invocation.getMethod().getName());
    return Mockito.CALLS_REAL_METHODS.answer(invocation);
  }

  @BeforeMethod
  public void initalizeMocks() {
    connection = Mockito.mock(
        DefaultHttp2Connection.class,
        Mockito.withSettings().useConstructor(false).defaultAnswer(Mockito.CALLS_REAL_METHODS));
    remoteFlowController = Mockito.mock(
        DefaultHttp2RemoteFlowController.class,
        Mockito.withSettings().useConstructor(connection).defaultAnswer(Mockito.CALLS_REAL_METHODS));
    localFlowController = Mockito.mock(
        DefaultHttp2LocalFlowController.class,
        Mockito.withSettings()
            .useConstructor(connection, DefaultHttp2LocalFlowController.DEFAULT_WINDOW_UPDATE_RATIO, false));
    connection.remote().flowController(remoteFlowController);
    connection.local().flowController(localFlowController);

    Http2FrameLogger frameLogger = new Http2FrameLogger(LogLevel.INFO);
    Http2FrameWriter loggingFrameWriter = new Http2OutboundFrameLogger(frameWriter, frameLogger);

    encoder = Mockito.mock(
        DefaultHttp2ConnectionEncoder.class,
        Mockito.withSettings()
            .useConstructor(connection, loggingFrameWriter)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    decoder = Mockito.mock(
        DefaultHttp2ConnectionDecoder.class,
        Mockito.withSettings()
            .useConstructor(connection, encoder, frameReader)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));

    Mockito.doAnswer(invocation -> ((ChannelPromise) invocation.getArgument(2)).setSuccess())
        .when(frameWriter)
        .writeSettings(Mockito.any(), Mockito.any(), Mockito.any());
    Mockito.doAnswer(invocation -> {
      ReferenceCountUtil.release(invocation.getArgument(3));
      return ((ChannelPromise) invocation.getArgument(4)).setSuccess();
    }).when(frameWriter).writeGoAway(Mockito.any(), Mockito.anyInt(), Mockito.anyLong(), Mockito.any(), Mockito.any());
    Mockito.doAnswer(invocation -> null).when(frameWriter).close();
    Mockito.doAnswer(invocation -> null).when(frameReader).close();

    Mockito.doAnswer(invocation -> {
      ChannelPromise writePromise = invocation.getArgument(3);
      return writePromise.setSuccess();
    }).when(frameWriter).writeRstStream(Mockito.any(), Mockito.anyInt(), Mockito.anyLong(), Mockito.any());

  }

  @AfterMethod
  public void resetMocks() {
    Mockito.reset(connection, remoteFlowController, localFlowController, encoder, decoder, frameReader, frameWriter);
    connection = null;
    remoteFlowController = null;
    localFlowController = null;
    encoder = null;
    decoder = null;
  }

  public void testBadEnvironment() {
    EmbeddedChannel ch = new EmbeddedChannel(new Http2ClientResponseHandler());
    Assert.assertTrue(ch.closeFuture().isDone());
  }

  public void testAcceptQuietly() {
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.doAnswer(invocation -> Mockito.mock(ChannelPromise.class)).when(ctx).newPromise();
    Consumer<Object> consumer = Mockito.mock(Consumer.class);
    HttpObject httpObject = Mockito.mock(HttpObject.class);
    ChannelPromise promise = Mockito.mock(ChannelPromise.class);
    Channel channel = Mockito.mock(Channel.class);
    SimpleChannelPromiseAggregator promiseAggregator =
        new SimpleChannelPromiseAggregator(promise, channel, ImmediateEventExecutor.INSTANCE);
    Http2ClientResponseHandler.State state =
        new Http2ClientResponseHandler.State(ctx, null, consumer, httpObject, 0, promiseAggregator);
    Mockito.doThrow(new IllegalStateException()).when(consumer).accept(Mockito.any());
    // this should capture the exception, log and discard.
    Http2ClientResponseHandler.acceptQuietly(state, 1);
  }

  public void testCaptureException() {
    NullPointerException ex = new NullPointerException();
    ChannelPromise promise = Mockito.mock(ChannelPromise.class);
    ReferenceCounted msg = Mockito.mock(ReferenceCounted.class);
    Mockito.when(promise.tryFailure(Mockito.any())).thenReturn(true);
    Http2ClientResponseHandler.captureException(ex, msg, promise);
    Mockito.verify(promise).tryFailure(ex);
    Mockito.verify(msg).release();
    Mockito.reset(msg, promise);
    Http2ClientResponseHandler.captureException(ex, msg, promise);
  }

  @Test(invocationCount = 5, expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "message does not implement ResponseConsumer")
  public void testBadRequest() throws InterruptedException {
    Http2Settings initialSettings = new Http2Settings();
    boolean decoupleCloseAndGoAway = false;
    Http2FrameCodec mockFrameCodec = Mockito.mock(
        Http2FrameCodec.class,
        Mockito.withSettings()
            .useConstructor(encoder, decoder, initialSettings, decoupleCloseAndGoAway)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    EmbeddedChannel ch = new EmbeddedChannel(mockFrameCodec, new Http2ClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    FullHttpRequest request =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", POOLED_ALLOCATOR.buffer());
    try {
      ch.writeAndFlush(request).sync();
    } finally {
      Assert.assertEquals(request.refCnt(), 0);
      ch.finishAndReleaseAll();
    }
  }

  @Test(invocationCount = 5, expectedExceptions = Http2Exception.class, expectedExceptionsMessageRegExp = "Stream never existed")
  public void testUnexpectedResponse() throws InterruptedException {
    Http2Settings initialSettings = new Http2Settings();
    boolean decoupleCloseAndGoAway = false;
    Http2FrameCodec mockFrameCodec = Mockito.mock(
        Http2FrameCodec.class,
        Mockito.withSettings()
            .useConstructor(encoder, decoder, initialSettings, decoupleCloseAndGoAway)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    EmbeddedChannel ch = new EmbeddedChannel(mockFrameCodec, new Http2ClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    Http2FrameStream stream = Mockito.mock(Http2FrameStream.class);
    Mockito.doReturn(Http2Stream.State.OPEN, Http2Stream.State.CLOSED).when(stream).state();
    Http2DataFrame response = new DefaultHttp2DataFrame(POOLED_ALLOCATOR.buffer()).stream(stream);
    try {
      ch.writeOneInbound(response).sync();
      ch.pipeline().fireChannelReadComplete();
      Assert.fail();
    } finally {
      Assert.assertEquals(response.refCnt(), 0);
      ch.finishAndReleaseAll();
    }
  }

  @Test(invocationCount = 5)
  public void testGoAway() throws Exception {
    Http2Settings initialSettings = new Http2Settings();
    boolean decoupleCloseAndGoAway = false;
    Http2FrameCodec mockFrameCodec = Mockito.mock(
        Http2FrameCodec.class,
        Mockito.withSettings()
            .useConstructor(encoder, decoder, initialSettings, decoupleCloseAndGoAway)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    EmbeddedChannel ch = new EmbeddedChannel(mockFrameCodec, new Http2ClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    try {
      // Should trigger the h2c handshake
      ch.writeAndFlush(Unpooled.EMPTY_BUFFER).sync();
      ByteBuf output = ch.readOutbound();
      Assert.assertTrue(
          ByteBufUtil.equals(
              output,
              Unpooled.wrappedBuffer("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII))),
          ByteBufUtil.prettyHexDump(output));
      output.release();
      Assert.assertSame(ch.readOutbound(), Unpooled.EMPTY_BUFFER);

      LinkedBlockingQueue<Object> responses = new LinkedBlockingQueue<>();

      class Request extends DefaultFullHttpRequest implements ResponseConsumer {
        Request(HttpMethod method, String uri) {
          super(
              HttpVersion.HTTP_1_1,
              method,
              uri,
              POOLED_ALLOCATOR.buffer(),
              new Http1Headers(new DefaultHttp2Headers(), false),
              new Http1Headers(new DefaultHttp2Headers(), false));
        }

        @Override
        public Consumer<Object> responseConsumer() {
          return responses::add;
        }
      }

      LinkedBlockingQueue<Pair<ChannelHandlerContext, Integer>> streamIds = streamIdQueue();

      for (int i = 5; i > 0; i--) {
        responses.clear();
        Request request = new Request(HttpMethod.GET, "/");
        ch.writeAndFlush(request).sync();
        Assert.assertEquals(request.refCnt(), 0);

        Assert.assertTrue(responses.isEmpty());

        ch.runScheduledPendingTasks();
        Pair<ChannelHandlerContext, Integer> pair = streamIds.remove();
        // Increase code coverage by exercising a branch
        decoder.frameListener().onPingRead(pair.getFirst(), ThreadLocalRandom.current().nextLong());
        Http2Headers responseHeaders = new DefaultHttp2Headers();
        responseHeaders.status(HttpResponseStatus.OK.codeAsText());
        ByteBuf goAwayMessage = encodeString("Chill out", StandardCharsets.UTF_8);
        decoder.frameListener()
            .onGoAwayRead(pair.getFirst(), pair.getSecond(), Http2Error.ENHANCE_YOUR_CALM.code(), goAwayMessage);
        goAwayMessage.release();
        ByteBuf content = encodeString("Simple response", StandardCharsets.UTF_8);
        decoder.frameListener().onHeadersRead(pair.getFirst(), pair.getSecond(), responseHeaders, 0, false);
        decoder.frameListener().onDataRead(pair.getFirst(), pair.getSecond(), content, 0, true);
        ch.pipeline().fireChannelReadComplete();
        ch.runScheduledPendingTasks();

        Assert.assertFalse(responses.isEmpty());
        HttpResponse response = (HttpResponse) responses.remove();
        Assert.assertSame(response.status(), HttpResponseStatus.OK);
        ReferenceCountUtil.release(response);
        responses.forEach(ReferenceCountUtil::release);
      }
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  private LinkedBlockingQueue<Pair<ChannelHandlerContext, Integer>> streamIdQueue() {
    LinkedBlockingQueue<Pair<ChannelHandlerContext, Integer>> streamIds = new LinkedBlockingQueue<>();

    Mockito.doAnswer(invocation -> {
      ChannelHandlerContext ctx = invocation.getArgument(0);
      int streamId = invocation.getArgument(1);
      ChannelPromise writePromise = invocation.getArgument(5);
      streamIds.add(Pair.make(ctx, streamId));
      return writePromise.setSuccess();
    })
        .when(frameWriter)
        .writeHeaders(
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.anyBoolean(),
            Mockito.any());
    return streamIds;
  }

  @Test(invocationCount = 5)
  public void testBasicResponse() throws Exception {
    Http2Settings initialSettings = new Http2Settings();
    boolean decoupleCloseAndGoAway = false;
    Http2FrameCodec mockFrameCodec = Mockito.mock(
        Http2FrameCodec.class,
        Mockito.withSettings()
            .useConstructor(encoder, decoder, initialSettings, decoupleCloseAndGoAway)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    EmbeddedChannel ch = new EmbeddedChannel(mockFrameCodec, new Http2ClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    try {
      LinkedBlockingQueue<Object> responses = new LinkedBlockingQueue<>();

      class Request extends DefaultFullHttpRequest implements ResponseConsumer {
        Request(HttpMethod method, String uri) {
          super(
              HttpVersion.HTTP_1_1,
              method,
              uri,
              POOLED_ALLOCATOR.buffer(),
              new Http1Headers(new DefaultHttp2Headers(), false),
              new Http1Headers(new DefaultHttp2Headers(), false));
        }

        @Override
        public Consumer<Object> responseConsumer() {
          return responses::add;
        }
      }

      LinkedBlockingQueue<Pair<ChannelHandlerContext, Integer>> streamIds = streamIdQueue();

      for (int i = 5; i > 0; i--) {
        responses.clear();
        Request request = new Request(HttpMethod.GET, "/");
        ch.writeAndFlush(request).sync();
        Assert.assertEquals(request.refCnt(), 0);

        Assert.assertTrue(responses.isEmpty());

        ch.runScheduledPendingTasks();
        Pair<ChannelHandlerContext, Integer> pair = streamIds.remove();
        Http2Headers responseHeaders = new DefaultHttp2Headers();
        responseHeaders.status(HttpResponseStatus.NOT_FOUND.codeAsText());
        decoder.frameListener().onHeadersRead(pair.getFirst(), pair.getSecond(), responseHeaders, 0, true);
        ch.pipeline().fireChannelReadComplete();
        ch.runScheduledPendingTasks();

        Assert.assertFalse(responses.isEmpty());
        HttpResponse response = (HttpResponse) responses.remove();
        Assert.assertSame(response.status(), HttpResponseStatus.NOT_FOUND);
        ReferenceCountUtil.release(response);
        responses.forEach(ReferenceCountUtil::release);
      }
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test(invocationCount = 5)
  public void testSmallResponse() throws Exception {
    Http2Settings initialSettings = new Http2Settings();
    boolean decoupleCloseAndGoAway = false;
    Http2FrameCodec mockFrameCodec = Mockito.mock(
        Http2FrameCodec.class,
        Mockito.withSettings()
            .useConstructor(encoder, decoder, initialSettings, decoupleCloseAndGoAway)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    EmbeddedChannel ch = new EmbeddedChannel(mockFrameCodec, new Http2ClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    try {
      LinkedBlockingQueue<Object> responses = new LinkedBlockingQueue<>();

      class Request extends DefaultFullHttpRequest implements ResponseConsumer {
        Request(HttpMethod method, String uri) {
          super(
              HttpVersion.HTTP_1_1,
              method,
              uri,
              POOLED_ALLOCATOR.buffer(),
              new Http1Headers(new DefaultHttp2Headers(), false),
              new Http1Headers(new DefaultHttp2Headers(), false));
        }

        @Override
        public Consumer<Object> responseConsumer() {
          return responses::add;
        }
      }

      LinkedBlockingQueue<Pair<ChannelHandlerContext, Integer>> streamIds = streamIdQueue();

      for (int i = 5; i > 0; i--) {
        responses.clear();
        Request request = new Request(HttpMethod.GET, "/");
        ch.writeAndFlush(request).sync();
        Assert.assertEquals(request.refCnt(), 0);

        Assert.assertTrue(responses.isEmpty());

        ch.runScheduledPendingTasks();
        Pair<ChannelHandlerContext, Integer> pair = streamIds.remove();
        Http2Headers responseHeaders = new DefaultHttp2Headers();
        responseHeaders.status(HttpResponseStatus.OK.codeAsText());
        decoder.frameListener().onHeadersRead(pair.getFirst(), pair.getSecond(), responseHeaders, 0, false);
        ByteBuf content = encodeString("Simple response", StandardCharsets.UTF_8);
        decoder.frameListener().onDataRead(pair.getFirst(), pair.getSecond(), content, 0, true);
        ch.pipeline().fireChannelReadComplete();
        ch.runScheduledPendingTasks();

        Assert.assertFalse(responses.isEmpty());
        HttpResponse response = (HttpResponse) responses.remove();
        Assert.assertSame(response.status(), HttpResponseStatus.OK);
        ReferenceCountUtil.release(response);
        responses.forEach(ReferenceCountUtil::release);
      }
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test(invocationCount = 5)
  public void testBadResponse() throws Exception {
    Http2Settings initialSettings = new Http2Settings();
    boolean decoupleCloseAndGoAway = false;
    Http2FrameCodec mockFrameCodec = Mockito.mock(
        Http2FrameCodec.class,
        Mockito.withSettings()
            .useConstructor(encoder, decoder, initialSettings, decoupleCloseAndGoAway)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    EmbeddedChannel ch = new EmbeddedChannel(mockFrameCodec, new Http2ClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    try {
      LinkedBlockingQueue<Object> responses = new LinkedBlockingQueue<>();

      class Request extends DefaultFullHttpRequest implements ResponseConsumer {
        Request(HttpMethod method, String uri) {
          super(
              HttpVersion.HTTP_1_1,
              method,
              uri,
              POOLED_ALLOCATOR.buffer(),
              new Http1Headers(new DefaultHttp2Headers(), false),
              new Http1Headers(new DefaultHttp2Headers(), false));
        }

        @Override
        public Consumer<Object> responseConsumer() {
          return responses::add;
        }
      }

      LinkedBlockingQueue<Pair<ChannelHandlerContext, Integer>> streamIds = streamIdQueue();

      for (int i = 5; i > 0; i--) {
        responses.clear();
        Request request = new Request(HttpMethod.GET, "/");
        ch.writeAndFlush(request).sync();
        Assert.assertEquals(request.refCnt(), 0);

        Assert.assertTrue(responses.isEmpty());

        ch.runScheduledPendingTasks();
        Pair<ChannelHandlerContext, Integer> pair = streamIds.remove();
        ByteBuf content = encodeString("Simple response", StandardCharsets.UTF_8);
        decoder.frameListener().onDataRead(pair.getFirst(), pair.getSecond(), content, 0, true);
        ch.pipeline().fireChannelReadComplete();
        ch.runScheduledPendingTasks();

        Assert.assertFalse(responses.isEmpty());
        IllegalStateException response = (IllegalStateException) responses.remove();
        Assert.assertEquals(response.getMessage(), "received a data frame before a headers frame");
        responses.forEach(ReferenceCountUtil::release);
      }
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test(invocationCount = 5)
  public void testFullResponse() throws Exception {
    Http2Settings initialSettings = new Http2Settings();
    boolean decoupleCloseAndGoAway = false;
    Http2FrameCodec mockFrameCodec = Mockito.mock(
        Http2FrameCodec.class,
        Mockito.withSettings()
            .useConstructor(encoder, decoder, initialSettings, decoupleCloseAndGoAway)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    EmbeddedChannel ch = new EmbeddedChannel(mockFrameCodec, new Http2ClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    try {
      ArrayList<Object> responses = new ArrayList<>();

      class Request extends DefaultFullHttpRequest implements ResponseConsumer {
        Request(HttpMethod method, String uri) {
          super(
              HttpVersion.HTTP_1_1,
              method,
              uri,
              POOLED_ALLOCATOR.buffer(),
              new Http1Headers(new DefaultHttp2Headers(), false),
              new Http1Headers(new DefaultHttp2Headers(), false));
        }

        @Override
        public Consumer<Object> responseConsumer() {
          return responses::add;
        }
      }

      LinkedBlockingQueue<Pair<ChannelHandlerContext, Integer>> streamIds = streamIdQueue();

      for (int i = 5; i > 0; i--) {
        responses.clear();
        Request request = new Request(HttpMethod.GET, "/");
        ch.writeAndFlush(request).sync();
        Assert.assertEquals(request.refCnt(), 0);

        Assert.assertTrue(responses.isEmpty());

        ch.runScheduledPendingTasks();
        Pair<ChannelHandlerContext, Integer> pair = streamIds.remove();
        Http2Headers responseHeaders = new DefaultHttp2Headers();
        Http2Headers trailingHeaders = new DefaultHttp2Headers();
        responseHeaders.status(HttpResponseStatus.OK.codeAsText());
        responseHeaders.set(HttpHeaderNames.TRAILER, HttpHeaderNames.EXPIRES, HttpHeaderNames.VIA);
        decoder.frameListener().onHeadersRead(pair.getFirst(), pair.getSecond(), responseHeaders, 0, false);
        ByteBuf content = encodeString("Simple response", StandardCharsets.UTF_8);
        decoder.frameListener().onDataRead(pair.getFirst(), pair.getSecond(), content, 0, false);
        trailingHeaders
            .set(HttpHeaderNames.EXPIRES, DateUtils.getRFC1123Date(Instant.now().plusSeconds(20).toEpochMilli()));
        trailingHeaders.set(HttpHeaderNames.VIA, "Foo");
        decoder.frameListener().onHeadersRead(pair.getFirst(), pair.getSecond(), trailingHeaders, 0, true);
        ch.pipeline().fireChannelReadComplete();
        ch.runScheduledPendingTasks();

        Assert.assertFalse(responses.isEmpty());

        HttpResponse response = (HttpResponse) responses.get(0);
        Assert.assertSame(response.status(), HttpResponseStatus.OK);
        HttpContent httpContent = (HttpContent) responses.get(1);
        LastHttpContent lastHttpContent = (LastHttpContent) responses.get(2);

        responses.forEach(ReferenceCountUtil::release);
      }
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test(invocationCount = 5)
  public void testResetResponse() throws Exception {
    Http2Settings initialSettings = new Http2Settings();
    boolean decoupleCloseAndGoAway = false;
    Http2FrameCodec mockFrameCodec = Mockito.mock(
        Http2FrameCodec.class,
        Mockito.withSettings()
            .useConstructor(encoder, decoder, initialSettings, decoupleCloseAndGoAway)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    EmbeddedChannel ch = new EmbeddedChannel(mockFrameCodec, new Http2ClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    try {
      LinkedBlockingQueue<Object> responses = new LinkedBlockingQueue<>();

      class Request extends DefaultFullHttpRequest implements ResponseConsumer {
        Request(HttpMethod method, String uri) {
          super(
              HttpVersion.HTTP_1_1,
              method,
              uri,
              POOLED_ALLOCATOR.buffer(),
              new Http1Headers(new DefaultHttp2Headers(), false),
              new Http1Headers(new DefaultHttp2Headers(), false));
        }

        @Override
        public Consumer<Object> responseConsumer() {
          return responses::add;
        }
      }

      LinkedBlockingQueue<Pair<ChannelHandlerContext, Integer>> streamIds = streamIdQueue();

      for (int i = 5; i > 0; i--) {
        responses.clear();
        Request request = new Request(HttpMethod.GET, "/");
        ch.writeAndFlush(request).sync();
        Assert.assertEquals(request.refCnt(), 0);

        Assert.assertTrue(responses.isEmpty());

        ch.runScheduledPendingTasks();
        Pair<ChannelHandlerContext, Integer> pair = streamIds.remove();
        Http2Stream stream = mockFrameCodec.connection().stream(pair.getSecond());
        decoder.frameListener().onRstStreamRead(pair.getFirst(), pair.getSecond(), Http2Error.CANCEL.code());

        ch.pipeline().fireChannelReadComplete();
        ch.runScheduledPendingTasks();

        Assert.assertFalse(responses.isEmpty());
        Throwable response = (Throwable) responses.remove();
        Assert.assertTrue(response instanceof PrematureChannelClosureException);
        responses.forEach(ReferenceCountUtil::release);
      }
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test(invocationCount = 5)
  public void testSmallRequestContent() throws Exception {
    Http2Settings initialSettings = new Http2Settings();
    boolean decoupleCloseAndGoAway = false;
    Http2FrameCodec mockFrameCodec = Mockito.mock(
        Http2FrameCodec.class,
        Mockito.withSettings()
            .useConstructor(encoder, decoder, initialSettings, decoupleCloseAndGoAway)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    EmbeddedChannel ch = new EmbeddedChannel(mockFrameCodec, new Http2ClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    try {
      LinkedBlockingQueue<Object> responses = new LinkedBlockingQueue<>();

      class Request extends DefaultFullHttpRequest implements ResponseConsumer {
        Request(HttpMethod method, String uri) {
          super(HttpVersion.HTTP_1_1, method, uri, encodeString("Smol Request", StandardCharsets.UTF_8));
        }

        @Override
        public Consumer<Object> responseConsumer() {
          return responses::add;
        }
      }

      Mockito.doAnswer(invocation -> {
        ChannelPromise promise = invocation.getArgument(5);
        ByteBuf data = invocation.getArgument(2);
        LOG.info("Write data to stream {} of {}", invocation.getArgument(1), ByteBufUtil.prettyHexDump(data));
        data.release();
        return promise.setSuccess();
      })
          .when(frameWriter)
          .writeData(
              Mockito.any(),
              Mockito.anyInt(),
              Mockito.any(),
              Mockito.anyInt(),
              Mockito.anyBoolean(),
              Mockito.any());

      LinkedBlockingQueue<Pair<ChannelHandlerContext, Integer>> streamIds = streamIdQueue();

      for (int i = 5; i > 0; i--) {
        responses.clear();
        Request request = new Request(HttpMethod.POST, "/");
        if (i == 1) {
          request.headers().set(HttpHeaderNames.TRAILER, "X-Foo");
          request.trailingHeaders().set("X-Foo", "bar");
        }
        ch.writeAndFlush(request);
        ch.runPendingTasks();
        Assert.assertEquals(request.refCnt(), 0);

        Assert.assertTrue(responses.isEmpty());

        ch.runScheduledPendingTasks();
        Pair<ChannelHandlerContext, Integer> pair = streamIds.remove();
        Http2Headers responseHeaders = new DefaultHttp2Headers();
        responseHeaders.status(HttpResponseStatus.OK.codeAsText());
        decoder.frameListener().onHeadersRead(pair.getFirst(), pair.getSecond(), responseHeaders, 0, false);
        ByteBuf content = encodeString("Simple response", StandardCharsets.UTF_8);
        decoder.frameListener().onDataRead(pair.getFirst(), pair.getSecond(), content, 0, true);
        ch.pipeline().fireChannelReadComplete();
        ch.runScheduledPendingTasks();

        Assert.assertFalse(responses.isEmpty());
        HttpResponse response = (HttpResponse) responses.remove();
        Assert.assertSame(response.status(), HttpResponseStatus.OK);
        ReferenceCountUtil.release(response);
        responses.forEach(ReferenceCountUtil::release);
      }
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test(invocationCount = 5)
  public void testLargeRequestContent() throws Exception {
    Http2Settings initialSettings = new Http2Settings();
    boolean decoupleCloseAndGoAway = false;
    Http2FrameCodec mockFrameCodec = Mockito.mock(
        Http2FrameCodec.class,
        Mockito.withSettings()
            .useConstructor(encoder, decoder, initialSettings, decoupleCloseAndGoAway)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    EmbeddedChannel ch = new EmbeddedChannel(mockFrameCodec, new Http2ClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    try {
      LinkedBlockingQueue<Object> responses = new LinkedBlockingQueue<>();

      class Request extends DefaultFullHttpRequest implements ResponseConsumer {
        Request(HttpMethod method, String uri) {
          super(
              HttpVersion.HTTP_1_1,
              method,
              uri,
              encodeString(
                  "Large Request"
                      + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
                      + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",
                  StandardCharsets.UTF_8));
        }

        @Override
        public Consumer<Object> responseConsumer() {
          return responses::add;
        }
      }

      Mockito.doAnswer(invocation -> {
        ChannelPromise promise = invocation.getArgument(5);
        ByteBuf data = invocation.getArgument(2);
        LOG.info("Write data to stream {} of {}", invocation.getArgument(1), ByteBufUtil.prettyHexDump(data));
        data.release();
        return promise.setSuccess();
      })
          .when(frameWriter)
          .writeData(
              Mockito.any(),
              Mockito.anyInt(),
              Mockito.any(),
              Mockito.anyInt(),
              Mockito.anyBoolean(),
              Mockito.any());

      LinkedBlockingQueue<Pair<ChannelHandlerContext, Integer>> streamIds = streamIdQueue();

      for (int i = 5; i > 0; i--) {
        responses.clear();
        Request request = new Request(HttpMethod.POST, "/");
        if (i == 1) {
          request.headers().set(HttpHeaderNames.TRAILER, "X-Foo");
          request.trailingHeaders().set("X-Foo", "bar");
        }

        ch.writeAndFlush(request);
        ch.runPendingTasks();
        ch.runPendingTasks();
        Assert.assertEquals(request.refCnt(), 0);

        Assert.assertTrue(responses.isEmpty());

        ch.runScheduledPendingTasks();
        Pair<ChannelHandlerContext, Integer> pair = streamIds.remove();

        // Just to improve code coverage but it won't do much here since request is fully sent.
        decoder.frameListener().onWindowUpdateRead(pair.getFirst(), pair.getSecond(), 100);

        Http2Headers responseHeaders = new DefaultHttp2Headers();
        responseHeaders.status(HttpResponseStatus.OK.codeAsText());
        decoder.frameListener().onHeadersRead(pair.getFirst(), pair.getSecond(), responseHeaders, 0, false);
        ByteBuf content = encodeString("Simple response", StandardCharsets.UTF_8);
        decoder.frameListener().onDataRead(pair.getFirst(), pair.getSecond(), content, 0, true);
        ch.pipeline().fireChannelReadComplete();
        ch.runScheduledPendingTasks();

        Assert.assertFalse(responses.isEmpty());
        HttpResponse response = (HttpResponse) responses.remove();
        Assert.assertSame(response.status(), HttpResponseStatus.OK);
        ReferenceCountUtil.release(response);
        responses.forEach(ReferenceCountUtil::release);
      }
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test(invocationCount = 5)
  public void testExceptionFired() throws Exception {
    Exception expectedException = new IllegalStateException("Expected");
    Http2Settings initialSettings = new Http2Settings();
    boolean decoupleCloseAndGoAway = false;
    Http2FrameCodec mockFrameCodec = Mockito.mock(
        Http2FrameCodec.class,
        Mockito.withSettings()
            .useConstructor(encoder, decoder, initialSettings, decoupleCloseAndGoAway)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    LinkedBlockingQueue<Pair<ChannelHandlerContext, Integer>> streamIds = streamIdQueue();

    EmbeddedChannel ch = new EmbeddedChannel(mockFrameCodec, new Http2ClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    try {
      CompletableFuture<Object> future = new CompletableFuture<>();

      class Request extends DefaultFullHttpRequest implements ResponseConsumer {
        Request(HttpMethod method, String uri) {
          super(HttpVersion.HTTP_1_1, method, uri, POOLED_ALLOCATOR.buffer());
        }

        @Override
        public Consumer<Object> responseConsumer() {
          return this::consumer;
        }

        private void consumer(Object o) {
          if (o instanceof HttpObject) {
            PlatformDependent.throwException(expectedException);
          }
          future.complete(o);
        }
      }

      Request request = new Request(HttpMethod.GET, "/");
      ch.writeAndFlush(request).sync();

      Assert.assertFalse(future.isDone());

      Pair<ChannelHandlerContext, Integer> pair = streamIds.remove();
      Http2Headers responseHeaders = new DefaultHttp2Headers();
      responseHeaders.status(HttpResponseStatus.OK.codeAsText());
      decoder.frameListener().onHeadersRead(pair.getFirst(), pair.getSecond(), responseHeaders, 0, false);
      ByteBuf content = encodeString("Simple response", StandardCharsets.UTF_8);
      decoder.frameListener().onDataRead(pair.getFirst(), pair.getSecond(), content, 0, true);
      ch.pipeline().fireChannelReadComplete();
      ch.runScheduledPendingTasks();

      Assert.assertTrue(future.isDone());
      Assert.assertSame(future.getNow(null), expectedException);

      Assert.assertEquals(content.refCnt(), 0);
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test(invocationCount = 5)
  public void testExceptionFiredClosed() throws InterruptedException {
    Http2Settings initialSettings = new Http2Settings();
    boolean decoupleCloseAndGoAway = false;
    Http2FrameCodec mockFrameCodec = Mockito.mock(
        Http2FrameCodec.class,
        Mockito.withSettings()
            .useConstructor(encoder, decoder, initialSettings, decoupleCloseAndGoAway)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    LinkedBlockingQueue<Pair<ChannelHandlerContext, Integer>> streamIds = streamIdQueue();

    EmbeddedChannel ch = new EmbeddedChannel(mockFrameCodec, new Http2ClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    try {
      CompletableFuture<Object> future = new CompletableFuture<>();

      class Request extends DefaultFullHttpRequest implements ResponseConsumer {
        Request(HttpMethod method, String uri) {
          super(HttpVersion.HTTP_1_1, method, uri, POOLED_ALLOCATOR.buffer());
        }

        @Override
        public Consumer<Object> responseConsumer() {
          return this::consumer;
        }

        private void consumer(Object o) {
          future.complete(o);
        }
      }

      Request request = new Request(HttpMethod.GET, "/");
      ch.writeAndFlush(request).sync();

      Assert.assertFalse(future.isDone());
      ch.close().sync();

      ch.runScheduledPendingTasks();

      Assert.assertTrue(future.isDone());
      Assert.assertTrue(future.getNow(null) instanceof PrematureChannelClosureException);
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  /**
   * since TestNG tends to sort by method name, this tries to be the last test
   * in the class. We do this because the AfterClass annotated methods may
   * execute after other tests classes have run and doesn't execute immediately
   * after the methods in this test class.
   */
  @Test(alwaysRun = true)
  public final void zz9PluralZAlpha() throws InterruptedException {
    finallyLeakDetect();
  }
}
