package com.linkedin.alpini.netty4.http2;

import com.linkedin.alpini.base.concurrency.Lazy;
import com.linkedin.alpini.netty4.handlers.HttpClientResponseHandler;
import com.linkedin.alpini.netty4.misc.Http2Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.PrematureChannelClosureException;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.DefaultHttp2ResetFrame;
import io.netty.handler.codec.http2.EspressoHttp2FrameCodecUtil;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Frame;
import io.netty.handler.codec.http2.Http2FrameCodec;
import io.netty.handler.codec.http2.Http2FrameStream;
import io.netty.handler.codec.http2.Http2FrameStreamEvent;
import io.netty.handler.codec.http2.Http2FrameStreamException;
import io.netty.handler.codec.http2.Http2GoAwayFrame;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2PingFrame;
import io.netty.handler.codec.http2.Http2ResetFrame;
import io.netty.handler.codec.http2.Http2SettingsAckFrame;
import io.netty.handler.codec.http2.Http2SettingsFrame;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.handler.codec.http2.Http2StreamFrame;
import io.netty.handler.codec.http2.Http2WindowUpdateFrame;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.util.AttributeMap;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A handler which processes {@link HttpRequest} objects decorated with {@link HttpClientResponseHandler.ResponseConsumer}
 * interface written to the pipeline and transforms it into HTTP/2 frames as well as handle responding HTTP/2 frames and
 * marshalling them to the response consumer.
 *
 * @author acurtis
 */
public class Http2ClientResponseHandler extends ChannelDuplexHandler {
  private static final Logger LOG = LogManager.getLogger(Http2ClientResponseHandler.class);

  private final Map<Http2FrameStream, State> _responseConsumers = new IdentityHashMap<>();
  private volatile Http2FrameCodec _frameCodec;
  private final HttpScheme _httpScheme;
  private final boolean _validateHeaders;
  private volatile Http2FrameStream _lastSeenStreamId;
  private volatile State _currentState;
  private volatile boolean _readInProgress;
  private volatile boolean _wantsFlush;

  private final Consumer<Throwable> _failedToWriteHeaderFrame;

  public Http2ClientResponseHandler() {
    this(HttpScheme.HTTPS, false, Http2ClientResponseHandler::failedToWriteHeaderFrame);
  }

  /**
   * Constructor
   *
   * @param httpScheme default HttpScheme if a scheme is not already set.
   * @param validateHeaders {@code true} to validate header names according to
   *      * <a href="https://tools.ietf.org/html/rfc7540">rfc7540</a>. {@code false} to not validate header names.
   * @param failedToWriteHeaderFrame Callback which is called for every time we fail to write the
   *                                 initial Http2HeaderFrame to the channel - which would occur
   *                                 when no more streams can be created.
   */
  public Http2ClientResponseHandler(
      HttpScheme httpScheme,
      boolean validateHeaders,
      Consumer<Throwable> failedToWriteHeaderFrame) {
    _httpScheme = httpScheme;
    _validateHeaders = validateHeaders;
    _failedToWriteHeaderFrame = Objects.requireNonNull(failedToWriteHeaderFrame);
  }

  private static void failedToWriteHeaderFrame(Throwable ex) {
    LOG.warn("Failed to write header frame", ex);
  }

  static final class State {
    final Http2FrameStream _stream;
    final Consumer<Object> _consumer;
    int _windowSize;
    HttpObject _httpObject;
    boolean _endOfHeaders;
    final ChannelPromise _lastWritePromise;
    ChannelFuture _previousDataWriteFuture;

    State(
        ChannelHandlerContext ctx,
        Http2FrameStream stream,
        Consumer<Object> consumer,
        HttpObject httpObject,
        int windowSize,
        SimpleChannelPromiseAggregator promiseAggregator) {
      _stream = stream;
      _consumer = consumer;
      _httpObject = httpObject;
      _windowSize = Math.max(64, windowSize); // guard against zero window size which occurs in tests
      _lastWritePromise = newAggregatedPromise(ctx, promiseAggregator);
      _previousDataWriteFuture = ctx.newSucceededFuture();
    }
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    _wantsFlush = false;
    super.flush(ctx);
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    _readInProgress = false;
    _lastSeenStreamId = null;
    if (_wantsFlush) {
      flush(ctx);
    }
    super.channelReadComplete(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof Http2Frame) {
      channelRead(ctx, (Http2Frame) msg);
    } else {
      super.channelRead(ctx, msg);
    }
  }

  private void channelRead(ChannelHandlerContext ctx, Http2Frame msg) throws Exception {
    _readInProgress = true;
    if (msg instanceof Http2StreamFrame) {
      channelRead(ctx, (Http2StreamFrame) msg);
      _lastSeenStreamId = null;
      return;
    }

    if (msg instanceof Http2GoAwayFrame) {
      Http2GoAwayFrame goAwayFrame = (Http2GoAwayFrame) msg;
      // we will rely on Http2FrameCodec managing the GoAway state. We just need to tell relevant
      // streams that they will not be getting any response.
      PrematureChannelClosureException exception = new PrematureChannelClosureException("Go Away received");
      _responseConsumers.forEach(((frameStream, state) -> {
        int streamId = frameStream.id();
        if (streamId > goAwayFrame.lastStreamId() && _frameCodec.connection().local().isValidStreamId(streamId)) {
          acceptQuietly(state, exception);
        }
      }));
      goAwayFrame.release();
      return;
    }

    if (!(msg instanceof Http2SettingsFrame) && !(msg instanceof Http2SettingsAckFrame)
        && !(msg instanceof Http2PingFrame)) {

      LOG.warn("Unhandled message type {}", msg.getClass());
    }

    super.channelRead(ctx, msg);
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof Http2FrameStreamEvent) {
      userEventTriggered(ctx, (Http2FrameStreamEvent) evt);
    }
    super.userEventTriggered(ctx, evt);
  }

  private void userEventTriggered(ChannelHandlerContext ctx, Http2FrameStreamEvent evt) {
    // Because in their infinite wisdom, the enum type is package private
    switch (String.valueOf(evt.type())) {
      case "State":
        switch (evt.stream().state()) {
          case CLOSED:
            State state = _responseConsumers.remove(evt.stream());
            if (state != null) {
              acceptQuietly(state, new PrematureChannelClosureException());
            }
          case OPEN: // SUPPRESS CHECKSTYLE FallThoroughCheck
          case HALF_CLOSED_LOCAL: // SUPPRESS CHECKSTYLE FallThoroughCheck
          case HALF_CLOSED_REMOTE: // SUPPRESS CHECKSTYLE FallThoroughCheck
          default:
            break;
        }
      case "Writability": // SUPPRESS CHECKSTYLE FallThoroughCheck
      default:
        break;
    }
    assert ctx != null;
  }

  private void channelRead(ChannelHandlerContext ctx, Http2StreamFrame msg) {
    _lastSeenStreamId = msg.stream();
    State state = _responseConsumers.get(ReferenceCountUtil.touch(msg, this).stream());
    try {
      if (msg instanceof Http2ResetFrame) {
        channelReadResetFrame(state, (Http2ResetFrame) msg);
        return;
      }
      if (state == null) {
        LOG.warn("Received message for an unknown stream: {}, {}", msg.stream(), msg.getClass().getSimpleName());
        // Received message for an unknown stream.
        resetStream(ctx, msg.stream(), Http2Error.CANCEL);
        return;
      }
      if (msg instanceof Http2DataFrame) {
        channelReadDataFrame(ctx, state, (Http2DataFrame) msg);
        return;
      }

      if (msg instanceof Http2HeadersFrame) {
        channelReadHeadersFrame(state, (Http2HeadersFrame) msg);
        return;
      }

      if (msg instanceof Http2WindowUpdateFrame) {
        Http2WindowUpdateFrame frame = (Http2WindowUpdateFrame) msg;
        state._windowSize += frame.windowSizeIncrement();
        return;
      }
      LOG.warn("unhandled frame type {}", msg.name());
    } catch (Exception ex) {
      _responseConsumers.remove(msg.stream());
      if (state != null) {
        acceptQuietly(state, ex);
      } else {
        throw ex;
      }
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }

  private void channelReadResetFrame(State state, Http2ResetFrame frame) {
    _responseConsumers.remove(frame.stream());
    if (state == null) {
      // Nothing to do
      return;
    }
    Http2Error http2Error = Http2Error.valueOf(frame.errorCode());
    Object o;
    if (http2Error == null) {
      o = new PrematureChannelClosureException("Unknown errorcode: " + frame.errorCode());
    } else {
      switch (http2Error) {
        case NO_ERROR:
          o = LastHttpContent.EMPTY_LAST_CONTENT;
          break;
        case CANCEL:
          o = new PrematureChannelClosureException();
          break;
        default:
          o = new PrematureChannelClosureException("Stream closed with error", new Http2Exception(http2Error));
          break;
      }
    }
    acceptQuietly(state, o);
  }

  private void endOfHeaders(State state) {
    state._endOfHeaders = true;
    state._consumer.accept(state._httpObject);
  }

  public void channelReadDataFrame(ChannelHandlerContext ctx, State state, Http2DataFrame frame) {
    frame.touch(this);
    try {
      if (!state._endOfHeaders) {
        if (state._httpObject instanceof HttpResponse) {
          endOfHeaders(state);
        } else {
          IllegalStateException ex = new IllegalStateException("received a data frame before a headers frame");
          _responseConsumers.remove(frame.stream());
          LOG.warn("received a data frame before a headers frame on stream {}", frame.stream(), ex);
          acceptQuietly(state, ex);
          resetStream(ctx, frame.stream(), Http2Error.PROTOCOL_ERROR);
          return;
        }
      }

      state._consumer.accept(new DefaultHttpContent(frame.content()).retain());

      if (frame.isEndStream()) {
        _responseConsumers.remove(frame.stream());
        state._consumer.accept(LastHttpContent.EMPTY_LAST_CONTENT);
      }
    } finally {
      frame.release();
    }
  }

  private void channelReadHeadersFrame(State state, Http2HeadersFrame frame) {
    if (state._httpObject instanceof HttpRequest) {
      HttpRequest request = (HttpRequest) state._httpObject;
      Http1Headers headers = new Http1Headers(frame.headers(), _validateHeaders);
      state._httpObject = new Http1Response(request, headers);
    } else if (!state._endOfHeaders) {
      // Handling concatenated HTTP/2 headers frame
      HttpResponse response = (HttpResponse) state._httpObject;
      frame.headers().forEach(entry -> response.headers().add(entry.getKey(), entry.getValue()));
    } else {
      LastHttpContent lastHttpContent;
      if (state._httpObject instanceof LastHttpContent) {
        lastHttpContent = (LastHttpContent) state._httpObject;
      } else {
        lastHttpContent = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER, _validateHeaders);
        state._httpObject = lastHttpContent;
      }
      frame.headers().forEach(entry -> lastHttpContent.trailingHeaders().add(entry.getKey(), entry.getValue()));

      if (frame.isEndStream()) {
        _responseConsumers.remove(frame.stream());
        state._consumer.accept(lastHttpContent);
      }
      return;
    }

    if (frame.isEndStream()) {
      _responseConsumers.remove(frame.stream());
      endOfHeaders(state);
      state._consumer.accept(LastHttpContent.EMPTY_LAST_CONTENT);
    }
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (!(msg instanceof HttpMessage || msg instanceof HttpContent)) {
      super.write(ctx, msg, promise);
      return;
    }

    SimpleChannelPromiseAggregator promiseAggregator =
        new SimpleChannelPromiseAggregator(promise, ctx.channel(), ctx.executor());
    boolean sync = true;
    try {
      boolean endStream = false;

      if (msg instanceof HttpRequest) {
        endStream = writeRequest(ctx, (HttpRequest) msg, promiseAggregator);
      }
      if (!endStream && msg instanceof HttpContent) {
        sync = writeContent(ctx, (HttpContent) msg, promiseAggregator);
      }
    } catch (Throwable t) {
      promiseAggregator.setFailure(t);
    } finally {
      if (sync) {
        // result of promises is propagated
        promiseAggregator.doneAllocatingPromises();
      }
      ReferenceCountUtil.release(msg);
    }
  }

  private boolean writeRequest(
      ChannelHandlerContext ctx,
      HttpRequest msg,
      SimpleChannelPromiseAggregator promiseAggregator) {
    LOG.debug("wrote a {}", msg);
    if (!(msg instanceof HttpClientResponseHandler.ResponseConsumer)) {
      throw new IllegalStateException("message does not implement ResponseConsumer");
    }
    HttpClientResponseHandler.ResponseConsumer responseConsumer = (HttpClientResponseHandler.ResponseConsumer) msg;

    Http2Headers http2Headers;

    if (msg.headers() instanceof Http1Headers) {
      http2Headers = ((Http1Headers) msg.headers()).getHttp2Headers();
      // Add HttpScheme if it's defined in constructor and header does not contain it.
      if (_httpScheme != null && http2Headers.scheme() == null) {
        http2Headers.scheme(_httpScheme.name());
      }
    } else {
      // Add HttpScheme if it's defined in constructor and header does not contain it.
      if (_httpScheme != null && !msg.headers().contains(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text())) {
        msg.headers().set(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), _httpScheme.name());
      }
      http2Headers = HttpConversionUtil.toHttp2Headers(msg, _validateHeaders);
    }

    Http2FrameStream frameStream = EspressoHttp2FrameCodecUtil.newStream(_frameCodec);
    bindFrameStream(msg, frameStream);

    State state = new State(
        ctx,
        frameStream,
        responseConsumer.responseConsumer(),
        msg,
        _frameCodec.connection().local().flowController().initialWindowSize(),
        promiseAggregator);
    _lastSeenStreamId = frameStream;
    _currentState = state;
    boolean endStream = msg instanceof FullHttpMessage && !((FullHttpMessage) msg).content().isReadable();
    // rely on the caller calling flush
    writeNoFlush(
        ctx,
        new DefaultHttp2HeadersFrame(http2Headers, endStream).stream(frameStream),
        endStream ? state._lastWritePromise : newAggregatedPromise(ctx, promiseAggregator)).addListener(future -> {
          if (future.isSuccess()) {
            _responseConsumers.put(frameStream, state);
          } else {
            _failedToWriteHeaderFrame.accept(future.cause());
          }
        });
    return endStream;
  }

  private static void bindFrameStream(HttpRequest msg, Http2FrameStream frameStream) {
    if (msg instanceof AttributeMap) {
      Http2Utils.setFrameStream((AttributeMap) msg, frameStream);
    }
  }

  private boolean writeContent(
      ChannelHandlerContext ctx,
      HttpContent msg,
      SimpleChannelPromiseAggregator promiseAggregator) {
    final State state = _currentState;
    if (state == null) {
      throw new IllegalStateException();
    }
    boolean isLastContent = false;
    HttpHeaders trailers = EmptyHttpHeaders.INSTANCE;
    _lastSeenStreamId = state._stream;

    if (msg instanceof LastHttpContent) {
      final LastHttpContent lastContent = (LastHttpContent) msg;
      isLastContent = true;
      // Convert any trailing headers.
      trailers = lastContent.trailingHeaders();
    }

    Http2Headers http2Trailers = HttpConversionUtil.toHttp2Headers(trailers, _validateHeaders);

    // Write the data
    final ByteBuf content = msg.touch(this).content().duplicate();
    boolean endStream = isLastContent && trailers.isEmpty();
    if (content.readableBytes() <= state._windowSize && state._previousDataWriteFuture.isSuccess()) {
      if (endStream || content.isReadable()) {
        // rely on the caller calling flush
        state._previousDataWriteFuture = writeNoFlush(
            ctx,
            new DefaultHttp2DataFrame(content, endStream).stream(state._stream).retain(),
            endStream ? state._lastWritePromise : newAggregatedPromise(ctx, promiseAggregator));
      }

      if (!trailers.isEmpty()) {
        // Write trailing headers.
        // rely on the caller calling flush
        writeNoFlush(
            ctx,
            new DefaultHttp2HeadersFrame(http2Trailers, true).stream(state._stream),
            state._lastWritePromise);
      }
      return true;
    } else {
      content.retain();
      final boolean dataIsEnd = endStream;
      final DefaultHttp2HeadersFrame trailerFrame =
          isLastContent && !trailers.isEmpty() ? new DefaultHttp2HeadersFrame(http2Trailers, true) : null;

      // Ensure in order writing of data
      ChannelFuture previousFuture = state._previousDataWriteFuture;
      ChannelPromise writePromise = dataIsEnd ? state._lastWritePromise : newAggregatedPromise(ctx, promiseAggregator);
      state._previousDataWriteFuture = writePromise;
      previousFuture.addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) {
          _lastSeenStreamId = state._stream;
          if (future.isSuccess()) {
            if (content.readableBytes() > state._windowSize) {
              // Data frame needs to be flushed before the next window can be written
              writeAndFlush(
                  ctx,
                  new DefaultHttp2DataFrame(content.readRetainedSlice(state._windowSize), false).stream(state._stream),
                  newAggregatedPromise(ctx, promiseAggregator)).addListener(this);
              return;
            }
            if (trailerFrame == null) {
              writeAndFlush(ctx, new DefaultHttp2DataFrame(content, dataIsEnd).stream(state._stream), writePromise);
            } else {
              writeNoFlush(
                  ctx,
                  new DefaultHttp2DataFrame(content, true).stream(state._stream),
                  newAggregatedPromise(ctx, promiseAggregator));
              writeAndFlush(ctx, trailerFrame.stream(state._stream), writePromise);
            }
          } else {
            content.release();
          }
          // result of promises is propagated. If any promise failed, the result is failure.
          promiseAggregator.doneAllocatingPromises();
        }
      });
      return false;
    }
  }

  @SuppressWarnings("UnusedReturnValue")
  private ChannelFuture writeNoFlush(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    try {
      return ctx.write(msg, promise);
    } catch (Throwable ex) {
      return captureException(ex, msg, promise);
    }
  }

  @SuppressWarnings("UnusedReturnValue")
  private ChannelFuture writeAndFlush(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    if (_readInProgress) {
      _wantsFlush = true;
      return writeNoFlush(ctx, msg, promise);
    }
    _wantsFlush = false;
    try {
      _wantsFlush = false;
      return ctx.writeAndFlush(msg, promise);
    } catch (Throwable ex) {
      return captureException(ex, msg, promise);
    }
  }

  static ChannelFuture captureException(Throwable ex, Object msg, ChannelPromise promise) {
    if (!promise.tryFailure(ex)) {
      LOG.warn("Exception trying to write message: {}", msg, ex);
    }
    ReferenceCountUtil.safeRelease(msg);
    return promise;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, final Throwable exception) throws Exception {
    Http2FrameStream stream = _lastSeenStreamId;
    Throwable cause = exception;
    boolean isFrameStreamException;

    if (cause instanceof Http2FrameStreamException) {
      Http2FrameStreamException streamException = (Http2FrameStreamException) cause;
      if (_lastSeenStreamId != streamException.stream()) {
        _lastSeenStreamId = null;
      }
      stream = streamException.stream();
      cause = Optional.ofNullable(streamException.getCause()).orElse(exception);
      isFrameStreamException = true;
    } else {
      isFrameStreamException = false;
      if (cause instanceof IOException || cause instanceof CodecException) {
        // These exceptions are not specific to any stream.
        stream = null;
      }
    }

    if (stream != null) {
      State state = _responseConsumers.remove(stream);
      resetStream(ctx, stream, state != null ? Http2Error.INTERNAL_ERROR : Http2Error.PROTOCOL_ERROR);
      if (state != null) {
        acceptQuietly(state, cause);
        return;
      }
      LOG.warn("Http2FrameStreamException occurred without a relevant stream state: {}, {}", stream, cause, exception);
      if (isFrameStreamException) {
        // A frame stream exception for a non-existent stream... no further processing is possible.
        return;
      }
    }
    super.exceptionCaught(ctx, exception);
  }

  private static void resetStream(ChannelHandlerContext ctx, Http2FrameStream stream, Http2Error http2Error) {
    Http2Stream.State streamState = stream.state();
    if (Http2Stream.State.CLOSED != streamState && Http2Stream.State.IDLE != streamState && null != streamState) {
      ctx.write(new DefaultHttp2ResetFrame(http2Error).stream(stream), ctx.voidPromise());
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    try {
      super.channelInactive(ctx);
    } finally {
      if (!_responseConsumers.isEmpty()) {
        Supplier<Exception> exceptionSupplier = Lazy.of(PrematureChannelClosureException::new);
        _responseConsumers.entrySet().removeIf(entry -> {
          LOG.debug("Frame stream not yet closed: {}", entry.getKey());
          acceptQuietly(entry.getValue(), exceptionSupplier.get());
          return true;
        });
      }
    }
  }

  static void acceptQuietly(State state, Object object) {
    try {
      state._consumer.accept(object);
    } catch (Throwable ex) {
      LOG.warn("unexpected exception for stream {}", state._stream, ex);
    }
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    _frameCodec = requireHttp2FrameCodec(ctx);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    _frameCodec = null;
    _lastSeenStreamId = null;
    _currentState = null;
  }

  private static Http2FrameCodec requireHttp2FrameCodec(ChannelHandlerContext ctx) {
    ChannelHandlerContext frameCodecCtx = ctx.pipeline().context(Http2FrameCodec.class);
    if (frameCodecCtx == null) {
      throw new IllegalArgumentException(
          Http2FrameCodec.class.getSimpleName() + " was not found in the channel pipeline.");
    }
    return (Http2FrameCodec) frameCodecCtx.handler();
  }

  /**
   * Helper method to construct a ChannelPromise which maintains its own listeners because the Promise returned
   * by SimpleChannelPromiseAggregator newPromise only invokes the listeners after doneAllocatingPromises.
   * @param ctx Channel handler context
   * @param promiseAggregator Promise aggregator.
   * @return
   */
  private static ChannelPromise newAggregatedPromise(
      ChannelHandlerContext ctx,
      SimpleChannelPromiseAggregator promiseAggregator) {
    return ctx.newPromise().addListener(new Listener(promiseAggregator));
  }

  private static class Listener implements ChannelFutureListener {
    private final ChannelPromise _promise;

    private Listener(SimpleChannelPromiseAggregator promiseAggregator) {
      _promise = promiseAggregator.newPromise();
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if (future.isSuccess()) {
        _promise.setSuccess();
      } else {
        _promise.setFailure(future.cause());
      }
    }
  }
}
