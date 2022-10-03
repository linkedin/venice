package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.Msg;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicHttpResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpContentEncoder.Result;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Promise;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A replacement for the Netty HttpContentEncoder which can handle use in client and server pipelines.
 *
 * Created by acurtis on 6/26/17.
 */
public abstract class BasicHttpContentEncoder extends MessageToMessageCodec<HttpObject, HttpObject> {
  private static final Logger LOG = LogManager.getLogger(BasicHttpContentEncoder.class);
  static final String IDENTITY = HttpHeaderValues.IDENTITY.toString();

  private enum State {
    PASS_THROUGH, AWAIT_HEADERS, AWAIT_CONTENT
  }

  private static final CharSequence ZERO_LENGTH_HEAD = "HEAD";
  private static final CharSequence ZERO_LENGTH_CONNECT = "CONNECT";
  private static final int CONTINUE_CODE = HttpResponseStatus.CONTINUE.code();

  private final Queue<CharSequence> acceptEncodingQueue = new ArrayDeque<CharSequence>();
  private CharSequence acceptEncoding;
  private CharSequence requestEncoding;
  private EmbeddedChannel encoder;
  private State state = State.AWAIT_HEADERS;

  @Override
  public boolean acceptOutboundMessage(Object msg) throws Exception {
    return msg instanceof HttpContent || msg instanceof HttpResponse || msg instanceof HttpRequest;
  }

  protected CharSequence requestEncoding(HttpRequest request) {
    if (request.method() == HttpMethod.CONNECT) {
      return ZERO_LENGTH_CONNECT;
    }
    if (request.method() == HttpMethod.HEAD) {
      return ZERO_LENGTH_HEAD;
    }

    return requestEncoding0(request);
  }

  protected CharSequence requestEncoding0(HttpRequest request) {
    return requestEncoding;
  }

  protected void encodeTime(long nanos) {
  }

  protected void decodeTime(long nanos) {
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
    long startNanos = Time.nanoTime();
    try {
      decode0(ctx, msg, out);
    } finally {
      decodeTime(Time.nanoTime() - startNanos);
    }
  }

  private void decode0(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
    if (msg instanceof HttpMessage) {
      HttpMessage message = (HttpMessage) msg;

      CharSequence acceptedEncoding = message.headers().get(HttpHeaderNames.ACCEPT_ENCODING);
      if (acceptedEncoding == null) {
        acceptedEncoding = IDENTITY;
      }

      if (message instanceof HttpRequest) {
        HttpRequest request = (HttpRequest) message;

        HttpMethod meth = request.method();
        if (meth == HttpMethod.HEAD) {
          acceptedEncoding = ZERO_LENGTH_HEAD;
        } else if (meth == HttpMethod.CONNECT) {
          acceptedEncoding = ZERO_LENGTH_CONNECT;
        }

        acceptEncodingQueue.add(acceptedEncoding);
      } else {
        requestEncoding = acceptedEncoding;
      }
    }
    out.add(ReferenceCountUtil.retain(msg));
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
    long startNanos = Time.nanoTime();
    try {
      encode0(ctx, msg, out);
    } finally {
      encodeTime(Time.nanoTime() - startNanos);
    }
  }

  private void encode0(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
    final boolean isFull = msg instanceof HttpMessage && msg instanceof LastHttpContent;
    HttpMessage newRes = null;
    switch (state) {
      case AWAIT_HEADERS: {
        final HttpMessage res = ensureHeaders(msg);
        assert encoder == null;

        final int code;
        if (res instanceof HttpResponse) {
          code = ((HttpResponse) res).status().code();
          if (code == CONTINUE_CODE) {
            // We need to not poll the encoding when response with CONTINUE as another response will follow
            // for the issued request. See https://github.com/netty/netty/issues/4079
            acceptEncoding = null;
          } else {
            // Get the list of encodings accepted by the peer.
            acceptEncoding = acceptEncodingQueue.poll();
            if (acceptEncoding == null) {
              throw new IllegalStateException("cannot send more responses than requests");
            }
          }
        } else if (res instanceof HttpRequest) {
          HttpRequest request = (HttpRequest) res;
          if (request.headers().contains(HttpHeaderNames.CONTENT_LENGTH)
              || request.headers().contains(HttpHeaderNames.TRANSFER_ENCODING)) {
            code = 200;
          } else {
            code = 204;
          }
          acceptEncoding = requestEncoding(request);
        } else {
          throw new IllegalStateException();
        }
        /*
         * per rfc2616 4.3 Message Body
         * All 1xx (informational), 204 (no content), and 304 (not modified) responses MUST NOT include a
         * message-body. All other responses do include a message-body, although it MAY be of zero length.
         *
         * 9.4 HEAD
         * The HEAD method is identical to GET except that the server MUST NOT return a message-body
         * in the response.
         *
         * Also we should pass through HTTP/1.0 as transfer-encoding: chunked is not supported.
         *
         * See https://github.com/netty/netty/issues/5382
         */
        if (isPassthru(res.protocolVersion(), code, acceptEncoding)) {
          if (isFull) {
            out.add(ReferenceCountUtil.retain(res));
          } else {
            out.add(res);
            // Pass through all following contents.
            state = State.PASS_THROUGH;
          }
          break;
        }

        if (isFull) {
          // Pass through the full response with empty content and continue waiting for the the next resp.
          if (!((ByteBufHolder) res).content().isReadable()) {
            out.add(ReferenceCountUtil.retain(res));
            break;
          }
        }

        // Prepare to encode the content.
        final Result result = beginEncode(res, acceptEncoding.toString());

        // If unable to encode, pass through.
        if (result == null) {
          if (isFull) {
            out.add(ReferenceCountUtil.retain(res));
          } else {
            out.add(res);
            // Pass through all following contents.
            state = State.PASS_THROUGH;
          }
          break;
        }

        encoder = result.contentEncoder();

        // Encode the content and remove or replace the existing headers
        // so that the message looks like a decoded message.
        res.headers().set(HttpHeaderNames.CONTENT_ENCODING, result.targetContentEncoding());

        if (!isFull || !res.headers().contains(HttpHeaderNames.CONTENT_LENGTH)) {
          // Make the response chunked to simplify content transformation.
          res.headers().remove(HttpHeaderNames.CONTENT_LENGTH);
          res.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        }

        // Output the rewritten response.
        if (isFull) {
          // Convert full message into unfull one.
          newRes = res instanceof HttpResponse
              ? new BasicHttpResponse((HttpResponse) res)
              : new BasicHttpRequest((HttpRequest) res);
          newRes.headers().set(res.headers());
          out.add(newRes);
          // Fall through to encode the content of the full response.
        } else {
          out.add(res);
          state = State.AWAIT_CONTENT;
          if (!(msg instanceof HttpContent)) {
            // only break out the switch statement if we have not content to process
            // See https://github.com/netty/netty/issues/2006
            break;
          }
        }
      }
      // Fall through to encode the content
      case AWAIT_CONTENT: { // SUPPRESS CHECKSTYLE FallThrough
        ensureContent(msg);
        int existingMessages = out.size();
        if (encodeContent(ctx, (HttpContent) msg, out)) {
          state = State.AWAIT_HEADERS;
          if (newRes != null) {
            if (HttpUtil.isContentLengthSet(newRes)) {
              // adjust the content-length header
              int messageSize = 0;
              for (int i = existingMessages; i < out.size(); i++) {
                Object item = out.get(i);
                if (item instanceof HttpContent) {
                  messageSize += ((HttpContent) item).content().readableBytes();
                }
              }
              HttpUtil.setContentLength(newRes, messageSize);
            } else {
              newRes.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
            }
          }
        } else if (newRes != null) {
          // We cannot yet determine the content length. Convert message into chunked encoding
          HttpUtil.setTransferEncodingChunked(newRes, true);
        }
        break;
      }
      case PASS_THROUGH: {
        ensureContent(msg);
        out.add(ReferenceCountUtil.retain(msg));
        // Passed through all following contents of the current response.
        if (msg instanceof LastHttpContent) {
          state = State.AWAIT_HEADERS;
        }
        break;
      }
      default:
        throw new IllegalStateException();
    }
  }

  private static boolean isPassthru(HttpVersion version, int code, CharSequence httpMethod) {
    return code < 200 || code == 204 || code == 304
        || (httpMethod == ZERO_LENGTH_HEAD || (httpMethod == ZERO_LENGTH_CONNECT && code == 200))
        || version == HttpVersion.HTTP_1_0;
  }

  private static HttpMessage ensureHeaders(HttpObject msg) {
    if (!(msg instanceof HttpResponse || msg instanceof HttpRequest)) {
      throw new IllegalStateException("unexpected message type: " + msg.getClass().getName());
    }
    return (HttpMessage) msg;
  }

  private static void ensureContent(HttpObject msg) {
    if (!(msg instanceof HttpContent)) {
      throw new IllegalStateException(
          "unexpected message type: " + msg.getClass().getName() + " (expected: " + HttpContent.class.getSimpleName()
              + ')');
    }
  }

  private boolean encodeContent(ChannelHandlerContext ctx, HttpContent c, List<Object> out)
      throws InterruptedException {
    ByteBuf content = c.content();

    encode(content, out);

    if (c instanceof LastHttpContent) {
      finishEncode(ctx, out);
      LastHttpContent last = (LastHttpContent) c;

      // Generate an additional chunk if the decoder produced
      // the last product on closure,
      HttpHeaders headers = last.trailingHeaders();
      if (headers.isEmpty()) {
        out.add(LastHttpContent.EMPTY_LAST_CONTENT);
      } else {
        out.add(new ComposedLastHttpContent(headers));
      }
      return true;
    }
    return false;
  }

  /**
   * Prepare to encode the HTTP message content.
   *
   * @param headers
   *        the headers
   * @param acceptEncoding
   *        the value of the {@code "Accept-Encoding"} header
   *
   * @return the result of preparation, which is composed of the determined
   *         target content encoding and a new {@link EmbeddedChannel} that
   *         encodes the content into the target content encoding.
   *         {@code null} if {@code acceptEncoding} is unsupported or rejected
   *         and thus the content should be handled as-is (i.e. no encoding).
   */
  protected abstract Result beginEncode(HttpMessage headers, String acceptEncoding) throws Exception;

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    cleanup();
    super.handlerRemoved(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    try {
      cleanup();
    } finally {
      super.channelInactive(ctx);
    }
  }

  private void cleanup() {
    if (encoder != null) {
      // Clean-up the previous encoder if not cleaned up correctly.
      if (encoder.finish()) {
        for (;;) {
          ByteBuf buf = encoder.readOutbound();
          if (buf == null) {
            break;
          }
          // Release the buffer
          // https://github.com/netty/netty/issues/1524
          buf.release();
        }
      }
      encoder = null;
    }
  }

  private void encode(ByteBuf in, List<Object> out) {
    // call retain here as it will call release after its written to the channel
    EmbeddedChannel encoder = this.encoder;
    encoder.writeOutbound(in.retain());
    fetchEncoderOutput(encoder, out);
  }

  private void finishEncode(ChannelHandlerContext ctx, List<Object> out) throws InterruptedException {
    EmbeddedChannel encoder = this.encoder;
    this.encoder = null;

    Promise<Void> promise = StreamSupport.stream(encoder.pipeline().spliterator(), false)
        .map(Map.Entry::getValue)
        .map(encoder.pipeline()::context)
        .filter(handlerContext -> handlerContext.executor() != encoder.eventLoop())
        .map(handlerContext -> handlerContext.executor().<Void>newPromise())
        .findFirst()
        .orElseGet(ctx.executor()::newPromise);

    encoder.close().addListener(closeFuture -> {
      if (closeFuture.isSuccess()) {
        promise.setSuccess(null);
      } else {
        promise.setFailure(closeFuture.cause());
      }
    });

    if (!promise.isDone()) {
      long blockStart = Time.nanoTime();
      try {
        promise.sync();
      } finally {
        long blockTime = Time.nanoTime() - blockStart;
        LOG.debug(
            "finishEncode blocked for {} ns : {}",
            blockTime,
            Msg.make(encoder.pipeline().names(), names -> names.stream().collect(Collectors.joining(","))));
      }
    }
    encoder.checkException();
    fetchEncoderOutput(encoder, out);
  }

  private void fetchEncoderOutput(EmbeddedChannel encoder, List<Object> out) {
    for (;;) {
      ByteBuf buf = encoder.readOutbound();
      if (buf == null) {
        break;
      }
      if (!buf.isReadable()) {
        buf.release();
        continue;
      }
      out.add(new DefaultHttpContent(buf));
    }
  }

  private static final class ComposedLastHttpContent implements LastHttpContent {
    private final HttpHeaders trailingHeaders;
    private DecoderResult result;

    ComposedLastHttpContent(HttpHeaders trailingHeaders) {
      this.trailingHeaders = trailingHeaders;
    }

    @Override
    public HttpHeaders trailingHeaders() {
      return trailingHeaders;
    }

    @Override
    public LastHttpContent copy() {
      LastHttpContent content = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
      content.trailingHeaders().set(trailingHeaders());
      return content;
    }

    @Override
    public LastHttpContent duplicate() {
      return copy();
    }

    @Override
    public LastHttpContent retainedDuplicate() {
      return copy();
    }

    @Override
    public LastHttpContent replace(ByteBuf content) {
      final LastHttpContent dup = new DefaultLastHttpContent(content);
      dup.trailingHeaders().setAll(trailingHeaders());
      return dup;
    }

    @Override
    public LastHttpContent retain(int increment) {
      return this;
    }

    @Override
    public LastHttpContent retain() {
      return this;
    }

    @Override
    public LastHttpContent touch() {
      return this;
    }

    @Override
    public LastHttpContent touch(Object hint) {
      return this;
    }

    @Override
    public ByteBuf content() {
      return Unpooled.EMPTY_BUFFER;
    }

    @Override
    public DecoderResult decoderResult() {
      return result;
    }

    @Override
    public DecoderResult getDecoderResult() {
      return decoderResult();
    }

    @Override
    public void setDecoderResult(DecoderResult result) {
      this.result = result;
    }

    @Override
    public int refCnt() {
      return 1;
    }

    @Override
    public boolean release() {
      return false;
    }

    @Override
    public boolean release(int decrement) {
      return false;
    }
  }
}
