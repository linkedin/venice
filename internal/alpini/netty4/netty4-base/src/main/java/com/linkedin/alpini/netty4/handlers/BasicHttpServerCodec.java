package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.ExceptionUtil;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.netty4.misc.BadHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicHttpRequestDecoder;
import com.linkedin.alpini.netty4.misc.BasicHttpResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.CombinedChannelDuplexHandler;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http.HttpStatusClass;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class BasicHttpServerCodec extends CombinedChannelDuplexHandler<BasicHttpRequestDecoder, HttpResponseEncoder>
    implements HttpServerUpgradeHandler.SourceCodec {
  private static final Logger LOG = LogManager.getLogger(BasicHttpServerCodec.class);

  private static final AttributeKey<Transaction> TRANSACTION_KEY =
      AttributeKey.valueOf(BasicHttpServerCodec.class, "Transaction");

  /** A queue that is used for correlating a request and a response. */
  private final Queue<Transaction> _queue = new ArrayDeque<>();

  private static class Transaction {
    private final HttpMethod _method;

    private Transaction(BasicHttpRequest request) {
      _method = request.method();
      request.attr(TRANSACTION_KEY).set(this);
    }
  }

  /**
   * Creates a new instance with the default decoder options
   * ({@code maxInitialLineLength (4096}}, {@code maxHeaderSize (8192)}, and
   * {@code maxChunkSize (8192)}).
   */
  public BasicHttpServerCodec() {
    this(4096, 8192, 8192);
  }

  /**
   * Creates a new instance with the specified decoder options.
   */
  public BasicHttpServerCodec(int maxInitialLineLength, int maxHeaderSize, int maxChunkSize) {
    this(maxInitialLineLength, maxHeaderSize, maxChunkSize, true);
  }

  /**
   * Creates a new instance with the specified decoder options.
   */
  public BasicHttpServerCodec(int maxInitialLineLength, int maxHeaderSize, int maxChunkSize, boolean validateHeaders) {
    this(maxInitialLineLength, maxHeaderSize, maxChunkSize, validateHeaders, 128);
  }

  /**
   * Creates a new instance with the specified decoder options.
   */
  public BasicHttpServerCodec(
      int maxInitialLineLength,
      int maxHeaderSize,
      int maxChunkSize,
      boolean validateHeaders,
      int initialBufferSize) {
    init(
        createDecoder(maxInitialLineLength, maxHeaderSize, maxChunkSize, validateHeaders, initialBufferSize),
        createEncoder());
  }

  protected Decoder createDecoder(
      int maxInitialLineLength,
      int maxHeaderSize,
      int maxChunkSize,
      boolean validateHeaders,
      int initialBufferSize) {
    return new Decoder(maxInitialLineLength, maxHeaderSize, maxChunkSize, validateHeaders, initialBufferSize);
  }

  protected Encoder createEncoder() {
    return new Encoder();
  }

  /**
   * Upgrades to another protocol from HTTP. Removes the {@link BasicHttpRequestDecoder} and
   * {@link HttpResponseEncoder} from the pipeline.
   */
  @Override
  public void upgradeFrom(ChannelHandlerContext ctx) {
    ctx.pipeline().remove(this);
  }

  protected HttpMessage createMessage(
      String[] initialLine,
      boolean validateHeaders,
      long startTimeMillis,
      long startNanos) {
    return new BasicHttpRequest(
        initialLine.length <= 2 || initialLine[2].isEmpty()
            ? HttpVersionFilter.HTTP_0_9
            : HttpVersion.valueOf(initialLine[2]),
        HttpMethod.valueOf(initialLine[0]),
        initialLine[1],
        validateHeaders,
        startTimeMillis,
        startNanos);

  }

  protected HttpMessage createInvalidMessage(long startTimeMillis, long startNanos) {
    return new BadRequest(startTimeMillis, startNanos);
  }

  protected class Decoder extends BasicHttpRequestDecoder {
    private long _startTimeMillis;
    private long _startNanos;

    /**
     * Creates a new instance with the specified parameters.
     *
     * @param maxInitialLineLength
     * @param maxHeaderSize
     * @param maxChunkSize
     */
    protected Decoder(
        int maxInitialLineLength,
        int maxHeaderSize,
        int maxChunkSize,
        boolean validateHeaders,
        int initialBufferSize) {
      super(maxInitialLineLength, maxHeaderSize, maxChunkSize, validateHeaders, initialBufferSize);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
      if (_startTimeMillis == 0) {
        _startTimeMillis = Time.currentTimeMillis();
        _startNanos = Time.nanoTime();
      }
      int oldSize = out.size();
      super.decode(ctx, buffer, out);
      int size = out.size();
      for (int i = oldSize; i < size; i++) {
        Object obj = out.get(i);
        if (obj instanceof HttpRequest) {
          BasicHttpRequest request = (BasicHttpRequest) obj;
          _queue.add(new Transaction(request));
        }
        if (obj instanceof LastHttpContent) {
          _startTimeMillis = 0;
        }
      }
    }

    @Override
    protected HttpMessage createMessage(String[] initialLine) throws Exception {
      return BasicHttpServerCodec.this.createMessage(initialLine, validateHeaders, _startTimeMillis, _startNanos);
    }

    @Override
    protected HttpMessage createInvalidMessage() {
      return BasicHttpServerCodec.this.createInvalidMessage(_startTimeMillis, _startNanos);
    }
  }

  protected class Encoder extends HttpResponseEncoder {
    @Override
    protected void encodeInitialLine(ByteBuf buf, HttpResponse msg) throws Exception {
      Transaction trans = _queue.peek();
      if (trans != null) {
        boolean verified = false;
        if (msg instanceof BasicHttpResponse) {
          BasicHttpResponse response = (BasicHttpResponse) msg;
          if (response.hasAttr(TRANSACTION_KEY)) {
            if (trans != response.attr(TRANSACTION_KEY).get()) {
              ReferenceCountUtil.release(buf);
              throw new ResponseOutOfSequence("expected " + trans + " but got " + response.attr(TRANSACTION_KEY).get());
            }
            verified = true;
          }
        }
        LOG.debug("verified transaction: {}", verified);
      }
      super.encodeInitialLine(buf, msg);
    }

    @Override
    protected boolean isContentAlwaysEmpty(HttpResponse msg) {
      if (msg.status().codeClass() != HttpStatusClass.INFORMATIONAL) {
        Transaction trans = _queue.poll();
        if (trans != null && HttpMethod.HEAD.equals(trans._method)) {
          return true;
        }
      }
      return super.isContentAlwaysEmpty(msg);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (cause instanceof ResponseOutOfSequence) {
      LOG.error("Out of sequence, closing channel to {}", ctx.channel().remoteAddress(), cause);
      ctx.close();
      return;
    }
    super.exceptionCaught(ctx, cause);
  }

  public static class ResponseOutOfSequence extends EncoderException {
    public ResponseOutOfSequence(String message) {
      super(message);
    }
  }

  private static class BadRequest extends BasicHttpRequest implements BadHttpRequest {
    BadRequest(long requestTimestamp, long requestNanos) {
      super(BAD_VERSION, HttpMethod.GET, "/bad-request", requestTimestamp, requestNanos);
      setDecoderResult(DecoderResult.failure(BAD_REQUEST));
    }
  }

  private static final HttpVersion BAD_VERSION = HttpVersion.valueOf("BAD/0.0");
  private static final DecoderException BAD_REQUEST =
      ExceptionUtil.withoutStackTrace(new DecoderException("bad request"));
}
