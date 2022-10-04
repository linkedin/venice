package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.CombinedChannelDuplexHandler;
import io.netty.handler.codec.PrematureChannelClosureException;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpClientUpgradeHandler;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;


/**
 * A combination of {@link HttpRequestEncoder} and {@link BasicHttpResponseDecoder}
 * which enables easier client side HTTP implementation. {@link io.netty.handler.codec.http.HttpClientCodec}
 * provides additional state management for <tt>HEAD</tt> and <tt>CONNECT</tt>
 * requests, which {@link BasicHttpResponseDecoder} lacks.  Please refer to
 * {@link BasicHttpResponseDecoder} to learn what additional state management needs
 * to be done for <tt>HEAD</tt> and <tt>CONNECT</tt> and why
 * {@link BasicHttpResponseDecoder} can not handle it by itself.
 *
 * If the {@link io.netty.channel.Channel} is closed and there are missing responses,
 * a {@link PrematureChannelClosureException} is thrown.
 *
 * @see BasicHttpServerCodec
 */
public final class BasicHttpClientCodec
    extends CombinedChannelDuplexHandler<BasicHttpResponseDecoder, HttpRequestEncoder>
    implements HttpClientUpgradeHandler.SourceCodec {
  /** A queue that is used for correlating a request and a response. */
  private final Queue<HttpRequest> queue = new ArrayDeque<>();
  private final boolean parseHttpAfterConnectRequest;

  /** If true, decoding stops (i.e. pass-through) */
  private boolean done;

  private final AtomicLong requestResponseCounter = new AtomicLong();
  private final boolean failOnMissingResponse;

  /**
   * Creates a new instance with the default decoder options
   * ({@code maxInitialLineLength (4096}}, {@code maxHeaderSize (8192)}, and
   * {@code maxChunkSize (8192)}).
   */
  public BasicHttpClientCodec() {
    this(4096, 8192, 8192, false);
  }

  /**
   * Creates a new instance with the specified decoder options.
   */
  public BasicHttpClientCodec(int maxInitialLineLength, int maxHeaderSize, int maxChunkSize) {
    this(maxInitialLineLength, maxHeaderSize, maxChunkSize, false);
  }

  /**
   * Creates a new instance with the specified decoder options.
   */
  public BasicHttpClientCodec(
      int maxInitialLineLength,
      int maxHeaderSize,
      int maxChunkSize,
      boolean failOnMissingResponse) {
    this(maxInitialLineLength, maxHeaderSize, maxChunkSize, failOnMissingResponse, true);
  }

  /**
   * Creates a new instance with the specified decoder options.
   */
  public BasicHttpClientCodec(
      int maxInitialLineLength,
      int maxHeaderSize,
      int maxChunkSize,
      boolean failOnMissingResponse,
      boolean validateHeaders) {
    this(maxInitialLineLength, maxHeaderSize, maxChunkSize, failOnMissingResponse, validateHeaders, false);
  }

  /**
   * Creates a new instance with the specified decoder options.
   */
  public BasicHttpClientCodec(
      int maxInitialLineLength,
      int maxHeaderSize,
      int maxChunkSize,
      boolean failOnMissingResponse,
      boolean validateHeaders,
      boolean parseHttpAfterConnectRequest) {
    init(new Decoder(maxInitialLineLength, maxHeaderSize, maxChunkSize, validateHeaders), new Encoder());
    this.failOnMissingResponse = failOnMissingResponse;
    this.parseHttpAfterConnectRequest = parseHttpAfterConnectRequest;
  }

  /**
   * Creates a new instance with the specified decoder options.
   */
  public BasicHttpClientCodec(
      int maxInitialLineLength,
      int maxHeaderSize,
      int maxChunkSize,
      boolean failOnMissingResponse,
      boolean validateHeaders,
      int initialBufferSize) {
    this(
        maxInitialLineLength,
        maxHeaderSize,
        maxChunkSize,
        failOnMissingResponse,
        validateHeaders,
        initialBufferSize,
        false);
  }

  /**
   * Creates a new instance with the specified decoder options.
   */
  public BasicHttpClientCodec(
      int maxInitialLineLength,
      int maxHeaderSize,
      int maxChunkSize,
      boolean failOnMissingResponse,
      boolean validateHeaders,
      int initialBufferSize,
      boolean parseHttpAfterConnectRequest) {
    init(
        new Decoder(maxInitialLineLength, maxHeaderSize, maxChunkSize, validateHeaders, initialBufferSize),
        new Encoder());
    this.parseHttpAfterConnectRequest = parseHttpAfterConnectRequest;
    this.failOnMissingResponse = failOnMissingResponse;
  }

  /**
   * Prepares to upgrade to another protocol from HTTP. Disables the {@link HttpRequestEncoder}.
   */
  @Override
  public void prepareUpgradeFrom(ChannelHandlerContext ctx) {
    ((Encoder) outboundHandler()).upgraded = true;
  }

  /**
   * Upgrades to another protocol from HTTP. Removes the {@linkplain BasicHttpResponseDecoder} and {@link HttpRequestEncoder} from
   * the pipeline.
   */
  @Override
  public void upgradeFrom(ChannelHandlerContext ctx) {
    final ChannelPipeline p = ctx.pipeline();
    p.remove(this);
  }

  public void setSingleDecode(boolean singleDecode) {
    inboundHandler().setSingleDecode(singleDecode);
  }

  public boolean isSingleDecode() {
    return inboundHandler().isSingleDecode();
  }

  private final class Encoder extends HttpRequestEncoder {
    boolean upgraded;

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {

      if (upgraded) {
        out.add(ReferenceCountUtil.retain(msg));
        return;
      }

      if (msg instanceof HttpRequest && !done) {
        HttpRequest request = (HttpRequest) msg;
        queue.offer(msg instanceof FullHttpMessage ? new BasicHttpRequest(request) : request);
      }

      super.encode(ctx, msg, out);

      if (failOnMissingResponse && !done) {
        // check if the request is chunked if so do not increment
        if (msg instanceof LastHttpContent) {
          // increment as its the last chunk
          requestResponseCounter.incrementAndGet();
        }
      }
    }
  }

  private final class Decoder extends BasicHttpResponseDecoder {
    final HttpRequest _dummyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    HttpRequest _lastRequest;

    Decoder(int maxInitialLineLength, int maxHeaderSize, int maxChunkSize, boolean validateHeaders) {
      super(maxInitialLineLength, maxHeaderSize, maxChunkSize, validateHeaders);
    }

    Decoder(
        int maxInitialLineLength,
        int maxHeaderSize,
        int maxChunkSize,
        boolean validateHeaders,
        int initialBufferSize) {
      super(maxInitialLineLength, maxHeaderSize, maxChunkSize, validateHeaders, initialBufferSize);
    }

    @Override
    protected HttpRequest lastRequest() {
      if (_lastRequest != null) {
        return _lastRequest;
      } else {
        return _dummyRequest;
      }
    }

    @Override
    protected HttpResponse checkLastResponse(HttpResponse response) {
      if (response.status().code() >= 200) {
        _lastRequest = null;
      }
      return response;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
      if (done) {
        int readable = actualReadableBytes();
        if (readable == 0) {
          // if non is readable just return null
          // https://github.com/netty/netty/issues/1159
          return;
        }
        out.add(buffer.readBytes(readable));
      } else {
        int oldSize = out.size();
        super.decode(ctx, buffer, out);
        if (failOnMissingResponse) {
          int size = out.size();
          for (int i = oldSize; i < size; i++) {
            decrement(out.get(i));
          }
        }
      }
    }

    private void decrement(Object msg) {
      if (msg == null) {
        return;
      }

      // check if it's an Header and its transfer encoding is not chunked.
      if (msg instanceof LastHttpContent) {
        requestResponseCounter.decrementAndGet();
      }
    }

    @Override
    protected boolean isContentAlwaysEmpty(HttpMessage msg) {
      final int statusCode = ((HttpResponse) msg).status().code();
      if (statusCode == 100 || statusCode == 101) {
        // 100-continue and 101 switching protocols response should be excluded from paired comparison.
        // Just delegate to super method which has all the needed handling.
        return super.isContentAlwaysEmpty(msg);
      }

      // Get the getMethod of the HTTP request that corresponds to the
      // current response.
      _lastRequest = queue.poll();

      char firstChar = _lastRequest != null ? _lastRequest.method().name().charAt(0) : '?';
      switch (firstChar) {
        case 'H':
          // According to 4.3, RFC2616:
          // All responses to the HEAD request method MUST NOT include a
          // message-body, even though the presence of entity-header fields
          // might lead one to believe they do.
          if (HttpMethod.HEAD.equals(_lastRequest.method())) {
            return true;

            // The following code was inserted to work around the servers
            // that behave incorrectly. It has been commented out
            // because it does not work with well behaving servers.
            // Please note, even if the 'Transfer-Encoding: chunked'
            // header exists in the HEAD response, the response should
            // have absolutely no content.
            //
            //// Interesting edge case:
            //// Some poorly implemented servers will send a zero-byte
            //// chunk if Transfer-Encoding of the response is 'chunked'.
            ////
            //// return !msg.isChunked();
          }
          break;
        case 'C':
          // Successful CONNECT request results in a response with empty body.
          if (statusCode == 200) {
            if (HttpMethod.CONNECT.equals(_lastRequest.method())) {
              // Proxy connection established - Parse HTTP only if configured by parseHttpAfterConnectRequest,
              // else pass through.
              if (!parseHttpAfterConnectRequest) {
                done = true;
                queue.clear();
              }
              return true;
            }
          }
          break;
        default:
          break;
      }

      return super.isContentAlwaysEmpty(msg);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      super.channelInactive(ctx);

      if (failOnMissingResponse) {
        long missingResponses = requestResponseCounter.get();
        if (missingResponses > 0) {
          ctx.fireExceptionCaught(
              new PrematureChannelClosureException(
                  "channel gone inactive with " + missingResponses + " missing response(s)"));
        }
      }
    }
  }
}
