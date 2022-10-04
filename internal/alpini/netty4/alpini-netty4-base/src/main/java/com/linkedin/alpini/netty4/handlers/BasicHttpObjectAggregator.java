package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicFullHttpResponse;
import com.linkedin.alpini.netty4.misc.HttpUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class BasicHttpObjectAggregator extends HttpObjectAggregator {
  private static final Logger LOG = LogManager.getLogger(BasicHttpObjectAggregator.class);

  /**
   * Creates a new instance.
   *
   * @param maxContentLength the maximum length of the aggregated content in bytes.
   *                         If the length of the aggregated content exceeds this value,
   *                         {@link #handleOversizedMessage(io.netty.channel.ChannelHandlerContext, HttpMessage)} will be called.
   */
  public BasicHttpObjectAggregator(int maxContentLength) {
    super(maxContentLength);
  }

  /**
   * Creates a new instance.
   *
   * @param maxContentLength         the maximum length of the aggregated content in bytes.
   *                                 If the length of the aggregated content exceeds this value,
   *                                 {@link #handleOversizedMessage(io.netty.channel.ChannelHandlerContext, HttpMessage)} will be called.
   * @param closeOnExpectationFailed If a 100-continue response is detected but the content length is too large
   *                                 then {@code true} means close the connection. otherwise the connection will remain open and data will be
   */
  public BasicHttpObjectAggregator(int maxContentLength, boolean closeOnExpectationFailed) {
    super(maxContentLength, closeOnExpectationFailed);
  }

  @Override
  protected Object newContinueResponse(HttpMessage start, int maxContentLength, ChannelPipeline pipeline) {
    Object obj = super.newContinueResponse(start, maxContentLength, pipeline);
    if (obj instanceof FullHttpResponse) {
      FullHttpResponse response = (FullHttpResponse) obj;
      obj = new BasicFullHttpResponse(
          (HttpRequest) start,
          response.status(),
          response.content(),
          response.headers(),
          response.trailingHeaders());
    }
    return obj;
  }

  @Override
  protected void handleOversizedMessage(final ChannelHandlerContext ctx, HttpMessage oversized) throws Exception {
    if (oversized instanceof HttpRequest) {
      HttpRequest request = (HttpRequest) oversized;
      LOG.warn(
          "handleOversizedMessage {} {} {} ({} > {})",
          ctx.channel().remoteAddress(),
          request.method(),
          request.uri(),
          request.headers().get(HttpHeaderNames.CONTENT_LENGTH),
          maxContentLength());
    }
    super.handleOversizedMessage(ctx, oversized);
  }

  @Override
  protected FullHttpMessage beginAggregation(HttpMessage start, ByteBuf content) throws Exception {
    assert !(start instanceof FullHttpMessage);

    HttpUtil.setTransferEncodingChunked(start, false);

    if (start instanceof HttpRequest) {
      HttpRequest request = (HttpRequest) start;
      return new AggregatedFullHttpRequest(request, content);
    } else if (start instanceof HttpResponse) {
      HttpResponse response = (HttpResponse) start;
      return new AggregatedFullHttpResponse(response, content);
    } else {
      throw new Error();
    }
  }

  @Override
  protected void aggregate(FullHttpMessage aggregated, HttpContent content) throws Exception {
    if (content instanceof LastHttpContent) {
      if (aggregated instanceof AggregatedMessage) {
        // Merge trailing headers into the message.
        ((AggregatedMessage) aggregated).setTrailingHeaders(((LastHttpContent) content).trailingHeaders());
      } else {
        super.aggregate(aggregated, content);
      }
    }
  }

  interface AggregatedMessage extends FullHttpMessage {
    void setTrailingHeaders(HttpHeaders trailingHeaders);
  }

  private static class AggregatedFullHttpRequest extends BasicFullHttpRequest implements AggregatedMessage {
    private final int _hashCode = ThreadLocalRandom.current().nextInt();
    private final HttpRequest _request;

    public AggregatedFullHttpRequest(HttpRequest request, ByteBuf content) {
      super(request, request.headers(), LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders(), content);
      _request = request;
      setDecoderResult(request.decoderResult());
    }

    private AggregatedFullHttpRequest(
        HttpRequest request,
        ByteBuf content,
        HttpHeaders headers,
        HttpHeaders trailingHeaders) {
      super(request, headers, trailingHeaders, content);
      _request = request;
    }

    @Override
    public AggregatedFullHttpRequest replace(ByteBuf content) {
      AggregatedFullHttpRequest dup = new AggregatedFullHttpRequest(
          _request,
          content,
          HttpUtils.copy(headers()),
          HttpUtils.copy(trailingHeaders()));
      dup.setDecoderResult(decoderResult());
      return dup;
    }

    @Override
    public AggregatedFullHttpRequest setMethod(HttpMethod method) {
      _request.setMethod(method);
      super.setMethod(method);
      return this;
    }

    @Override
    public AggregatedFullHttpRequest setUri(String uri) {
      _request.setUri(uri);
      super.setUri(uri);
      return this;
    }

    @Override
    public HttpMethod method() {
      return _request.method();
    }

    @Override
    public String uri() {
      return _request.uri();
    }

    public void setTrailingHeaders(HttpHeaders trailingHeaders) {
      _trailingHeaders = trailingHeaders;
    }

    @Override
    public DecoderResult decoderResult() {
      DecoderResult decoderResult = super.decoderResult();
      return decoderResult.isSuccess() ? _request.decoderResult() : decoderResult;
    }

    @Override
    public boolean equals(Object o) {
      return this == o;
    }

    @Override
    public int hashCode() {
      return _hashCode;
    }
  }

  private static class AggregatedFullHttpResponse extends BasicFullHttpResponse implements AggregatedMessage {
    private final int _hashCode = ThreadLocalRandom.current().nextInt();
    private final HttpResponse _response;

    public AggregatedFullHttpResponse(HttpResponse response, ByteBuf content) {
      super(response, response.headers(), LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders(), content);
      _response = response;
      setDecoderResult(response.decoderResult());
    }

    private AggregatedFullHttpResponse(
        HttpResponse response,
        ByteBuf content,
        HttpHeaders headers,
        HttpHeaders trailingHeaders) {
      super(response, headers, trailingHeaders, content);
      _response = response;
    }

    @Override
    public AggregatedFullHttpResponse replace(ByteBuf content) {
      AggregatedFullHttpResponse dup = new AggregatedFullHttpResponse(
          _response,
          content,
          HttpUtils.copy(headers()),
          HttpUtils.copy(trailingHeaders()));
      dup.setDecoderResult(decoderResult());
      return dup;
    }

    @Override
    public FullHttpResponse setStatus(HttpResponseStatus status) {
      _response.setStatus(status);
      return super.setStatus(status);
    }

    @Override
    public HttpResponseStatus status() {
      return _response.status();
    }

    public void setTrailingHeaders(HttpHeaders trailingHeaders) {
      _trailingHeaders = trailingHeaders;
    }

    @Override
    public DecoderResult decoderResult() {
      DecoderResult decoderResult = super.decoderResult();
      return decoderResult.isSuccess() ? _response.decoderResult() : decoderResult;
    }

    @Override
    public boolean equals(Object o) {
      return this == o;
    }

    @Override
    public int hashCode() {
      return _hashCode;
    }
  }
}
