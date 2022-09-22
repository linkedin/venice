package com.linkedin.alpini.netty4.misc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.CombinedHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.IllegalReferenceCountException;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by acurtis on 4/19/17.
 */
public class BasicFullHttpResponse extends BasicHttpResponse implements FullHttpResponse {
  private static final Logger LOG = LogManager.getLogger(BasicFullHttpResponse.class);

  private final ByteBuf _content;
  protected HttpHeaders _trailingHeaders;

  public BasicFullHttpResponse(HttpRequest httpRequest, HttpResponseStatus status) {
    this(httpRequest, status, Unpooled.buffer(0));
  }

  public BasicFullHttpResponse(HttpRequest httpRequest, HttpResponseStatus status, ByteBuf content) {
    this(httpRequest, status, content, true);
  }

  public BasicFullHttpResponse(HttpRequest httpRequest, HttpResponseStatus status, boolean validateHeaders) {
    this(httpRequest, status, Unpooled.buffer(0), validateHeaders);
  }

  public BasicFullHttpResponse(
      HttpRequest httpRequest,
      HttpResponseStatus status,
      boolean validateHeaders,
      boolean singleFieldHeaders) {
    this(httpRequest, status, Unpooled.buffer(0), validateHeaders, singleFieldHeaders);
  }

  public BasicFullHttpResponse(
      HttpRequest httpRequest,
      HttpResponseStatus status,
      ByteBuf content,
      boolean validateHeaders) {
    this(httpRequest, status, content, validateHeaders, false);
  }

  public BasicFullHttpResponse(
      HttpRequest httpRequest,
      HttpResponseStatus status,
      ByteBuf content,
      boolean validateHeaders,
      boolean singleFieldHeaders) {
    super(httpRequest, status, validateHeaders, singleFieldHeaders);
    _content = Objects.requireNonNull(content, "content");
    _trailingHeaders =
        singleFieldHeaders ? new CombinedHttpHeaders(validateHeaders) : new DefaultHttpHeaders(validateHeaders);
  }

  public BasicFullHttpResponse(
      HttpRequest httpRequest,
      HttpResponseStatus status,
      ByteBuf content,
      HttpHeaders headers,
      HttpHeaders trailingHeaders) {
    super(httpRequest, status, headers);
    _content = Objects.requireNonNull(content, "content");
    _trailingHeaders = Objects.requireNonNull(trailingHeaders, "trailingHeaders");
  }

  public BasicFullHttpResponse(FullHttpResponse httpResponse) {
    this(httpResponse, httpResponse.headers(), httpResponse.trailingHeaders(), httpResponse.content());
  }

  public BasicFullHttpResponse(
      HttpResponse httpResponse,
      HttpHeaders headers,
      HttpHeaders trailingHeaders,
      ByteBuf content) {
    super(httpResponse, headers);
    _content = Objects.requireNonNull(content, "content");
    _trailingHeaders = Objects.requireNonNull(trailingHeaders, "trailingHeaders");
  }

  @Override
  public FullHttpResponse setProtocolVersion(HttpVersion version) {
    super.setProtocolVersion(version);
    return this;
  }

  @Override
  public FullHttpResponse setStatus(HttpResponseStatus status) {
    super.setStatus(status);
    return this;
  }

  @Override
  public HttpHeaders trailingHeaders() {
    return _trailingHeaders;
  }

  @Override
  public ByteBuf content() {
    return _content;
  }

  @Override
  public FullHttpResponse copy() {
    return replace(content().copy());
  }

  @Override
  public FullHttpResponse duplicate() {
    return replace(content().duplicate());
  }

  @Override
  public FullHttpResponse retainedDuplicate() {
    return replace(content().retainedDuplicate());
  }

  @Override
  public FullHttpResponse replace(ByteBuf content) {
    return new BasicFullHttpResponse(this, headers(), trailingHeaders(), Objects.requireNonNull(content, "content"));
  }

  @Override
  public FullHttpResponse retain(int increment) {
    _content.retain(increment);
    return this;
  }

  @Override
  public int refCnt() {
    return _content.refCnt();
  }

  @Override
  public FullHttpResponse retain() {
    _content.retain();
    return this;
  }

  @Override
  public FullHttpResponse touch() {
    _content.touch();
    return this;
  }

  @Override
  public FullHttpResponse touch(Object hint) {
    _content.touch(hint);
    return this;
  }

  private boolean quietRelease(int decrement) {
    boolean released = false;
    try {
      released = _content.release(decrement);
    } catch (IllegalReferenceCountException e) {
      // The IllegalReferenceCountException, if thrown, is by io.netty.buffer.AbstractReferenceCountedByteBuf.release0
      // when the refCnt originally 0, was set to less than 0 by getAndAdd(-1). The method, release0, would restore the
      // refCnt back to 0 and throw the exception.
      // We don't really care about the over-release as no leak is done. So instead of filling logs with the stack
      // trace, we should only print an info.
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Over-releasing or Overflow ReferenceCount occurred with IllegalReferenceCountException: {}",
            e.getMessage(),
            e);
      } else {
        LOG.info(
            "Over-releasing or Overflow ReferenceCount occurred with IllegalReferenceCountException: {}",
            e.getMessage());
      }
    }

    return released;
  }

  @Override
  public boolean release() {
    return quietRelease(1);
  }

  @Override
  public boolean release(int decrement) {
    return quietRelease(decrement);
  }

  @Override
  public String toString() {
    return HttpToStringUtils.appendFullResponse(new StringBuilder(256), this).toString();
  }
}
