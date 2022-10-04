package com.linkedin.alpini.netty4.misc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.IllegalReferenceCountException;
import java.util.Objects;
import java.util.UUID;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class BasicFullHttpRequest extends BasicHttpRequest implements FullHttpRequest {
  private ByteBuf _content;
  protected HttpHeaders _trailingHeaders;

  /**
   * Used to cache the value of the hash code and avoid {@link IllegalReferenceCountException}.
   */
  private int hash;

  public BasicFullHttpRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      long requestTimestamp,
      long requestNanos) {
    this(httpVersion, method, uri, Unpooled.buffer(0), requestTimestamp, requestNanos);
  }

  public BasicFullHttpRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      ByteBuf content,
      long requestTimestamp,
      long requestNanos) {
    this(httpVersion, method, uri, content, true, requestTimestamp, requestNanos);
  }

  public BasicFullHttpRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      boolean validateHeaders,
      long requestTimestamp,
      long requestNanos) {
    this(httpVersion, method, uri, Unpooled.buffer(0), validateHeaders, requestTimestamp, requestNanos);
  }

  public BasicFullHttpRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      ByteBuf content,
      boolean validateHeaders,
      long requestTimestamp,
      long requestNanos) {
    super(httpVersion, method, uri, validateHeaders, requestTimestamp, requestNanos);
    _content = Objects.requireNonNull(content, "content");
    _trailingHeaders = new DefaultHttpHeaders(validateHeaders);
  }

  public BasicFullHttpRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      ByteBuf content,
      HttpHeaders headers,
      HttpHeaders trailingHeader,
      UUID requestId,
      long requestTimestamp,
      long requestNanos) {
    super(httpVersion, method, uri, headers, requestId, requestTimestamp, requestNanos);
    _content = Objects.requireNonNull(content, "content");
    _trailingHeaders = Objects.requireNonNull(trailingHeader, "trailingHeader");
  }

  public BasicFullHttpRequest(FullHttpRequest request) {
    this(request, request.headers(), request.trailingHeaders(), request.content());
  }

  public BasicFullHttpRequest(HttpRequest request, HttpHeaders headers, HttpHeaders trailingHeader, ByteBuf content) {
    super(request, headers);
    _content = Objects.requireNonNull(content, "content");
    _trailingHeaders = Objects.requireNonNull(trailingHeader, "trailingHeader");
  }

  @Override
  public BasicFullHttpRequest clone() {
    BasicFullHttpRequest dup = (BasicFullHttpRequest) super.clone();
    dup._content = content().duplicate();
    return dup;
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
  public int refCnt() {
    return _content.refCnt();
  }

  @Override
  public BasicFullHttpRequest retain() {
    _content.retain();
    return this;
  }

  @Override
  public BasicFullHttpRequest retain(int increment) {
    _content.retain(increment);
    return this;
  }

  @Override
  public BasicFullHttpRequest touch() {
    _content.touch();
    return this;
  }

  @Override
  public FullHttpRequest touch(Object hint) {
    _content.touch(hint);
    return this;
  }

  @Override
  public boolean release() {
    return _content.release();
  }

  @Override
  public boolean release(int decrement) {
    return _content.release(decrement);
  }

  @Override
  public BasicFullHttpRequest setProtocolVersion(HttpVersion version) {
    super.setProtocolVersion(version);
    return this;
  }

  @Override
  public BasicFullHttpRequest setMethod(HttpMethod method) {
    super.setMethod(method);
    return this;
  }

  @Override
  public BasicFullHttpRequest setUri(String uri) {
    super.setUri(uri);
    return this;
  }

  @Override
  public BasicFullHttpRequest copy() {
    return replace(content().copy());
  }

  @Override
  public BasicFullHttpRequest duplicate() {
    return replace(content().duplicate());
  }

  @Override
  public BasicFullHttpRequest retainedDuplicate() {
    return replace(content().retainedDuplicate());
  }

  @Override
  public BasicFullHttpRequest replace(ByteBuf content) {
    return new BasicFullHttpRequest(this, headers(), trailingHeaders(), content);
  }

  @Override
  public int hashCode() {
    int hash = this.hash;
    if (hash == 0) {
      if (content().refCnt() != 0) {
        try {
          hash = 31 + content().hashCode();
        } catch (IllegalReferenceCountException ignored) {
          // Handle race condition between checking refCnt() == 0 and using the object.
          hash = 31;
        }
      } else {
        hash = 31;
      }
      hash = 31 * hash + trailingHeaders().hashCode();
      hash = 31 * hash + super.hashCode();
      this.hash = hash;
    }
    return hash;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof BasicFullHttpRequest)) {
      return false;
    }

    BasicFullHttpRequest other = (BasicFullHttpRequest) o;

    return super.equals(other) && ByteBufUtil.compare(content(), other.content()) == 0
        && trailingHeaders().equals(other.trailingHeaders());
  }

  @Override
  public String toString() {
    return HttpToStringUtils.appendFullRequest(new StringBuilder(256), this).toString();
  }
}
