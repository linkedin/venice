package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.misc.HeaderUtils;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Created by acurtis on 3/27/17.
 */
public class BasicFullHttpMultiPartRequest extends BasicHttpRequest implements FullHttpMultiPartRequest {
  private final HttpHeaders trailingHeader;

  private final Collection<FullHttpMultiPart> content;

  public BasicFullHttpMultiPartRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      long requestTimestamp,
      long requestNanos) {
    this(httpVersion, method, uri, Collections.emptyList(), requestTimestamp, requestNanos);
  }

  public BasicFullHttpMultiPartRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      Collection<FullHttpMultiPart> content,
      long requestTimestamp,
      long requestNanos) {
    this(httpVersion, method, uri, content, true, requestTimestamp, requestNanos);
  }

  public BasicFullHttpMultiPartRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      boolean validateHeaders,
      long requestTimestamp,
      long requestNanos) {
    this(httpVersion, method, uri, Collections.emptyList(), validateHeaders, requestTimestamp, requestNanos);
  }

  public BasicFullHttpMultiPartRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      Collection<FullHttpMultiPart> content,
      boolean validateHeaders,
      long requestTimestamp,
      long requestNanos) {
    super(httpVersion, method, uri, validateHeaders, requestTimestamp, requestNanos);
    this.content = Optional.of(Objects.requireNonNull(content, "content"))
        .filter(collection -> !collection.isEmpty())
        .map((Function<Collection<FullHttpMultiPart>, List<FullHttpMultiPart>>) ArrayList::new)
        .orElseGet(Collections::emptyList);
    trailingHeader = new DefaultHttpHeaders(validateHeaders);
    headers().set(HttpHeaderNames.CONTENT_TYPE, "multipart/mixed");
  }

  public BasicFullHttpMultiPartRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      Collection<FullHttpMultiPart> content,
      HttpHeaders headers,
      HttpHeaders trailingHeader,
      UUID requestId,
      long requestTimestamp,
      long requestNanos) {
    super(httpVersion, method, uri, headers, requestId, requestTimestamp, requestNanos);
    if (!HeaderUtils.parseContentType(headers.get(HttpHeaderNames.CONTENT_TYPE, "text/plain")).isMultipart()) {
      throw new IllegalArgumentException("Expected content-type to be multipart");
    }
    this.content = Optional.of(Objects.requireNonNull(content, "content"))
        .filter(collection -> !collection.isEmpty())
        .map((Function<Collection<FullHttpMultiPart>, List<FullHttpMultiPart>>) ArrayList::new)
        .orElseGet(Collections::emptyList);
    this.trailingHeader = Objects.requireNonNull(trailingHeader, "trailingHeader");
  }

  private BasicFullHttpMultiPartRequest(BasicFullHttpMultiPartRequest request, Collection<FullHttpMultiPart> content) {
    super(
        request.protocolVersion(),
        request.method(),
        request.uri(),
        request.headers(),
        request.getRequestId(),
        request.getRequestTimestamp(),
        request.getRequestNanos());
    this.content = Objects.requireNonNull(content, "content");
    this.trailingHeader = request.trailingHeaders();
  }

  @Override
  public HttpHeaders trailingHeaders() {
    return trailingHeader;
  }

  @Override
  public ByteBuf content() {
    return null;
  }

  @Override
  public BasicFullHttpMultiPartRequest copy() {
    return replace(content.stream().map(FullHttpMultiPart::copy).collect(Collectors.toList()));
  }

  @Override
  public BasicFullHttpMultiPartRequest duplicate() {
    return replace(content.stream().map(FullHttpMultiPart::duplicate).collect(Collectors.toList()));
  }

  @Override
  public BasicFullHttpMultiPartRequest retainedDuplicate() {
    return replace(content.stream().map(FullHttpMultiPart::retainedDuplicate).collect(Collectors.toList()));
  }

  @Override
  public BasicFullHttpMultiPartRequest replace(ByteBuf content) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BasicFullHttpMultiPartRequest replace(Collection<FullHttpMultiPart> parts) {
    return new BasicFullHttpMultiPartRequest(this, parts);
  }

  @Override
  public BasicFullHttpMultiPartRequest retain(int increment) {
    content.forEach(part -> part.retain(increment));
    return this;
  }

  @Override
  public int refCnt() {
    return content.stream().mapToInt(FullHttpMultiPart::refCnt).min().orElse(1);
  }

  @Override
  public BasicFullHttpMultiPartRequest retain() {
    content.forEach(FullHttpMultiPart::retain);
    return this;
  }

  @Override
  public BasicFullHttpMultiPartRequest touch() {
    content.forEach(FullHttpMultiPart::touch);
    return this;
  }

  @Override
  public BasicFullHttpMultiPartRequest touch(Object hint) {
    content.forEach(part -> part.touch(hint));
    return this;
  }

  @Override
  public boolean release() {
    return content.stream().map(FullHttpMultiPart::release).filter(Boolean.TRUE::equals).count() > 0;
  }

  @Override
  public boolean release(int decrement) {
    return content.stream().map(part -> part.release(decrement)).filter(Boolean.TRUE::equals).count() > 0;
  }

  @Override
  public Iterator<FullHttpMultiPart> iterator() {
    return Collections.unmodifiableCollection(content).iterator();
  }

  @Override
  public BasicFullHttpMultiPartRequest setMethod(HttpMethod method) {
    super.setMethod(method);
    return this;
  }

  @Override
  public BasicFullHttpMultiPartRequest setUri(String uri) {
    super.setUri(uri);
    return this;
  }

  @Override
  public BasicFullHttpMultiPartRequest setProtocolVersion(HttpVersion version) {
    super.setProtocolVersion(version);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BasicFullHttpMultiPartRequest)) {
      return false;
    }

    if (!super.equals(o)) {
      return false;
    }

    BasicFullHttpMultiPartRequest that = (BasicFullHttpMultiPartRequest) o;

    if (!trailingHeader.equals(that.trailingHeader)) {
      return false;
    }
    return content.equals(that.content);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + trailingHeader.hashCode();
    result = 31 * result + content.hashCode();
    return result;
  }
}
