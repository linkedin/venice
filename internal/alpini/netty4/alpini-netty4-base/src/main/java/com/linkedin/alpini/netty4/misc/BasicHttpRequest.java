package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.misc.BasicRequest;
import com.linkedin.alpini.base.misc.HeaderUtils;
import com.linkedin.alpini.base.misc.Headers;
import com.linkedin.alpini.base.misc.Time;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import io.netty.util.DefaultAttributeMap;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class BasicHttpRequest extends DefaultHttpRequest implements BasicRequest, AttributeMap, Cloneable {

  /* package */
  static final AtomicInteger SEQUENCE = new AtomicInteger();

  private final UUID _requestId;
  private final long _requestTimestamp;
  private final long _requestNanos;
  private transient Headers _headers;
  private AttributeMap _attributes;

  /**
   * Creates a new instance.
   *
   * @param httpVersion the HTTP version of the request
   * @param method      the HTTP method of the request
   * @param uri         the URI or path of the request
   */
  public BasicHttpRequest(HttpVersion httpVersion, HttpMethod method, String uri) {
    this(httpVersion, method, uri, Time.currentTimeMillis(), Time.nanoTime());
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof BasicHttpRequest && _requestTimestamp == ((BasicHttpRequest) o)._requestTimestamp
        && _requestNanos == ((BasicHttpRequest) o)._requestNanos && _requestId.equals(((BasicHttpRequest) o)._requestId)
        && super.equals(o);
  }

  /**
   * Creates a new instance.
   *
   * @param httpVersion the HTTP version of the request
   * @param method      the HTTP method of the request
   * @param uri         the URI or path of the request
   */
  public BasicHttpRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      long requestTimestamp,
      long requestNanos) {
    this(httpVersion, method, uri, true, requestTimestamp, requestNanos);
  }

  /**
   * Creates a new instance.
   *
   * @param httpVersion     the HTTP version of the request
   * @param method          the HTTP method of the request
   * @param uri             the URI or path of the request
   * @param validateHeaders validate the header names and values when adding them to the {@link HttpHeaders}
   */
  public BasicHttpRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      boolean validateHeaders,
      long requestTimestamp,
      long requestNanos) {
    this(
        httpVersion,
        method,
        uri,
        new DefaultHttpHeaders(validateHeaders),
        generateRequestId(method, uri, requestTimestamp, requestNanos),
        requestTimestamp,
        requestNanos);
  }

  /**
   * Creates a new instance.
   *
   * @param httpVersion the HTTP version of the request
   * @param method      the HTTP method of the request
   * @param uri         the URI or path of the request
   * @param headers     the Headers for this Request
   */
  public BasicHttpRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      HttpHeaders headers,
      UUID requestId,
      long requestTimestamp,
      long requestNanos) {
    super(httpVersion, method, uri, headers);
    _requestId = Objects.requireNonNull(requestId, "requestId");
    _requestTimestamp = requestTimestamp;
    _requestNanos = requestNanos;
  }

  public BasicHttpRequest(HttpRequest request) {
    this(request, request.headers());
  }

  public BasicHttpRequest(HttpRequest request, HttpHeaders headers) {
    this(request, request.method(), request.uri(), headers);
  }

  private BasicHttpRequest(HttpRequest request, HttpMethod method, String uri, HttpHeaders headers) {
    super(request.protocolVersion(), method, uri, headers);
    if (request instanceof BasicRequest) {
      BasicRequest basicRequest = (BasicRequest) request;
      _requestId = basicRequest.getRequestId();
      _requestTimestamp = basicRequest.getRequestTimestamp();
      _requestNanos = basicRequest.getRequestNanos();
    } else {
      _requestNanos = Time.nanoTime();
      _requestTimestamp = Time.currentTimeMillis();
      _requestId = generateRequestId(method, uri, _requestTimestamp, _requestNanos);
    }
    setAttributeMap(getAttibuteMap(request));
  }

  @Override
  public BasicHttpRequest clone() {
    try {
      BasicHttpRequest dup = (BasicHttpRequest) super.clone();
      return dup;
    } catch (CloneNotSupportedException e) {
      // should never occur because we implement Cloneable
      throw new Error(e);
    }
  }

  /**
   * Returns the computed request ID of the packet, which is intended to be reasonably unique.
   * @return an integer.
   */
  public final UUID getRequestId() {
    return _requestId;
  }

  @Override
  public String getMethodName() {
    return method().name();
  }

  /**
   * Returns the timestamp of the first packet of which formed this request object.
   * @return timestamp
   */
  public final long getRequestTimestamp() {
    return _requestTimestamp;
  }

  /**
   * Returns the nanotime of the first packet of which formed this request object.
   * @return nanotime
   */
  public final long getRequestNanos() {
    return _requestNanos;
  }

  @Override
  public long getRequestContentLength() {
    return HttpUtil.getContentLength(this, -1);
  }

  @Override
  public Headers getRequestHeaders() {
    if (_headers == null) {
      _headers = new BasicHeaders(headers());
    }
    return _headers;
  }

  protected static UUID generateRequestId(HttpMethod method, String uri, long requestTimestamp, long requestNanos) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeLong(requestTimestamp);
      oos.writeLong(requestNanos);
      oos.writeInt(SEQUENCE.incrementAndGet());
      oos.writeUTF(method.name());
      oos.writeByte(0);
      oos.writeUTF(uri);
    } catch (IOException ex) {
      return HeaderUtils.randomWeakUUID();
    }
    return HeaderUtils.nameUUIDFromBytes(baos.toByteArray());
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + _requestId.hashCode();
    result = 31 * result + (int) (_requestTimestamp ^ (_requestTimestamp >>> 32));
    result = 31 * result + (int) (_requestNanos ^ (_requestNanos >>> 32));
    return result;
  }

  static AttributeMap getAttibuteMap(HttpMessage httpMessage) {
    if (httpMessage instanceof BasicHttpRequest) {
      return ((BasicHttpRequest) httpMessage).attributeMap();
    }
    if (httpMessage instanceof BasicHttpResponse) {
      return ((BasicHttpResponse) httpMessage).attributeMap();
    }
    return null;
  }

  AttributeMap attributeMap() {
    if (_attributes == null) {
      _attributes = new DefaultAttributeMap();
    }
    return _attributes;
  }

  @SuppressWarnings("unchecked")
  public final void setAttributeMap(AttributeMap attributes) {
    if (_attributes != null && _attributes != attributes) {
      throw new IllegalStateException();
    }
    _attributes = attributes;
  }

  /**
   * Get the {@link Attribute} for the given {@link AttributeKey}. This method will never return null, but may return
   * an {@link Attribute} which does not have a value set yet.
   *
   * @param key
   */
  @Override
  public <T> Attribute<T> attr(AttributeKey<T> key) {
    return attributeMap().attr(key);
  }

  /**
   * Returns {@code} true if and only if the given {@link Attribute} exists in this {@link AttributeMap}.
   *
   * @param key
   */
  @Override
  public <T> boolean hasAttr(AttributeKey<T> key) {
    return _attributes != null && _attributes.hasAttr(key);
  }

  /**
   * Construct a full copy of the request.
   * @return copy of request
   */
  public BasicHttpRequest duplicate() {
    return new BasicHttpRequest(this, headers());
  }
}
