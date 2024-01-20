package com.linkedin.alpini.netty4.misc;

import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import io.netty.util.DefaultAttributeMap;


/**
 * Created by acurtis on 3/23/17.
 */
public class BasicHttpResponse extends DefaultHttpResponse implements AttributeMap {
  private AttributeMap _attributes;

  public BasicHttpResponse(HttpRequest httpRequest, HttpResponseStatus status) {
    super(httpRequest.protocolVersion(), status);
    _attributes = BasicHttpRequest.getAttibuteMap(httpRequest);
  }

  public BasicHttpResponse(HttpRequest httpRequest, HttpResponseStatus status, boolean validateHeaders) {
    super(httpRequest.protocolVersion(), status, validateHeaders);
    _attributes = BasicHttpRequest.getAttibuteMap(httpRequest);
  }

  public BasicHttpResponse(
      HttpRequest httpRequest,
      HttpResponseStatus status,
      boolean validateHeaders,
      boolean singleFieldHeaders) {
    super(httpRequest.protocolVersion(), status, validateHeaders, singleFieldHeaders);
    _attributes = BasicHttpRequest.getAttibuteMap(httpRequest);
  }

  public BasicHttpResponse(HttpRequest httpRequest, HttpResponseStatus status, HttpHeaders headers) {
    super(httpRequest.protocolVersion(), status, headers);
    _attributes = BasicHttpRequest.getAttibuteMap(httpRequest);
  }

  public BasicHttpResponse(HttpResponse httpResponse) {
    this(httpResponse, httpResponse.headers());
  }

  protected BasicHttpResponse(HttpResponse httpResponse, HttpHeaders headers) {
    super(httpResponse.protocolVersion(), httpResponse.status(), headers);
    _attributes = BasicHttpRequest.getAttibuteMap(httpResponse);
  }

  AttributeMap attributeMap() {
    if (_attributes == null) {
      _attributes = new DefaultAttributeMap();
    }
    return _attributes;
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

}
