package com.linkedin.alpini.netty4.misc;

import io.netty.handler.codec.http.DefaultHttpMessage;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import io.netty.util.DefaultAttributeMap;


/**
 * Created by acurtis on 3/22/17.
 */
public class BasicHttpMultiPart extends DefaultHttpMessage implements HttpMultiPart, AttributeMap {
  AttributeMap _attributes;

  public BasicHttpMultiPart() {
    this((HttpMessage) null);
  }

  public BasicHttpMultiPart(boolean validateHeaders, boolean singleFieldHeaders) {
    this(null, validateHeaders, singleFieldHeaders);
  }

  public BasicHttpMultiPart(HttpHeaders headers) {
    this((HttpMessage) null, headers);
  }

  public BasicHttpMultiPart(HttpMessage httpMessage) {
    super(HttpVersion.HTTP_1_0);
    _attributes = BasicHttpRequest.getAttibuteMap(httpMessage);
  }

  public BasicHttpMultiPart(HttpMessage httpMessage, boolean validateHeaders, boolean singleFieldHeaders) {
    super(HttpVersion.HTTP_1_0, validateHeaders, singleFieldHeaders);
    _attributes = BasicHttpRequest.getAttibuteMap(httpMessage);
  }

  public BasicHttpMultiPart(HttpMessage httpMessage, HttpHeaders headers) {
    this(BasicHttpRequest.getAttibuteMap(httpMessage), headers);
  }

  BasicHttpMultiPart(AttributeMap attributes, HttpHeaders headers) {
    super(HttpVersion.HTTP_1_0, headers);
    _attributes = attributes;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof BasicHttpMultiPart)) {
      return false;
    }
    return super.equals(o);
  }

  @Override
  public String toString() {
    return HttpToStringUtils
        .removeLastNewLine(
            HttpToStringUtils.appendHeaders(HttpToStringUtils.appendCommon(new StringBuilder(256), this), headers()))
        .toString();
  }

  /**
   * Get the {@link Attribute} for the given {@link AttributeKey}. This method will never return null, but may return
   * an {@link Attribute} which does not have a value set yet.
   *
   * @param key
   */
  @Override
  public <T> Attribute<T> attr(AttributeKey<T> key) {
    if (_attributes == null) {
      _attributes = new DefaultAttributeMap();
    }
    return _attributes.attr(key);
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
