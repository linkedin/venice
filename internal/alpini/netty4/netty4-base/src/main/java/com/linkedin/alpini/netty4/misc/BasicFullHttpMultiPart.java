package com.linkedin.alpini.netty4.misc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.util.AttributeMap;
import io.netty.util.IllegalReferenceCountException;
import java.util.Objects;


/**
 * Created by acurtis on 3/22/17.
 */
public class BasicFullHttpMultiPart extends BasicHttpMultiPart implements FullHttpMultiPart, MultipartContent {
  private final ByteBuf content;

  /**
   * Used to cache the value of the hash code and avoid {@link IllegalReferenceCountException}.
   */
  private int hash;

  public BasicFullHttpMultiPart() {
    this(Unpooled.buffer(0));
  }

  public BasicFullHttpMultiPart(ByteBuf content) {
    this(content, true);
  }

  public BasicFullHttpMultiPart(boolean validateHeaders) {
    this(Unpooled.buffer(0), validateHeaders, false);
  }

  public BasicFullHttpMultiPart(boolean validateHeaders, boolean singleFieldHeaders) {
    this(Unpooled.buffer(0), validateHeaders, singleFieldHeaders);
  }

  public BasicFullHttpMultiPart(ByteBuf content, boolean validateHeaders) {
    this(content, validateHeaders, false);
  }

  public BasicFullHttpMultiPart(ByteBuf content, boolean validateHeaders, boolean singleFieldHeaders) {
    super(validateHeaders, singleFieldHeaders);
    this.content = Objects.requireNonNull(content, "content");
  }

  public BasicFullHttpMultiPart(ByteBuf content, HttpHeaders headers) {
    super(headers);
    this.content = Objects.requireNonNull(content, "content");
  }

  public BasicFullHttpMultiPart(HttpMessage httpMessage) {
    this(httpMessage, Unpooled.buffer(0));
  }

  public BasicFullHttpMultiPart(HttpMessage httpMessage, ByteBuf content) {
    this(httpMessage, content, true);
  }

  public BasicFullHttpMultiPart(HttpMessage httpMessage, boolean validateHeaders) {
    this(httpMessage, Unpooled.buffer(0), validateHeaders, false);
  }

  public BasicFullHttpMultiPart(HttpMessage httpMessage, boolean validateHeaders, boolean singleFieldHeaders) {
    this(httpMessage, Unpooled.buffer(0), validateHeaders, singleFieldHeaders);
  }

  public BasicFullHttpMultiPart(HttpMessage httpMessage, ByteBuf content, boolean validateHeaders) {
    this(httpMessage, content, validateHeaders, false);
  }

  public BasicFullHttpMultiPart(
      HttpMessage httpMessage,
      ByteBuf content,
      boolean validateHeaders,
      boolean singleFieldHeaders) {
    super(httpMessage, validateHeaders, singleFieldHeaders);
    this.content = Objects.requireNonNull(content, "content");
  }

  public BasicFullHttpMultiPart(HttpMessage httpMessage, ByteBuf content, HttpHeaders headers) {
    super(httpMessage, headers);
    this.content = Objects.requireNonNull(content, "content");
  }

  protected BasicFullHttpMultiPart(AttributeMap attributes, ByteBuf content, HttpHeaders headers) {
    super(attributes, headers);
    this.content = Objects.requireNonNull(content, "content");
  }

  @Override
  public ByteBuf content() {
    return content;
  }

  @Override
  public int refCnt() {
    return content.refCnt();
  }

  @Override
  public BasicFullHttpMultiPart retain() {
    content.retain();
    return this;
  }

  @Override
  public BasicFullHttpMultiPart retain(int increment) {
    content.retain(increment);
    return this;
  }

  @Override
  public BasicFullHttpMultiPart touch() {
    content.touch();
    return this;
  }

  @Override
  public BasicFullHttpMultiPart touch(Object hint) {
    content.touch(hint);
    return this;
  }

  @Override
  public boolean release() {
    return content.release();
  }

  @Override
  public boolean release(int decrement) {
    return content.release(decrement);
  }

  @Override
  public BasicFullHttpMultiPart copy() {
    return replace(content().copy());
  }

  @Override
  public BasicFullHttpMultiPart duplicate() {
    return replace(content().duplicate());
  }

  @Override
  public BasicFullHttpMultiPart retainedDuplicate() {
    return replace(content().retainedDuplicate());
  }

  @Override
  public BasicFullHttpMultiPart replace(ByteBuf content) {
    return new BasicFullHttpMultiPart(_attributes, content, headers());
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
      hash = 31 * hash + super.hashCode();
      this.hash = hash;
    }
    return hash;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof BasicFullHttpMultiPart)) {
      return false;
    }

    BasicFullHttpMultiPart other = (BasicFullHttpMultiPart) o;

    return super.equals(other) && content().equals(other.content());
  }
}
