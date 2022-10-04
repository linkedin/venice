package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.netty4.misc.BasicFullHttpMultiPart;
import com.linkedin.alpini.netty4.misc.FullHttpMultiPart;
import com.linkedin.alpini.netty4.misc.HttpToStringUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.MessageAggregator;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;


/**
 * This _only_ knows how to aggregate HttpMultiPart messages and "bad things"(tm) will occur when it encounters other
 * types of messages.
 *
 * Created by acurtis on 3/22/17.
 */
public class HttpMultiPartContentAggregator
    extends MessageAggregator<HttpObject, HttpMessage, HttpContent, FullHttpMultiPart> {
  public HttpMultiPartContentAggregator(int maxContentLength) {
    super(maxContentLength);
  }

  @Override
  public boolean acceptInboundMessage(Object msg) throws Exception {
    return /*!(msg instanceof LastHttpContent) && */ super.acceptInboundMessage(msg);
  }

  @Override
  protected boolean isStartMessage(HttpObject msg) throws Exception {
    return msg instanceof HttpMessage;
  }

  @Override
  protected boolean isContentMessage(HttpObject msg) throws Exception {
    return msg instanceof HttpContent;
  }

  @Override
  protected boolean isLastContentMessage(HttpContent msg) throws Exception {
    return msg instanceof LastHttpContent;
  }

  @Override
  protected boolean isAggregated(HttpObject msg) throws Exception {
    return msg instanceof FullHttpMultiPart;
  }

  @Override
  protected boolean isContentLengthInvalid(HttpMessage start, int maxContentLength) throws Exception {
    return false;
  }

  @Override
  protected Object newContinueResponse(HttpMessage start, int maxContentLength, ChannelPipeline pipeline)
      throws Exception {
    return null;
  }

  @Override
  protected boolean closeAfterContinueResponse(Object msg) throws Exception {
    return false;
  }

  @Override
  protected boolean ignoreContentAfterContinueResponse(Object msg) throws Exception {
    return false;
  }

  @Override
  protected FullHttpMultiPart beginAggregation(HttpMessage start, ByteBuf content) throws Exception {
    assert !(start instanceof FullHttpMessage);
    return new AggregatedFullHttpMultiPart(start, content);
  }

  @Override
  protected void handleOversizedMessage(ChannelHandlerContext ctx, HttpMessage oversized) throws Exception {
    super.handleOversizedMessage(ctx, oversized);
  }

  private static class AggregatedFullHttpMultiPart implements FullHttpMultiPart, AttributeMap {
    protected final HttpMessage message;
    private final ByteBuf content;

    AggregatedFullHttpMultiPart(HttpMessage message, ByteBuf content) {
      this.message = message;
      this.content = content;
    }

    @Override
    public HttpVersion getProtocolVersion() {
      return message.protocolVersion();
    }

    @Override
    public HttpVersion protocolVersion() {
      return message.protocolVersion();
    }

    @Override
    public FullHttpMultiPart setProtocolVersion(HttpVersion version) {
      message.setProtocolVersion(version);
      return this;
    }

    @Override
    public HttpHeaders headers() {
      return message.headers();
    }

    @Override
    public DecoderResult decoderResult() {
      return message.decoderResult();
    }

    @Override
    public DecoderResult getDecoderResult() {
      return message.decoderResult();
    }

    @Override
    public void setDecoderResult(DecoderResult result) {
      message.setDecoderResult(result);
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
    public FullHttpMultiPart retain() {
      content.retain();
      return this;
    }

    @Override
    public FullHttpMultiPart retain(int increment) {
      content.retain(increment);
      return this;
    }

    @Override
    public FullHttpMultiPart touch(Object hint) {
      content.touch(hint);
      return this;
    }

    @Override
    public FullHttpMultiPart touch() {
      content.touch();
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
    public FullHttpMultiPart replace(ByteBuf content) {
      BasicFullHttpMultiPart dup = new BasicFullHttpMultiPart(message, content);
      dup.headers().set(headers());
      return dup;
    }

    @Override
    public FullHttpMultiPart copy() {
      return replace(content().copy());
    }

    @Override
    public FullHttpMultiPart duplicate() {
      return replace(content().duplicate());
    }

    @Override
    public FullHttpMultiPart retainedDuplicate() {
      return replace(content().retainedDuplicate());
    }

    @Override
    public String toString() {
      return HttpToStringUtils
          .removeLastNewLine(
              HttpToStringUtils.appendHeaders(HttpToStringUtils.appendCommon(new StringBuilder(256), this), headers()))
          .toString();
    }

    @Override
    public <T> Attribute<T> attr(AttributeKey<T> key) {
      return message instanceof AttributeMap ? ((AttributeMap) message).attr(key) : null;
    }

    @Override
    public <T> boolean hasAttr(AttributeKey<T> key) {
      return message instanceof AttributeMap && ((AttributeMap) message).hasAttr(key);
    }
  }
}
