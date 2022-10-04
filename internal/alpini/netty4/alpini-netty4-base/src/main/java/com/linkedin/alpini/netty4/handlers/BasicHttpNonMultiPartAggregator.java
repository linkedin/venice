package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.HeaderUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;


/**
 * Created by acurtis on 3/24/17.
 */
public class BasicHttpNonMultiPartAggregator extends BasicHttpObjectAggregator {
  private HeaderUtils.ContentType _contentType;

  public BasicHttpNonMultiPartAggregator(int maxContentLength) {
    super(maxContentLength);
  }

  public BasicHttpNonMultiPartAggregator(int maxContentLength, boolean closeOnExpectationFailed) {
    super(maxContentLength, closeOnExpectationFailed);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (_contentType != null && _contentType.isMultipart()) {
      if (msg instanceof LastHttpContent) {
        _contentType = null;
      }
      fireChannelRead0(ctx, msg);
    } else if (msg instanceof HttpRequest || msg instanceof HttpResponse) {
      HttpMessage message = (HttpMessage) msg;
      _contentType = HeaderUtils.parseContentType(message.headers().get(HttpHeaderNames.CONTENT_TYPE, "text/plain"));
      if (_contentType.isMultipart()) {
        fireChannelRead0(ctx, message);
      } else {
        super.channelRead(ctx, message);
      }
    } else {
      super.channelRead(ctx, msg);
    }
  }

  @SuppressWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  private static void fireChannelRead0(ChannelHandlerContext ctx, Object msg) {
    ChannelHandlerContext ctx2 = ctx.fireChannelRead(msg);
    assert ctx == ctx2;
  }
}
