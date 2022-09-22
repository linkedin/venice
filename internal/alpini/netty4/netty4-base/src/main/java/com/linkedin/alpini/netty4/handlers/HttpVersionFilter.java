package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.HeaderNames;
import com.linkedin.alpini.base.misc.HeaderUtils;
import com.linkedin.alpini.netty4.misc.BadHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicFullHttpResponse;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;


/**
 * Created by acurtis on 9/28/17.
 */
@ChannelHandler.Sharable
public class HttpVersionFilter extends ChannelInboundHandlerAdapter {
  public static final HttpVersion HTTP_0_9 = HttpVersion.valueOf("HTTP/0.9");

  protected boolean checkHttpVersion(HttpVersion version) {
    return HttpVersion.HTTP_1_1.equals(version) || HttpVersion.HTTP_1_0.equals(version);
  }

  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof HttpRequest) {
      HttpRequest request = (HttpRequest) msg;
      if (!checkHttpVersion(request.protocolVersion())) {
        HttpResponse response =
            new BasicFullHttpResponse(request, HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED, Unpooled.EMPTY_BUFFER);
        response.setProtocolVersion(HttpVersion.HTTP_1_1);
        HttpUtil.setContentLength(response, 0);
        HttpUtil.setKeepAlive(response, false);
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        return;
      }
      if (request.decoderResult().isFailure() || msg instanceof BadHttpRequest) {
        HttpResponse response =
            new BasicFullHttpResponse(request, HttpResponseStatus.BAD_REQUEST, Unpooled.EMPTY_BUFFER);
        Throwable cause = request.decoderResult().cause();
        if (cause != null) {
          response.headers().set(HeaderNames.X_ERROR_CLASS, cause.getClass().getName());
          response.headers().set(HeaderNames.X_ERROR_MESSAGE, HeaderUtils.cleanHeaderValue(cause.getMessage()));
        }
        HttpUtil.setContentLength(response, 0);
        HttpUtil.setKeepAlive(response, false);
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        return;
      }
    }
    super.channelRead(ctx, msg);
  }
}
