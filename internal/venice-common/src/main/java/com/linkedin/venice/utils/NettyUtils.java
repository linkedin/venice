package com.linkedin.venice.utils;

import static com.linkedin.venice.utils.ExceptionUtils.logClassLoaderContent;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import com.linkedin.venice.HttpConstants;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class NettyUtils {
  private static final Logger LOGGER = LogManager.getLogger(NettyUtils.class);

  public static void setupResponseAndFlush(
      HttpResponseStatus status,
      byte[] body,
      boolean isJson,
      ChannelHandlerContext ctx) {
    setupResponseAndFlush(status, body, isJson, ctx, false);
  }

  public static void setupResponseAndFlush(
      HttpResponseStatus status,
      byte[] body,
      boolean isJson,
      ChannelHandlerContext ctx,
      boolean closeChannelAfterFlush) {
    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status, Unpooled.wrappedBuffer(body));
    try {
      if (isJson) {
        response.headers().set(CONTENT_TYPE, HttpConstants.JSON);
      } else {
        response.headers().set(CONTENT_TYPE, HttpConstants.TEXT_PLAIN);
      }
    } catch (NoSuchMethodError e) { // netty version conflict
      LOGGER.warn("NoSuchMethodError, probably from netty version conflict.", e);
      logClassLoaderContent("netty");
      throw e;
    }
    response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
    if (closeChannelAfterFlush) {
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    } else {
      ctx.writeAndFlush(response);
    }
  }
}
