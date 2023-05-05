package com.linkedin.venice.listener;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslHandler;


public class ServerHandlerUtils {
  public static SslHandler extractSslHandler(ChannelHandlerContext ctx) {
    /**
     * Try to extract ssl handler in current channel, which is mostly for http/1.1 request.
     */
    SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
    if (sslHandler != null) {
      return sslHandler;
    }
    /**
     * Try to extract ssl handler in parent channel, which is for http/2 request.
     */
    if (ctx.channel().parent() != null) {
      sslHandler = ctx.channel().parent().pipeline().get(SslHandler.class);
    }
    return sslHandler;
  }
}
