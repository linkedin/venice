package com.linkedin.venice.listener;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslHandler;
import java.util.Optional;


public class ServerHandlerUtils {

  public static Optional<SslHandler> extractSslHandler(ChannelHandlerContext ctx) {
    /**
     * Try to extract ssl handler in current channel, which is mostly for http/1.1 request.
     */
    SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
    if (null != sslHandler) {
      return Optional.of(sslHandler);
    }
    /**
     * Try to extract ssl handler in parent channel, which is for http/2 request.
     */
    if (null != ctx.channel().parent()) {
      sslHandler = ctx.channel().parent().pipeline().get(SslHandler.class);
    }
    return null == sslHandler ? Optional.empty() : Optional.of(sslHandler);
  }
}
