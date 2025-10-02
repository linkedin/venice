package com.linkedin.venice.listener;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.SslUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslHandler;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLPeerUnverifiedException;


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

  /**
   * Return the channel, which contains the ssl handler and it could be the current channel (http/1.x) or the parent channel (http/2).
   */
  public static Channel getOriginalChannel(ChannelHandlerContext ctx) {
    /**
     * Try to extract ssl handler in current channel, which is mostly for http/1.1 request.
     */
    SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
    if (sslHandler != null) {
      return ctx.channel();
    }
    /**
     * Try to extract ssl handler in parent channel, which is for http/2 request.
     */
    if (ctx.channel().parent() != null && ctx.channel().parent().pipeline().get(SslHandler.class) != null) {
      return ctx.channel().parent();
    }

    return null;
  }

  public static X509Certificate extractClientCert(ChannelHandlerContext ctx) throws SSLPeerUnverifiedException {
    SslHandler sslHandler = ServerHandlerUtils.extractSslHandler(ctx);
    if (sslHandler != null) {
      return SslUtils.getX509Certificate(sslHandler.engine().getSession().getPeerCertificates()[0]);
    } else {
      throw new VeniceException("Failed to extract client cert from the incoming request");
    }
  }

  public static String extractClientPrincipal(ChannelHandlerContext ctx) {
    try {
      return extractClientCert(ctx).getSubjectX500Principal().getName();
    } catch (Exception e) {
      return "";
    }
  }
}
