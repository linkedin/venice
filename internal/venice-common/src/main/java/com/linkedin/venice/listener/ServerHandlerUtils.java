package com.linkedin.venice.listener;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.SslUtils;
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

  public static X509Certificate extractClientCert(ChannelHandlerContext ctx) throws SSLPeerUnverifiedException {
    SslHandler sslHandler = ServerHandlerUtils.extractSslHandler(ctx);
    if (sslHandler != null) {
      return SslUtils.getX509Certificate(sslHandler.engine().getSession().getPeerCertificates()[0]);
    } else {
      throw new VeniceException("Failed to extract client cert from the incoming request");
    }
  }
}
