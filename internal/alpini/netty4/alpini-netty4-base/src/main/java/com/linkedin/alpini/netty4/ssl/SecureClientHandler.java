package com.linkedin.alpini.netty4.ssl;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.AttributeKey;
import java.security.cert.X509Certificate;
import java.util.function.BiPredicate;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This is a port of the {@literal com.linkedin.security.netty.ssl.access.control.SecureClientHandler} class as
 * a netty 4 implementation.
 *
 * Created by acurtis on 9/7/17.
 */
@ChannelHandler.Sharable
public class SecureClientHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LogManager.getLogger(SecureClientHandler.class);
  public static final AttributeKey<X509Certificate> CLIENT_CERTIFICATE_ATTRIBUTE_KEY =
      AttributeKey.valueOf(SecureClientHandler.class, "Principal");

  private final BiPredicate<ChannelHandlerContext, X509Certificate> _clientCertificateValidation;

  public SecureClientHandler() {
    this(null);
  }

  public SecureClientHandler(BiPredicate<ChannelHandlerContext, X509Certificate> clientCertificateValidation) {
    _clientCertificateValidation = clientCertificateValidation;
  }

  protected void sslHandshakeComplete(ChannelHandlerContext ctx, SslHandshakeCompletionEvent evt) {
    if (ctx.channel().hasAttr(CLIENT_CERTIFICATE_ATTRIBUTE_KEY)) {
      return;
    }
    SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
    SSLSession session = sslHandler.engine().getSession();

    if (evt.isSuccess()) {
      try {
        java.security.cert.Certificate[] certs = session.getPeerCertificates();
        if (certs == null || 0 == certs.length) {
          throw new SSLPeerUnverifiedException("No peer certificates available");
        }

        if (!(certs[0] instanceof X509Certificate)) {
          throw new SSLPeerUnverifiedException("Not using an x509 certificate");
        }

        if (_clientCertificateValidation != null) {
          X509Certificate clientCert = (X509Certificate) certs[0];
          if (!_clientCertificateValidation.test(ctx, clientCert)) {
            throw new SSLPeerUnverifiedException("Client failed principal validation: " + clientCert);
          }

          ctx.channel().attr(CLIENT_CERTIFICATE_ATTRIBUTE_KEY).set(clientCert);
        }

      } catch (Exception ex) {
        LOG.error("Peer validation failed: {}", ctx.channel().remoteAddress(), ex);
        ctx.channel().close();
      }
    } else {
      LOG.error("SSL handshake failed: {}", ctx.channel().remoteAddress(), evt.cause());
      ctx.channel().close();
    }
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof SslHandshakeCompletionEvent) {
      sslHandshakeComplete(ctx, (SslHandshakeCompletionEvent) evt);
    }

    super.userEventTriggered(ctx, evt);
  }
}
