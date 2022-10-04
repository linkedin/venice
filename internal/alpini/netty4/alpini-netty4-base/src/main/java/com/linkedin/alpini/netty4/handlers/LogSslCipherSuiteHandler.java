package com.linkedin.alpini.netty4.handlers;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@ChannelHandler.Sharable
public class LogSslCipherSuiteHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LogManager.getLogger(LogSslCipherSuiteHandler.class);

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt == SslHandshakeCompletionEvent.SUCCESS || evt.toString().equals("READY_EVENT")) {
      SslHandler handler = ctx.pipeline().get(SslHandler.class);
      SSLSession session = handler.engine().getSession();
      String remoteCN = null;
      try {
        for (Certificate cert: session.getPeerCertificates()) {
          if (cert instanceof X509Certificate) {
            X500Principal cn = ((X509Certificate) cert).getSubjectX500Principal();
            if (cn != null) {
              remoteCN = cn.getName();
              break;
            }
          }
        }
      } catch (Throwable ex) {
        LOG.warn("Unable to obtain remote CN for {}", ctx.channel().remoteAddress(), ex);
      }
      LOG.info(
          "Cipher suite used is {}, remote address is {}, remote cn is {}",
          session.getCipherSuite(),
          ctx.channel().remoteAddress(),
          remoteCN);
    }
    super.userEventTriggered(ctx, evt);
  }
}
