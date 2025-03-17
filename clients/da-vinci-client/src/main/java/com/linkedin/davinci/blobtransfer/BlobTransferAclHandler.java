package com.linkedin.davinci.blobtransfer;

import com.linkedin.venice.listener.ServerHandlerUtils;
import com.linkedin.venice.utils.NettyUtils;
import com.linkedin.venice.utils.SslUtils;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import java.security.cert.X509Certificate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Acl handler for blob transfer
 */
@ChannelHandler.Sharable
public class BlobTransferAclHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger LOGGER = LogManager.getLogger(BlobTransferAclHandler.class);

  public BlobTransferAclHandler() {
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpRequest req) throws Exception {
    SslHandler sslHandler = ServerHandlerUtils.extractSslHandler(ctx);
    if (sslHandler == null) {
      LOGGER.error("No SSL handler in the incoming blob transfer request from {}", ctx.channel().remoteAddress());
      NettyUtils.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx, true);
      return;
    }
    try {
      X509Certificate clientCert =
          SslUtils.getX509Certificate(sslHandler.engine().getSession().getPeerCertificates()[0]);
      X509Certificate localCert =
          SslUtils.getX509Certificate(sslHandler.engine().getSession().getLocalCertificates()[0]);

      boolean samePrincipal = clientCert.getIssuerX500Principal().equals(localCert.getIssuerX500Principal());
      if (samePrincipal) {
        LOGGER.info(
            "Client certification {} and local certification {} are the same. Acl check passed.",
            clientCert.getIssuerX500Principal(),
            localCert.getIssuerX500Principal());
        ReferenceCountUtil.retain(req);
        ctx.fireChannelRead(req);
      } else {
        String clientAddress = ctx.channel().remoteAddress().toString();
        LOGGER.error(
            "Unauthorized blob transfer access rejected: {} requested from {} "
                + "due to client certificate mismatch, expected {} but got {}",
            req.uri(),
            clientAddress,
            clientCert.getIssuerX500Principal(),
            localCert.getIssuerX500Principal());
        NettyUtils.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx, true);
      }
    } catch (Exception e) {
      LOGGER.error("Error validating client certificate for blob transfer from {}", ctx.channel().remoteAddress(), e);
      NettyUtils.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx, true);
    }
  }
}
