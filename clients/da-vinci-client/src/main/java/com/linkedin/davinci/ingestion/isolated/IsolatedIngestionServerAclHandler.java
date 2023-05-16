package com.linkedin.davinci.ingestion.isolated;

import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.exceptions.VeniceException;
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


@ChannelHandler.Sharable
public class IsolatedIngestionServerAclHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger LOGGER = LogManager.getLogger(IsolatedIngestionServerAclHandler.class);
  private final IdentityParser identityParser;
  private final String allowedPrincipalName;

  public IsolatedIngestionServerAclHandler(IdentityParser identityParser, String allowedPrincipalName) {
    this.identityParser = identityParser;
    this.allowedPrincipalName = allowedPrincipalName;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpRequest req) throws Exception {
    SslHandler sslHandler = ServerHandlerUtils.extractSslHandler(ctx);
    if (sslHandler == null) {
      throw new VeniceException("No SSL handler in the incoming request.");
    }
    X509Certificate clientCert = SslUtils.getX509Certificate(sslHandler.engine().getSession().getPeerCertificates()[0]);
    String identity = identityParser.parseIdentityFromCert(clientCert);
    /**
     * Check if the principal identity name extracted from SSL session matches the allowed principal name.
     * We do not enforce SERVICE_PRINCIPAL type here since some testing environments are using PERSONAL_PRINCIPAL.
     */
    if (identity.equals(allowedPrincipalName)) {
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
    } else {
      String clientAddress = ctx.channel().remoteAddress().toString();
      LOGGER.error(
          "Unauthorized access rejected: {} requested from {} with identity {}",
          req.uri(),
          clientAddress,
          identity);
      NettyUtils.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx);
    }
  }
}
