package com.linkedin.davinci.ingestion.isolated;

import com.linkedin.security.datavault.common.principal.Principal;
import com.linkedin.security.datavault.common.principal.PrincipalBuilder;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.ServerHandlerUtils;
import com.linkedin.venice.utils.NettyUtils;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import java.security.cert.X509Certificate;
import java.util.Optional;
import org.apache.log4j.Logger;


@ChannelHandler.Sharable
public class IsolatedIngestionServerAclHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger logger = Logger.getLogger(IsolatedIngestionServerAclHandler.class);
  private final String allowedPrincipalName;

  public IsolatedIngestionServerAclHandler(String allowedPrincipalName) {
    this.allowedPrincipalName = allowedPrincipalName;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpRequest req) throws Exception {
    Optional<SslHandler> sslHandler = ServerHandlerUtils.extractSslHandler(ctx);
    if (!sslHandler.isPresent()) {
      throw new VeniceException("No SSL handler in the incoming request.");
    }

    X509Certificate clientCert = (X509Certificate) sslHandler.get().engine().getSession().getPeerCertificates()[0];
    Principal principal = PrincipalBuilder.builderForCertificate(clientCert).build();
    String principalIdentityName = principal.getPrincipalId() + "-identity";
    /**
     * Check if the principal identity name extracted from SSL session matches the allowed principal name.
     * We do not enforce SERVICE_PRINCIPAL type here since some testing environments are using PERSONAL_PRINCIPAL.
     */
    if (principalIdentityName.equals(allowedPrincipalName)) {
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
    } else {
      String clientAddress = ctx.channel().remoteAddress().toString();
      String errLine = String.format("%s requested from %s with principal: %s", req.uri(), clientAddress, principal);
      logger.error("Unauthorized access rejected: " + errLine);
      NettyUtils.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx);
    }
  }
}
