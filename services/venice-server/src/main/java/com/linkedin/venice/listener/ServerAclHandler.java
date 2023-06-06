package com.linkedin.venice.listener;

import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.acl.handler.StoreAclHandler;
import com.linkedin.venice.authentication.AuthenticationService;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.authorization.Method;
import com.linkedin.venice.authorization.Principal;
import com.linkedin.venice.authorization.Resource;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.NettyUtils;
import com.linkedin.venice.utils.SslUtils;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import java.security.cert.X509Certificate;
import java.util.Optional;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Together with {@link ServerStoreAclHandler}, Server will allow two kinds of access pattern:
 * 1. Access from Router, and Router request will be validated in {@link ServerAclHandler}, and {@link ServerStoreAclHandler} will be a quick pass-through.
 * 2. Access from Client directly, and {@link ServerAclHandler} will deny the request, and {@link ServerStoreAclHandler} will
 *    validate the request in store-level, which is exactly same as the access control behavior in Router.
 *
 * If both of them fail, the request will be rejected.
 */
@ChannelHandler.Sharable
public class ServerAclHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger LOGGER = LogManager.getLogger(ServerAclHandler.class);

  public static final AttributeKey<Boolean> SERVER_ACL_APPROVED_ATTRIBUTE_KEY =
      AttributeKey.valueOf("SERVER_ACL_APPROVED_ATTRIBUTE_KEY");

  private final Optional<StaticAccessController> accessController;
  private final Optional<AuthenticationService> authenticationService;
  private final Optional<AuthorizerService> authorizerService;
  private final boolean failOnAccessRejection;

  public ServerAclHandler(
      Optional<StaticAccessController> accessController,
      Optional<AuthenticationService> authenticationService,
      Optional<AuthorizerService> authorizerService) {
    this(accessController, authenticationService, authorizerService, true);
  }

  public ServerAclHandler(
      Optional<StaticAccessController> accessController,
      Optional<AuthenticationService> authenticationService,
      Optional<AuthorizerService> authorizerService,
      boolean failOnAccessRejection) {
    this.accessController = accessController;
    this.failOnAccessRejection = failOnAccessRejection;
    this.authenticationService = authenticationService;
    this.authorizerService = authorizerService;
  }

  /**
   * Verify if client has permission to access one particular resource.
   *
   * @param ctx
   * @param req
   * @throws SSLPeerUnverifiedException
   */
  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest req) throws SSLPeerUnverifiedException {
    Optional<SslHandler> sslHandler = Optional.ofNullable(ServerHandlerUtils.extractSslHandler(ctx));
    if (!sslHandler.isPresent() && accessController.isPresent()) {
      throw new VeniceException("Failed to extract ssl handler from the incoming request");
    }

    X509Certificate clientCert = null;
    if (sslHandler.isPresent()) {
      clientCert = SslUtils.getX509Certificate(sslHandler.get().engine().getSession().getPeerCertificates()[0]);
    }
    String method = req.method().name();

    boolean accessApproved = false;
    if (accessController.isPresent()) {
      accessApproved = accessController.get().hasAccess(clientCert, VeniceComponent.SERVER, method);
    }
    if (authenticationService.isPresent()) {
      Principal principal = StoreAclHandler.getPrincipal(ctx, req, clientCert, authenticationService);
      if (authorizerService.isPresent()) {
        accessApproved = authorizerService.get().canAccess(Method.valueOf(method), new Resource("*"), principal);
      }
      LOGGER.info("Authenticate {} accessApproved: {}", principal, accessApproved);
    }

    ctx.channel().attr(SERVER_ACL_APPROVED_ATTRIBUTE_KEY).set(accessApproved);
    if (accessApproved || !failOnAccessRejection) {
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
    } else {
      if (LOGGER.isDebugEnabled()) {
        String client = ctx.channel().remoteAddress().toString(); // ip and port
        String errLine = String.format("%s requested %s %s", client, method, req.uri());
        LOGGER.debug("Unauthorized access rejected: {}", errLine);
      }
      NettyUtils.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx);
    }
  }
}
