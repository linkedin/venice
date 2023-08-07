package com.linkedin.venice.listener;

import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.grpc.GrpcHandlerContext;
import com.linkedin.venice.listener.grpc.GrpcHandlerPipeline;
import com.linkedin.venice.listener.grpc.VeniceGrpcHandler;
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
public class ServerAclHandler extends SimpleChannelInboundHandler<HttpRequest> implements VeniceGrpcHandler {
  private static final Logger LOGGER = LogManager.getLogger(ServerAclHandler.class);

  public static final AttributeKey<Boolean> SERVER_ACL_APPROVED_ATTRIBUTE_KEY =
      AttributeKey.valueOf("SERVER_ACL_APPROVED_ATTRIBUTE_KEY");

  private final StaticAccessController accessController;
  private final boolean failOnAccessRejection;

  public ServerAclHandler(StaticAccessController accessController) {
    this(accessController, true);
  }

  public ServerAclHandler(StaticAccessController accessController, boolean failOnAccessRejection) {
    this.accessController = accessController;
    this.failOnAccessRejection = failOnAccessRejection;
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
    SslHandler sslHandler = ServerHandlerUtils.extractSslHandler(ctx);
    if (sslHandler == null) {
      throw new VeniceException("Failed to extract ssl handler from the incoming request");
    }

    X509Certificate clientCert = SslUtils.getX509Certificate(sslHandler.engine().getSession().getPeerCertificates()[0]);
    String method = req.method().name();

    boolean accessApproved = accessController.hasAccess(clientCert, VeniceComponent.SERVER, method);
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

  @Override
  public void grpcRead(GrpcHandlerContext ctx, GrpcHandlerPipeline pipeline) {
    pipeline.processRequest(ctx);
  }

  @Override
  public void grpcWrite(GrpcHandlerContext ctx, GrpcHandlerPipeline pipeline) {
    pipeline.processResponse(ctx);
  }
}
