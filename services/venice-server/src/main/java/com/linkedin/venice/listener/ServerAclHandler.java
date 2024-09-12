package com.linkedin.venice.listener;

import static com.linkedin.venice.listener.ServerHandlerUtils.extractClientCert;
import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static io.grpc.Metadata.Key;

import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.grpc.GrpcUtils;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.utils.NettyUtils;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import java.security.cert.X509Certificate;
import java.util.Objects;
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
public class ServerAclHandler extends SimpleChannelInboundHandler<HttpRequest> implements ServerInterceptor {
  public static final String SERVER_ACL_APPROVED = "SERVER_ACL_APPROVED_ATTRIBUTE_KEY";
  public static final AttributeKey<Boolean> SERVER_ACL_APPROVED_ATTRIBUTE_KEY =
      AttributeKey.valueOf(SERVER_ACL_APPROVED);
  private static final Logger LOGGER = LogManager.getLogger(ServerAclHandler.class);
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
    X509Certificate clientCert = extractClientCert(ctx);
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
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call,
      Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(next.startCall(call, headers)) {
      @Override
      public void onMessage(ReqT message) {
        String method = ((VeniceClientRequest) message).getMethod();
        String clientAddr =
            Objects.requireNonNull(call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR)).toString();
        Key<String> accessApprovedKey = Key.of(SERVER_ACL_APPROVED, ASCII_STRING_MARSHALLER);

        boolean accessApproved = false;
        try {
          X509Certificate clientCert = GrpcUtils.extractGrpcClientCert(call);
          accessApproved = accessController.hasAccess(clientCert, VeniceComponent.SERVER, method);
          headers.put(accessApprovedKey, Boolean.toString(accessApproved));
        } catch (SSLPeerUnverifiedException e) {
          LOGGER.error("Failed to extract ssl session from the incoming request", e);
          headers.put(accessApprovedKey, Boolean.toString(accessApproved));
        }

        if (accessApproved || !failOnAccessRejection) {
          super.onMessage(message);
        } else {
          LOGGER.debug("Unauthorized access rejected: {} requested {}", clientAddr, method);
          call.close(Status.PERMISSION_DENIED, headers);
        }
      }
    };
  }
}
