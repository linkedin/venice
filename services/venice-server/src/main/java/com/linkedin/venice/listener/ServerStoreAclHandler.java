package com.linkedin.venice.listener;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.handler.StoreAclHandler;
import com.linkedin.venice.authentication.AuthenticationService;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.Attribute;
import io.netty.util.ReferenceCountUtil;
import java.util.Optional;
import javax.net.ssl.SSLPeerUnverifiedException;


/**
 * Together with {@link ServerAclHandler}, Server will allow two kinds of access pattern:
 * 1. Access from Router, and Router request will be validated in {@link ServerAclHandler}, and {@link ServerStoreAclHandler} will be a quick pass-through.
 * 2. Access from Client directly, and {@link ServerAclHandler} will deny the request, and {@link ServerStoreAclHandler} will
 *    validate the request in store-level, which is exactly same as the access control behavior in Router.
 *
 * If both of them fail, the request will be rejected.
 */
public class ServerStoreAclHandler extends StoreAclHandler {
  public ServerStoreAclHandler(
      Optional<DynamicAccessController> accessController,
      Optional<AuthenticationService> authenticationService,
      Optional<AuthorizerService> authorizerService,
      ReadOnlyStoreRepository metadataRepository) {
    super(accessController, authenticationService, authorizerService, metadataRepository);
  }

  /**
   * In Venice Server, the resource name is actually a Kafka topic name.
   */
  @Override
  protected String extractStoreName(String resourceName) {
    return Version.parseStoreFromKafkaTopicName(resourceName);
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest req) throws SSLPeerUnverifiedException {
    if (checkWhetherAccessHasAlreadyApproved(ctx)) {
      /**
       * Access has been approved by {@link ServerAclHandler}.
        */
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
    } else {
      super.channelRead0(ctx, req);
    }
  }

  protected static boolean checkWhetherAccessHasAlreadyApproved(ChannelHandlerContext ctx) {
    Attribute<Boolean> serverAclApprovedAttr = ctx.channel().attr(ServerAclHandler.SERVER_ACL_APPROVED_ATTRIBUTE_KEY);
    return Boolean.TRUE.equals(serverAclApprovedAttr.get());
  }
}
