package com.linkedin.venice.acl.handler;

import static com.linkedin.venice.listener.ServerHandlerUtils.extractClientCert;

import com.linkedin.venice.acl.AclCreationDeletionListener;
import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.NettyUtils;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.stream.Collectors;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Store-level access control handler, which is being used by both Router and Server.
 */
@ChannelHandler.Sharable
public abstract class AbstractStoreAclHandler<REQUEST_TYPE> extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger LOGGER = LogManager.getLogger(AbstractStoreAclHandler.class);

  private final IdentityParser identityParser;
  private final ReadOnlyStoreRepository metadataRepository;
  private final DynamicAccessController accessController;

  public AbstractStoreAclHandler(
      IdentityParser identityParser,
      DynamicAccessController accessController,
      ReadOnlyStoreRepository metadataRepository) {
    this.identityParser = identityParser;
    this.metadataRepository = metadataRepository;
    this.accessController = accessController
        .init(metadataRepository.getAllStores().stream().map(Store::getName).collect(Collectors.toList()));
    this.metadataRepository.registerStoreDataChangedListener(new AclCreationDeletionListener(accessController));
  }

  /**
   * Verify if client has permission to access.
   *
   * @param ctx
   * @param req
   * @throws SSLPeerUnverifiedException
   */
  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest req) throws SSLPeerUnverifiedException {
    if (isAccessAlreadyApproved(ctx)) {
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
      return;
    }

    String uri = req.uri();
    String method = req.method().name();

    // Parse resource type and store name
    String[] requestParts = URI.create(uri).getPath().split("/");
    REQUEST_TYPE requestType = validateRequest(requestParts);

    if (requestType == null) {
      String errorMessage = "Invalid request uri: " + uri;
      NettyUtils.setupResponseAndFlush(HttpResponseStatus.BAD_REQUEST, errorMessage.getBytes(), false, ctx);
      return;
    }

    if (!needsAclValidation(requestType)) {
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
      return;
    }

    String storeName = extractStoreName(requestType, requestParts);

    // When there is no store present in the metadata repository, pass the ACL check and let the next handler handle the
    // case of deleted or migrated store
    if (metadataRepository.getStore(storeName) == null) {
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
      return;
    }

    X509Certificate clientCert = extractClientCert(ctx);

    AccessResult accessResult = checkAccess(uri, clientCert, storeName, method);
    switch (accessResult) {
      case GRANTED:
        ReferenceCountUtil.retain(req);
        ctx.fireChannelRead(req);
        break;
      case UNAUTHORIZED:
        NettyUtils
            .setupResponseAndFlush(HttpResponseStatus.UNAUTHORIZED, accessResult.getMessage().getBytes(), false, ctx);
        break;
      case FORBIDDEN:
      case ERROR_FORBIDDEN:
        NettyUtils
            .setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, accessResult.getMessage().getBytes(), false, ctx);
        break;
    }
  }

  protected boolean isAccessAlreadyApproved(ChannelHandlerContext ctx) {
    return false;
  }

  protected abstract boolean needsAclValidation(REQUEST_TYPE requestType);

  protected abstract String extractStoreName(REQUEST_TYPE requestType, String[] requestParts);

  /**
   * Validate the request and return the request type. If the request is invalid, return {@code null}
   *
   * @param requestParts the parts of the request URI
   * @return the request type; null if the request is invalid
   */
  protected abstract REQUEST_TYPE validateRequest(String[] requestParts);

  protected AccessResult checkAccess(String uri, X509Certificate clientCert, String storeName, String method) {
    if (VeniceSystemStoreUtils.isSystemStore(storeName)) {
      return AccessResult.GRANTED;
    }

    String client = identityParser.parseIdentityFromCert(clientCert);
    try {
      /**
       * TODO: Consider making this the first check, so that we optimize for the hot path. If rejected, then we
       *       could check whether the request is for a system store, METADATA, etc.
       */
      if (accessController.hasAccess(clientCert, storeName, method)) {
        return AccessResult.GRANTED;
      }

      // Fact:
      // Request gets rejected.
      // Possible Reasons:
      // A. ACL not found. OR,
      // B. ACL exists but caller does not have permission.
      String errLine = String.format("%s requested %s %s", client, method, uri);

      if (!accessController.isFailOpen() && !accessController.hasAcl(storeName)) { // short circuit, order matters
        // Case A
        // Conditions:
        // 0. (outside) Store exists and is being access controlled. AND,
        // 1. (left) The following policy is applied: if ACL not found, reject the request. AND,
        // 2. (right) ACL not found.
        // Result:
        // Request is rejected by DynamicAccessController#hasAccess()
        // Root cause:
        // Requested resource exists but does not have ACL.
        // Action:
        // return 401 Unauthorized
        LOGGER.warn("Requested store does not have ACL: {}", errLine);
        LOGGER.debug(
            "Existing stores: {}",
            () -> metadataRepository.getAllStores().stream().map(Store::getName).sorted().collect(Collectors.toList()));
        LOGGER.debug(
            "Access-controlled stores: {}",
            () -> accessController.getAccessControlledResources().stream().sorted().collect(Collectors.toList()));
        return AccessResult.UNAUTHORIZED;
      } else {
        // Case B
        // Conditions:
        // 1. Fail closed, and ACL found. OR,
        // 2. Fail open, and ACL found. OR,
        // 3. Fail open, and ACL not found.
        // Analyses:
        // (1) ACL exists, therefore result is determined by ACL.
        // Since the request has been rejected, it must be due to lack of permission.
        // (2) ACL exists, therefore result is determined by ACL.
        // Since the request has been rejected, it must be due to lack of permission.
        // (3) In such case, request would NOT be rejected in the first place,
        // according to the definition of hasAccess() in DynamicAccessController interface.
        // Contradiction to the fact, therefore this case is impossible.
        // Root cause:
        // Caller does not have permission to access the resource.
        // Action:
        // return 403 Forbidden
        LOGGER.debug("Unauthorized access rejected: {}", errLine);
        return AccessResult.FORBIDDEN;
      }
    } catch (AclException e) {
      String errLine = String.format("%s requested %s %s", client, method, uri);

      if (accessController.isFailOpen()) {
        LOGGER.warn("Exception occurred! Access granted: {} {}", errLine, e);
        return AccessResult.GRANTED;
      } else {
        LOGGER.warn("Exception occurred! Access rejected: {} {}", errLine, e);
        return AccessResult.ERROR_FORBIDDEN;
      }
    }
  }
}
