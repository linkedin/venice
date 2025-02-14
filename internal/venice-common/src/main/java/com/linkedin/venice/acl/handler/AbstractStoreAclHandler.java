package com.linkedin.venice.acl.handler;

import static com.linkedin.venice.listener.ServerHandlerUtils.extractClientCert;

import com.linkedin.venice.acl.AclCreationDeletionListener;
import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.listener.ServerHandlerUtils;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.NettyUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.AttributeKey;
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
  private static class CachedAcl {
    AccessResult accessResult;
    long timestamp;

    public CachedAcl(AccessResult accessResult, long timestamp) {
      this.accessResult = accessResult;
      this.timestamp = timestamp;
    }
  }

  private static final Logger LOGGER = LogManager.getLogger(AbstractStoreAclHandler.class);
  public static final String STORE_ACL_CHECK_RESULT = "STORE_ACL_CHECK_RESULT_ATTRIBUTE_KEY";
  public static final AttributeKey<VeniceConcurrentHashMap<String, CachedAcl>> STORE_ACL_CHECK_RESULT_ATTRIBUTE_KEY =
      AttributeKey.valueOf(STORE_ACL_CHECK_RESULT);
  private static final byte[] BAD_REQUEST_RESPONSE = "Unexpected! Original channel should not be null".getBytes();

  private final int cacheTTLMs;
  private final Time time;
  private final boolean aclCacheEnabled;

  private final IdentityParser identityParser;
  private final ReadOnlyStoreRepository metadataRepository;
  private final DynamicAccessController accessController;

  public AbstractStoreAclHandler(
      IdentityParser identityParser,
      DynamicAccessController accessController,
      ReadOnlyStoreRepository metadataRepository,
      int cacheTTLMs) {
    this(identityParser, accessController, metadataRepository, cacheTTLMs, new SystemTime());
  }

  public AbstractStoreAclHandler(
      IdentityParser identityParser,
      DynamicAccessController accessController,
      ReadOnlyStoreRepository metadataRepository,
      int cacheTTLMs,
      Time time) {
    this.identityParser = identityParser;
    this.metadataRepository = metadataRepository;
    this.accessController = accessController
        .init(metadataRepository.getAllStores().stream().map(Store::getName).collect(Collectors.toList()));
    this.metadataRepository.registerStoreDataChangedListener(new AclCreationDeletionListener(accessController));
    this.cacheTTLMs = cacheTTLMs;
    this.time = time;
    this.aclCacheEnabled = cacheTTLMs > 0;
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
    Channel originalChannel = ServerHandlerUtils.getOriginalChannel(ctx);
    if (originalChannel == null) {
      NettyUtils.setupResponseAndFlush(HttpResponseStatus.BAD_REQUEST, BAD_REQUEST_RESPONSE, false, ctx);
      return;
    }
    if (isAccessAlreadyApproved(originalChannel)) {
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
      return;
    }

    String uri = req.uri();
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
    if (!metadataRepository.hasStore(storeName)) {
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
      return;
    }

    String method = req.method().name();

    if (!method.equals(HttpMethod.GET.name()) && !method.equals(HttpMethod.POST.name())) {
      // Neither get nor post method, just let it pass
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
      return;
    }
    AccessResult accessResult;
    if (aclCacheEnabled) {
      VeniceConcurrentHashMap<String, CachedAcl> storeAclCache =
          originalChannel.attr(STORE_ACL_CHECK_RESULT_ATTRIBUTE_KEY).get();
      if (storeAclCache == null) {
        originalChannel.attr(STORE_ACL_CHECK_RESULT_ATTRIBUTE_KEY).setIfAbsent(new VeniceConcurrentHashMap<>());
        storeAclCache = originalChannel.attr(STORE_ACL_CHECK_RESULT_ATTRIBUTE_KEY).get();
      }

      accessResult = storeAclCache.compute(storeName, (ignored, value) -> {
        long currentTimestamp = time.getMilliseconds();
        if (value == null || currentTimestamp - value.timestamp > cacheTTLMs) {
          try {
            return new CachedAcl(
                checkAccess(uri, extractClientCert(ctx), storeName, HttpMethod.GET.name()),
                currentTimestamp);
          } catch (Exception e) {
            LOGGER.error("Error while checking access", e);
            return new CachedAcl(AccessResult.ERROR_FORBIDDEN, currentTimestamp);
          }
        } else {
          return value;
        }
      }).accessResult;
    } else {
      accessResult = checkAccess(uri, extractClientCert(ctx), storeName, HttpMethod.GET.name());
    }
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

  protected boolean isAccessAlreadyApproved(Channel originalChannel) {
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

  /**
   * N.B.: This function is called on the hot path, so it's important to make it as efficient as possible. The order of
   *       operations is carefully considered so that short-circuiting comes into play as much as possible. We also try
   *       to minimize the overhead of logging wherever possible (e.g., by minimizing expensive calls, such as the one
   *       to {@link IdentityParser#parseIdentityFromCert(X509Certificate)}).
   */
  protected AccessResult checkAccess(String uri, X509Certificate clientCert, String storeName, String method) {
    try {
      if (accessController.hasAccess(clientCert, storeName, method)) {
        return AccessResult.GRANTED;
      }

      if (VeniceSystemStoreUtils.isSystemStore(storeName)) {
        return AccessResult.GRANTED;
      }

      // Fact:
      // Request gets rejected.
      // Possible Reasons:
      // A. ACL not found. OR,
      // B. ACL exists but caller does not have permission.

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
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(
              "Requested store does not have ACL: {} requested {} {}",
              identityParser.parseIdentityFromCert(clientCert),
              method,
              uri);
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Existing stores: {}",
                metadataRepository.getAllStores().stream().map(Store::getName).sorted().collect(Collectors.toList()));
            LOGGER.debug(
                "Access-controlled stores: {}",
                accessController.getAccessControlledResources().stream().sorted().collect(Collectors.toList()));
          }
        }
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
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Unauthorized access rejected: {} requested {} {}",
              identityParser.parseIdentityFromCert(clientCert),
              method,
              uri);
        }
        return AccessResult.FORBIDDEN;
      }
    } catch (AclException e) {
      if (accessController.isFailOpen()) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(
              "Exception occurred! Access granted: {} requested {} {}",
              identityParser.parseIdentityFromCert(clientCert),
              method,
              uri,
              e);
        }
        return AccessResult.GRANTED;
      } else {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(
              "Exception occurred! Access rejected: {} requested {} {}",
              identityParser.parseIdentityFromCert(clientCert),
              method,
              uri,
              e);
        }
        return AccessResult.ERROR_FORBIDDEN;
      }
    }
  }
}
