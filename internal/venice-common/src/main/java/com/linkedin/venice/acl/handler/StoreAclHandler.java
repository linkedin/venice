package com.linkedin.venice.acl.handler;

import static com.linkedin.venice.grpc.GrpcUtils.extractGrpcClientCert;
import static com.linkedin.venice.grpc.GrpcUtils.httpResponseStatusToGrpcStatus;
import static com.linkedin.venice.listener.ServerHandlerUtils.extractClientCert;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.venice.acl.AclCreationDeletionListener;
import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
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
import io.netty.util.ReferenceCountUtil;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Store-level access control handler, which is being used by both Router and Server.
 */
@ChannelHandler.Sharable
public class StoreAclHandler extends SimpleChannelInboundHandler<HttpRequest> implements ServerInterceptor {
  private static final Logger LOGGER = LogManager.getLogger(StoreAclHandler.class);

  /**
   *  Skip ACL for requests to /metadata, /admin, /current_version, /health and /topic_partition_ingestion_context
   *  as there's no sensitive information in the response.
   */
  private static final Set<QueryAction> QUERIES_TO_SKIP_ACL = new HashSet<>(
      Arrays.asList(
          QueryAction.METADATA,
          QueryAction.ADMIN,
          QueryAction.HEALTH,
          QueryAction.CURRENT_VERSION,
          QueryAction.TOPIC_PARTITION_INGESTION_CONTEXT));

  private final ReadOnlyStoreRepository metadataRepository;
  private final DynamicAccessController accessController;

  public StoreAclHandler(DynamicAccessController accessController, ReadOnlyStoreRepository metadataRepository) {
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
    String uri = req.uri();
    String method = req.method().name();
    String client = ctx.channel().remoteAddress().toString(); // ip and port
    BiConsumer<HttpResponseStatus, String> errorHandler =
        (status, errorMessage) -> NettyUtils.setupResponseAndFlush(status, errorMessage.getBytes(), false, ctx);

    // Parse resource type and store name
    String[] requestParts = URI.create(uri).getPath().split("/");

    if (isInvalidRequest(requestParts)) {
      errorHandler.accept(HttpResponseStatus.BAD_REQUEST, "Invalid request uri: " + uri);
      return;
    }

    /*
     * Skip request uri validations for store name and certificates due to special actions
     * TODO: Identify validations for each query actions and have a flow to perform validations and actions based on
     * query actions
     */
    QueryAction queryAction = QueryAction.valueOf(requestParts[1].toUpperCase());
    if (QUERIES_TO_SKIP_ACL.contains(queryAction)) {
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
      return;
    }

    X509Certificate clientCert = extractClientCert(ctx);
    String resourceName = requestParts[2];
    String storeName = extractStoreName(resourceName, queryAction);

    try {
      // Check ACL in case of non system store as system store contain public information
      if (VeniceSystemStoreUtils.isSystemStore(storeName)
          || hasAccess(client, uri, clientCert, storeName, method, errorHandler)) {
        ReferenceCountUtil.retain(req);
        ctx.fireChannelRead(req);
      }
    } catch (VeniceNoStoreException noStoreException) {
      LOGGER.debug("Requested store does not exist: {} requested {} {}", client, method, req.uri());
      errorHandler.accept(HttpResponseStatus.BAD_REQUEST, "Invalid Venice store name: " + storeName);
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
        VeniceClientRequest request = (VeniceClientRequest) message;
        // For now, GRPC only supports STORAGE query
        String storeName = extractStoreName(request.getResourceName(), QueryAction.STORAGE);
        String method = request.getMethod();

        BiConsumer<Status, Metadata> grpcCloseConsumer = call::close;
        BiConsumer<HttpResponseStatus, String> errorHandler = ((httpResponseStatus, s) -> grpcCloseConsumer
            .accept(httpResponseStatusToGrpcStatus(httpResponseStatus, s), headers));

        if (StringUtils.isEmpty(storeName) || StringUtils.isEmpty(method)) {
          LOGGER.error("Invalid store name {} or method {}", storeName, method);
          grpcCloseConsumer.accept(Status.INVALID_ARGUMENT.withDescription("Invalid request"), headers);
          return;
        }

        try {
          X509Certificate clientCert = extractGrpcClientCert(call);
          String client = Objects.requireNonNull(call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR)).toString();

          if (VeniceSystemStoreUtils.isSystemStore(storeName)
              || hasAccess(client, call.getAuthority(), clientCert, storeName, method, errorHandler)) {
            LOGGER.info("Requested principal has access to resource. Processing request");
            super.onMessage(message);
          }
        } catch (SSLPeerUnverifiedException e) {
          LOGGER.error("Cannot verify the certificate.", e);
          grpcCloseConsumer.accept(Status.UNAUTHENTICATED.withDescription("Invalid certificate"), headers);
        } catch (VeniceException e) {
          LOGGER.error("Cannot process request successfully due to", e);
          grpcCloseConsumer.accept(Status.INTERNAL.withDescription(e.getMessage()), headers);
        }
      }
    };
  }

  /**
   * Extract the store name from the incoming resource name.
   */
  protected String extractStoreName(String resourceName, QueryAction queryAction) {
    return resourceName;
  }

  @VisibleForTesting
  boolean isInvalidRequest(String[] requestParts) {
    int partsLength = requestParts.length;
    boolean invalidRequest = false;

    // Only for HEALTH queries, parts length can be 2
    if (partsLength == 2) {
      invalidRequest = !requestParts[1].equalsIgnoreCase(QueryAction.HEALTH.name());
    } else if (partsLength < 3) { // invalid request if parts length < 3 except health queries
      invalidRequest = true;
    } else { // throw exception to retain current behavior for invalid query actions
      try {
        QueryAction.valueOf(requestParts[1].toUpperCase());
      } catch (IllegalArgumentException exception) {
        throw new VeniceException("Unknown query action: " + requestParts[1]);
      }
    }

    return invalidRequest;
  }

  @VisibleForTesting
  boolean hasAccess(
      String client,
      String uri,
      X509Certificate clientCert,
      String storeName,
      String method,
      BiConsumer<HttpResponseStatus, String> errorHandler) {
    boolean allowRequest = false;
    try {
      /**
       * TODO: Consider making this the first check, so that we optimize for the hot path. If rejected, then we
       *       could check whether the request is for a system store, METADATA, etc.
       */
      allowRequest = accessController.hasAccess(clientCert, storeName, method);
      if (!allowRequest) {
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
              () -> metadataRepository.getAllStores()
                  .stream()
                  .map(Store::getName)
                  .sorted()
                  .collect(Collectors.toList()));
          LOGGER.debug(
              "Access-controlled stores: {}",
              () -> accessController.getAccessControlledResources().stream().sorted().collect(Collectors.toList()));
          errorHandler.accept(
              HttpResponseStatus.UNAUTHORIZED,
              "ACL not found!\n" + "Either it has not been created, or can not be loaded.\n"
                  + "Please create the ACL, or report the error if you know for sure that ACL exists for this store: "
                  + storeName);
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
          errorHandler.accept(
              HttpResponseStatus.FORBIDDEN,
              "Access denied!\n"
                  + "If you are the store owner, add this application (or your own username for Venice shell client) to the store ACL.\n"
                  + "Otherwise, ask the store owner for read permission.");
        }
      }
    } catch (AclException e) {
      String errLine = String.format("%s requested %s %s", client, method, uri);

      if (accessController.isFailOpen()) {
        LOGGER.warn("Exception occurred! Access granted: {} {}", errLine, e);
        allowRequest = true;
      } else {
        LOGGER.warn("Exception occurred! Access rejected: {} {}", errLine, e);
        errorHandler.accept(HttpResponseStatus.FORBIDDEN, "Access denied!");
      }
    }

    return allowRequest;
  }
}
