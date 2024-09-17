package com.linkedin.venice.listener;

import static com.linkedin.venice.grpc.GrpcUtils.accessResultToGrpcStatus;
import static com.linkedin.venice.grpc.GrpcUtils.extractGrpcClientCert;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.handler.AbstractStoreAclHandler;
import com.linkedin.venice.acl.handler.AccessResult;
import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.protocols.VeniceClientRequest;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Together with {@link ServerAclHandler}, Server will allow two kinds of access pattern:
 * 1. Access from Router, and Router request will be validated in {@link ServerAclHandler}, and {@link ServerStoreAclHandler} will be a quick pass-through.
 * 2. Access from Client directly, and {@link ServerAclHandler} will deny the request, and {@link ServerStoreAclHandler} will
 *    validate the request in store-level, which is exactly same as the access control behavior in Router.
 * If both of them fail, the request will be rejected.
 */
public class ServerStoreAclHandler extends AbstractStoreAclHandler<QueryAction> implements ServerInterceptor {
  private final static Logger LOGGER = LogManager.getLogger(ServerStoreAclHandler.class);

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

  public ServerStoreAclHandler(
      IdentityParser identityParser,
      DynamicAccessController accessController,
      ReadOnlyStoreRepository metadataRepository) {
    super(identityParser, accessController, metadataRepository);
  }

  @Override
  protected boolean needsAclValidation(QueryAction queryAction) {
    /*
     * Skip request uri validations for store name and certificates due to special actions
     * TODO: Identify validations for each query actions and have a flow to perform validations and actions based on
     * query actions
     */
    return !QUERIES_TO_SKIP_ACL.contains(queryAction);
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call,
      Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {
    if (checkWhetherAccessHasAlreadyApproved(headers)) {
      LOGGER.debug("Access already approved by ServerAclHandler");
      return next.startCall(call, headers);
    } else {
      return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(next.startCall(call, headers)) {
        @Override
        public void onMessage(ReqT message) {
          validateStoreAclForGRPC(super::onMessage, message, call, headers);
        }
      };
    }
  }

  // Visible for testing
  <ReqT, RespT> void validateStoreAclForGRPC(
      Consumer<ReqT> onAuthenticated,
      ReqT message,
      ServerCall<ReqT, RespT> call,
      Metadata headers) {
    VeniceClientRequest request = (VeniceClientRequest) message;
    // For now, GRPC only supports STORAGE query
    String resourceName = request.getResourceName();
    String storeName;
    try {
      storeName = extractStoreName(resourceName, QueryAction.STORAGE);
    } catch (IllegalArgumentException e) {
      LOGGER.error("Invalid store name in resource '{}'", resourceName);
      call.close(Status.INVALID_ARGUMENT.withDescription("Invalid request"), headers);
      return;
    }
    String method = request.getMethod();

    if (StringUtils.isEmpty(method)) {
      LOGGER.error("Invalid method {}", method);
      call.close(Status.INVALID_ARGUMENT.withDescription("Invalid request"), headers);
      return;
    }

    try {
      X509Certificate clientCert = extractGrpcClientCert(call);
      AccessResult accessResult = checkAccess(call.getAuthority(), clientCert, storeName, method);
      switch (accessResult) {
        case GRANTED:
          onAuthenticated.accept(message);
          break;
        case UNAUTHORIZED:
        case FORBIDDEN:
        case ERROR_FORBIDDEN:
          call.close(accessResultToGrpcStatus(accessResult), headers);
          break;
      }
    } catch (SSLPeerUnverifiedException e) {
      LOGGER.error("Cannot verify the certificate.", e);
      call.close(Status.UNAUTHENTICATED.withDescription("Invalid certificate"), headers);
    } catch (VeniceException e) {
      LOGGER.error("Cannot process request successfully due to", e);
      call.close(Status.INTERNAL.withDescription(e.getMessage()), headers);
    }
  }

  @Override
  protected String extractStoreName(QueryAction queryAction, String[] requestParts) {
    String resourceName = requestParts[2];
    return extractStoreName(resourceName, queryAction);
  }

  /**
   * In Venice Server, the resource name is actually a Kafka topic name for STORAGE/COMPUTE but store name for DICTIONARY.
   */
  private String extractStoreName(String resourceName, QueryAction queryAction) {
    switch (queryAction) {
      case STORAGE:
      case COMPUTE:
        return Version.parseStoreFromKafkaTopicName(resourceName);
      case DICTIONARY:
        return resourceName;
      default:
        throw new IllegalArgumentException(
            String.format("Unexpected QueryAction: %s with resource name: %s", queryAction, resourceName));
    }
  }

  @Override
  protected QueryAction validateRequest(String[] requestParts) {
    int partsLength = requestParts.length;
    // Only for HEALTH queries, parts length can be 2
    if (partsLength == 2) {
      if (requestParts[1].equalsIgnoreCase(QueryAction.HEALTH.name())) {
        return QueryAction.HEALTH;
      } else {
        return null;
      }
    } else if (partsLength < 3) { // invalid request if parts length < 3 except health queries
      return null;
    } else { // throw exception to retain current behavior for invalid query actions
      try {
        return QueryAction.valueOf(requestParts[1].toUpperCase());
      } catch (IllegalArgumentException exception) {
        return null;
      }
    }
  }

  @Override
  protected boolean isAccessAlreadyApproved(ChannelHandlerContext ctx) {
    /**
     * Access has been approved by {@link ServerAclHandler}.
     */
    Attribute<Boolean> serverAclApprovedAttr = ctx.channel().attr(ServerAclHandler.SERVER_ACL_APPROVED_ATTRIBUTE_KEY);
    return Boolean.TRUE.equals(serverAclApprovedAttr.get());
  }

  static boolean checkWhetherAccessHasAlreadyApproved(Metadata headers) {
    return Boolean.parseBoolean(
        headers.get(Metadata.Key.of(ServerAclHandler.SERVER_ACL_APPROVED, Metadata.ASCII_STRING_MARSHALLER)));
  }
}
