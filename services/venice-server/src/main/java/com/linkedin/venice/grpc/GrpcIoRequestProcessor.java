package com.linkedin.venice.grpc;

import static com.linkedin.venice.listener.QuotaEnforcementHandler.QuotaEnforcementResult.ALLOWED;
import static com.linkedin.venice.listener.ReadQuotaEnforcementHandler.INVALID_REQUEST_RESOURCE_MSG;
import static com.linkedin.venice.listener.ReadQuotaEnforcementHandler.SERVER_OVER_CAPACITY_MSG;

import com.linkedin.venice.listener.QuotaEnforcementHandler;
import com.linkedin.venice.listener.QuotaEnforcementHandler.QuotaEnforcementResult;
import com.linkedin.venice.listener.StorageReadRequestHandler;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is responsible for processing IO requests i.e. single-get, multi-get, and compute operations,
 * in a gRPC server. It manages the request lifecycle by enforcing quotas, delegating valid requests to the storage
 * read handler, and ensuring appropriate error responses for requests that exceed quota limits or encounter other issues.
 *
 * The core responsibilities of this class include:
 * <ul>
 *   <li>Enforce quotas on incoming requests using the {@link QuotaEnforcementHandler} to ensure requests adhere
 *       to system limits, preventing overloading of the system.</li>
 *   <li>Handle quota successful requests by forwarding them to the {@link StorageReadRequestHandler}, which performs the actual
 *       read operations and processes the data asynchronously using a callback (in storage executor thread pool).</li>
 *   <li>Handle failed requests by setting appropriate gRPC error responses based on the quota enforcement result or
 *       other failure conditions.</li>
 * </ul>
 *
 * The {@link #processRequest(GrpcRequestContext)} method is the central entry point for processing gRPC IO requests.
 * It checks for quota violations, processes valid requests, and sends responses back to the client, either success or error,
 * depending on the result of the quota enforcement.
 */
public class GrpcIoRequestProcessor {
  private static final Logger LOGGER = LogManager.getLogger(GrpcIoRequestProcessor.class);
  private final QuotaEnforcementHandler quotaEnforcementHandler;
  private final StorageReadRequestHandler storageReadRequestHandler;
  private final GrpcReplyProcessor replyProcessor;

  public GrpcIoRequestProcessor(GrpcServiceDependencies services) {
    this.quotaEnforcementHandler = services.getQuotaEnforcementHandler();
    this.storageReadRequestHandler = services.getStorageReadRequestHandler();
    this.replyProcessor = services.getGrpcReplyProcessor();
  }

  /**
   * Processes a gRPC request by enforcing quota limits, delegating the request for storage read handling, or
   * setting an appropriate error response depending on the result of quota enforcement.
   *
   * The request is first passed to the {@link QuotaEnforcementHandler}, which determines if the request is allowed
   * based on the current quota limits. If the quota enforcement result is {@link QuotaEnforcementResult#ALLOWED},
   * the request is handed off to the {@link StorageReadRequestHandler} for further processing.
   * The response handling is done asynchronously via the {@link GrpcStorageResponseHandlerCallback}.
   *
   * If the quota enforcement result is not {@link QuotaEnforcementResult#ALLOWED}, an error response is set based on the specific result.
   * The error responses can be:
   * <ul>
   *   <li>{@link QuotaEnforcementResult#BAD_REQUEST}: Indicates the request was malformed or invalid.
   *       A {@code BAD_REQUEST} status is set with a relevant error message.</li>
   *   <li>{@link QuotaEnforcementResult#REJECTED}: Indicates the request was rejected due to exceeding quota limits.
   *       A {@code TOO_MANY_REQUESTS} status is set, along with a message indicating quota exceeded for the resource.</li>
   *   <li>{@link QuotaEnforcementResult#OVER_CAPACITY}: Indicates the server is over capacity. A {@code SERVICE_UNAVAILABLE}
   *       status is set with a message stating server overcapacity.</li>
   *   <li>Any other case results in a default {@code INTERNAL_SERVER_ERROR} status with an error message
   *       indicating an unknown quota enforcement result.</li>
   * </ul>
   *
   * After determining the appropriate response, the method calls {@link GrpcReplyProcessor#sendResponse(GrpcRequestContext)} to
   * finalize and send the response to the client.
   *
   * This method is executed in the gRPC executor thread, so it should not perform any blocking operations.
   *
   * @param requestContext The {@link GrpcRequestContext} containing the request details, response context, and
   *                       associated metrics/stats to be reported.
   */
  public void processRequest(GrpcRequestContext requestContext) {
    RouterRequest request = requestContext.getRouterRequest();

    QuotaEnforcementResult result = quotaEnforcementHandler.enforceQuota(request);
    // If the request is allowed, hand it off to the storage read request handler
    if (result == ALLOWED) {
      storageReadRequestHandler.queueIoRequestForAsyncProcessing(
          request,
          GrpcStorageResponseHandlerCallback.create(requestContext, replyProcessor));
      return;
    }

    // Otherwise, set an error response based on the quota enforcement result
    switch (result) {
      case BAD_REQUEST:
        requestContext.setErrorMessage(INVALID_REQUEST_RESOURCE_MSG + request.getResourceName());
        requestContext.setReadResponseStatus(VeniceReadResponseStatus.BAD_REQUEST);
        break;
      case REJECTED:
        requestContext.setReadResponseStatus(VeniceReadResponseStatus.TOO_MANY_REQUESTS);
        requestContext.setErrorMessage("Quota exceeded for resource: " + request.getResourceName());
        break;
      case OVER_CAPACITY:
        requestContext.setReadResponseStatus(VeniceReadResponseStatus.SERVICE_UNAVAILABLE);
        requestContext.setErrorMessage(SERVER_OVER_CAPACITY_MSG);
        break;
      default:
        requestContext.setReadResponseStatus(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR);
        requestContext.setErrorMessage("Unknown quota enforcement result: " + result);
        LOGGER.error("Unknown quota enforcement result: {}", result);
    }

    replyProcessor.sendResponse(requestContext);
  }
}
