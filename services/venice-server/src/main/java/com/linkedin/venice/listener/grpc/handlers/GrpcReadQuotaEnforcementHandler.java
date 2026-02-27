package com.linkedin.venice.listener.grpc.handlers;

import static com.linkedin.venice.listener.ReadQuotaEnforcementHandler.INVALID_REQUEST_RESOURCE_MSG;
import static com.linkedin.venice.listener.ReadQuotaEnforcementHandler.SERVER_OVER_CAPACITY_MSG;

import com.linkedin.venice.listener.ReadQuotaEnforcementHandler;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import java.util.Objects;


public class GrpcReadQuotaEnforcementHandler extends VeniceServerGrpcHandler {
  private final ReadQuotaEnforcementHandler readQuotaHandler;

  public GrpcReadQuotaEnforcementHandler(ReadQuotaEnforcementHandler readQuotaEnforcementHandler) {
    readQuotaHandler =
        Objects.requireNonNull(readQuotaEnforcementHandler, "ReadQuotaEnforcementHandler cannot be null");
  }

  @Override
  public void processRequest(GrpcRequestContext context) {
    RouterRequest request = context.getRouterRequest();
    ReadQuotaEnforcementHandler.QuotaEnforcementResult result = readQuotaHandler.enforceQuota(request);
    if (result == ReadQuotaEnforcementHandler.QuotaEnforcementResult.ALLOWED) {
      invokeNextHandler(context);
      return;
    }

    context.setError();
    if (result == ReadQuotaEnforcementHandler.QuotaEnforcementResult.BAD_REQUEST) {
      context.getVeniceServerResponseBuilder()
          .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST.getCode())
          .setErrorMessage(INVALID_REQUEST_RESOURCE_MSG + request.getResourceName());
    } else if (result == ReadQuotaEnforcementHandler.QuotaEnforcementResult.REJECTED) {
      context.getVeniceServerResponseBuilder()
          .setErrorCode(VeniceReadResponseStatus.TOO_MANY_REQUESTS.getCode())
          .setErrorMessage("");
    } else if (result == ReadQuotaEnforcementHandler.QuotaEnforcementResult.OVER_CAPACITY) {
      context.getVeniceServerResponseBuilder()
          .setErrorCode(VeniceReadResponseStatus.SERVICE_UNAVAILABLE.getCode())
          .setErrorMessage(SERVER_OVER_CAPACITY_MSG);
    }

    // If we reach here, the request is allowed; retain the request and pass it to the next handler
    invokeNextHandler(context);
  }
}
