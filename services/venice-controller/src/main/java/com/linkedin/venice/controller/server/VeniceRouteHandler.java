package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.utils.ExceptionUtils;
import java.util.function.BooleanSupplier;
import org.apache.http.HttpStatus;
import spark.Request;
import spark.Response;
import spark.Route;


/**
 * Common route handler implement the common part of error handling and writing response to json.
 *
 * @param <T>
 */
public abstract class VeniceRouteHandler<T extends ControllerResponse> implements Route {
  public static final String ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX = "Only admin users are allowed to run ";

  private Class<T> responseType;

  public VeniceRouteHandler(Class<T> responseType) {
    this.responseType = responseType;
  }

  @Override
  public Object handle(Request request, Response response) throws Exception {
    T veniceResponse = responseType.getDeclaredConstructor().newInstance();
    try {
      internalHandle(request, veniceResponse);
    } catch (Throwable e) {
      if (e.getMessage() != null) {
        veniceResponse.setError(e);
      } else {
        veniceResponse.setError(ExceptionUtils.stackTraceToString(e));
      }
      if (veniceResponse instanceof StoreResponse && e instanceof VeniceNoStoreException) {
        // Non-existent store queries set http status code to 404.
        // They do not increase controller EXC count because they do not log the exception.
        response.status(((VeniceNoStoreException) e).getHttpStatusCode());
      } else {
        AdminSparkServer.handleError(e, request, response);
      }
    }
    response.type(HttpConstants.JSON);
    if (veniceResponse.isError() && veniceResponse.getError().startsWith(ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX)) {
      response.status(HttpStatus.SC_FORBIDDEN);
    }
    return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(veniceResponse);
  }

  protected boolean checkIsAllowListUser(
      Request request,
      ControllerResponse veniceResponse,
      BooleanSupplier isAllowListUser) {
    if (!isAllowListUser.getAsBoolean()) {
      veniceResponse.setError(ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX + request.url());
      veniceResponse.setErrorType(ErrorType.BAD_REQUEST);
      return false;
    }
    return true;
  }

  /**
   * {@internalHandle} provides a common way to write a handler function for an HTTP request and fill in the HTTP response.
   * @param request HTTP request for Venice.
   * @param veniceResponse Venice constructed response.
   */
  public abstract void internalHandle(Request request, T veniceResponse);
}
