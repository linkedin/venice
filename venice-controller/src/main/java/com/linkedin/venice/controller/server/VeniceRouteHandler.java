package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
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
    T veniceResponse = responseType.newInstance();
    try {
      internalHandle(request, veniceResponse);
    } catch (Throwable e) {
       if (e.getMessage() != null) {
        veniceResponse.setError(e.getMessage());
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
    return AdminSparkServer.mapper.writeValueAsString(veniceResponse);
  }

  protected boolean checkIsAllowListUser(Request request, ControllerResponse veniceResponse, BooleanSupplier isAllowListUser) {
    if (!isAllowListUser.getAsBoolean()) {
      veniceResponse.setError(ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX + request.url());
      return false;
    }
    return true;
  }

  public abstract void internalHandle(Request request, T veniceResponse);
}
