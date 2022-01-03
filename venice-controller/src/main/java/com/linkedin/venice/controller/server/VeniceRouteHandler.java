package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.utils.ExceptionUtils;
import spark.Request;
import spark.Response;
import spark.Route;


/**
 * Common route handler implement the common part of error handling and writing response to json.
 *
 * @param <T>
 */
public abstract class VeniceRouteHandler<T extends ControllerResponse> implements Route {
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
    return AdminSparkServer.mapper.writeValueAsString(veniceResponse);
  }

  public abstract void internalHandle(Request request, T veniceResponse);
}
