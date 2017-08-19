package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.exceptions.VeniceException;
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
  public Object handle(Request request, Response response)
      throws Exception {
    T veniceResponse = responseType.newInstance();
    try {
      internalHandle(request, veniceResponse);
    } catch (Throwable e) {
      if (e.getMessage() != null) {
        veniceResponse.setError(e.getMessage());
      } else {
        veniceResponse.setError(e.getClass().getName());
      }
      AdminSparkServer.handleError(e, request, response);
    }
    response.type(HttpConstants.JSON);
    return AdminSparkServer.mapper.writeValueAsString(veniceResponse);
  }

  public abstract void internalHandle(Request request, T veniceRepsonse);
}
