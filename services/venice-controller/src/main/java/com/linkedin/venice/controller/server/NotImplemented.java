package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
import spark.Route;


public class NotImplemented {
  public static final String NOT_IMPLEMENTED_MSG = "This method is not implemented yet";

  public static Route getRoute() {
    return (request, response) -> {
      AdminSparkServer.handleError(new VeniceException(NOT_IMPLEMENTED_MSG), request, response);
      response.type(HttpConstants.TEXT_PLAIN);
      return NOT_IMPLEMENTED_MSG;
    };
  }
}
