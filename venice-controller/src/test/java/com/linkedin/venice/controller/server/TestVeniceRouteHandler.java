package com.linkedin.venice.controller.server;

import com.linkedin.venice.controllerapi.ControllerResponse;
import org.apache.commons.httpclient.HttpStatus;
import org.testng.annotations.Test;
import spark.Request;
import spark.Response;
import spark.Route;

import static org.mockito.Mockito.*;


public class TestVeniceRouteHandler {

  @Test
  public void testIsAllowListUser() throws Exception {
    Route userAllowedRoute = new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        if (!checkIsAllowListUser(request, veniceResponse, () -> true)) {
          return;
        }
      }
    };

    Route userNotAllowedRoute = new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        if (!checkIsAllowListUser(request, veniceResponse, () -> false)) {
          return;
        }
      }
    };

    Request request = mock(Request.class);
    Response response = mock(Response.class);

    userAllowedRoute.handle(request, response);
    verify(response, never()).status(HttpStatus.SC_FORBIDDEN);

    userNotAllowedRoute.handle(request, response);
    verify(response).status(HttpStatus.SC_FORBIDDEN);
  }
}
