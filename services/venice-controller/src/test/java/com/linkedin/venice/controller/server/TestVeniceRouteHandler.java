package com.linkedin.venice.controller.server;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.ExceptionType;
import com.linkedin.venice.utils.ObjectMapperFactory;
import org.apache.commons.httpclient.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import spark.Request;
import spark.Response;
import spark.Route;


public class TestVeniceRouteHandler {
  private Admin mockAdmin;

  @BeforeMethod
  public void setUp() {
    mockAdmin = mock(VeniceParentHelixAdmin.class);
    ControllerRequestHandlerDependencies dependencies = mock(ControllerRequestHandlerDependencies.class);
    doReturn(mockAdmin).when(dependencies).getAdmin();
  }

  @Test
  public void testIsAllowListUser() throws Exception {
    Route userAllowedRoute = new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        checkIsAllowListUser(request, veniceResponse, () -> true);
      }
    };

    Route userNotAllowedRoute = new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        checkIsAllowListUser(request, veniceResponse, () -> false);
      }
    };

    Request request = mock(Request.class);
    Response response = mock(Response.class);

    userAllowedRoute.handle(request, response).toString();
    verify(response, never()).status(HttpStatus.SC_FORBIDDEN);

    String veniceResponseStr = userNotAllowedRoute.handle(request, response).toString();
    ControllerResponse veniceResponse =
        ObjectMapperFactory.getInstance().readValue(veniceResponseStr, ControllerResponse.class);
    verify(response).status(HttpStatus.SC_FORBIDDEN);
    Assert.assertEquals(veniceResponse.getErrorType(), ErrorType.BAD_REQUEST);
    Assert.assertEquals(veniceResponse.getExceptionType(), ExceptionType.BAD_REQUEST);
  }
}
