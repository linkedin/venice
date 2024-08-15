package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controller.ParentControllerRegionState.ACTIVE;
import static com.linkedin.venice.controller.ParentControllerRegionState.PASSIVE;
import static com.linkedin.venice.controller.server.VeniceParentControllerRegionStateHandler.ACTIVE_CHECK_FAILURE_WARN_MESSAGE_PREFIX;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.ExceptionType;
import com.linkedin.venice.utils.ObjectMapperFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;
import spark.Request;
import spark.Response;
import spark.Route;


public class TestVeniceParentControllerRegionStateHandler {
  private static final Logger LOGGER = LogManager.getLogger(TestVeniceParentControllerRegionStateHandler.class);
  private Admin admin = mock(Admin.class);
  private Request request = mock(Request.class);
  private Response response = mock(Response.class);
  private VeniceRouteHandler<ControllerResponse> veniceRouteHandler =
      new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
        @Override
        public void internalHandle(Request request, ControllerResponse veniceResponse) {
          LOGGER.info("VeniceParentControllerRegionStateHandler is working");
          veniceResponse.setName("working test response");
          veniceResponse.setCluster("working test cluster");
        }
      };

  @Test
  public void testActiveParent() throws Exception {
    reset(admin, request, response);
    when(admin.getParentControllerRegionState()).thenReturn(ACTIVE);
    when(admin.isParent()).thenReturn(true);
    Assert.assertEquals(admin.getParentControllerRegionState(), ACTIVE);
    Assert.assertTrue(admin.isParent());
    verifySuccessResponse(response);
  }

  @Test
  public void testPassiveParent() throws Exception {
    reset(admin, request, response);
    when(admin.getParentControllerRegionState()).thenReturn(PASSIVE);
    when(admin.isParent()).thenReturn(true);
    Assert.assertEquals(admin.getParentControllerRegionState(), PASSIVE);
    Assert.assertTrue(admin.isParent());
    verifyErrorResponse(response);
  }

  @Test
  public void testActiveChild() throws Exception {
    reset(admin, request, response);
    when(admin.getParentControllerRegionState()).thenReturn(ACTIVE);
    when(admin.isParent()).thenReturn(false);
    Assert.assertEquals(admin.getParentControllerRegionState(), ACTIVE);
    Assert.assertFalse(admin.isParent());
    verifySuccessResponse(response);
  }

  @Test
  public void testPassiveChild() throws Exception {
    reset(admin, request, response);
    when(admin.getParentControllerRegionState()).thenReturn(PASSIVE);
    when(admin.isParent()).thenReturn(false);
    Assert.assertEquals(admin.getParentControllerRegionState(), PASSIVE);
    Assert.assertFalse(admin.isParent());
    verifySuccessResponse(response);
  }

  private void verifySuccessResponse(Response response) throws Exception {
    Route route = new VeniceParentControllerRegionStateHandler(admin, veniceRouteHandler);
    String veniceResponseStr = route.handle(request, response).toString();
    verify(response, never()).status(HttpConstants.SC_MISDIRECTED_REQUEST);
    ControllerResponse veniceResponse =
        ObjectMapperFactory.getInstance().readValue(veniceResponseStr, ControllerResponse.class);
    Assert.assertEquals(veniceResponse.getName(), "working test response");
    Assert.assertEquals(veniceResponse.getCluster(), "working test cluster");
  }

  private void verifyErrorResponse(Response response) throws Exception {
    Route route = new VeniceParentControllerRegionStateHandler(admin, veniceRouteHandler);
    String veniceResponseStr = route.handle(request, response).toString();
    ControllerResponse veniceResponse =
        ObjectMapperFactory.getInstance().readValue(veniceResponseStr, ControllerResponse.class);
    verify(response).status(HttpConstants.SC_MISDIRECTED_REQUEST);
    Assert.assertTrue(veniceResponse.getError().startsWith(ACTIVE_CHECK_FAILURE_WARN_MESSAGE_PREFIX));
    Assert.assertEquals(veniceResponse.getErrorType(), ErrorType.INCORRECT_CONTROLLER);
    Assert.assertEquals(veniceResponse.getExceptionType(), ExceptionType.INCORRECT_CONTROLLER);
  }
}
