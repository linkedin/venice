package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controller.ParentControllerRegionState.ACTIVE;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ParentControllerRegionState;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.exceptions.ErrorType;
import spark.Request;
import spark.Response;
import spark.Route;


/**
 * Handler for checking the state of the region of the parent controller to handle requests in its region.
 */
public class VeniceParentControllerRegionStateHandler implements Route {
  public static final String ACTIVE_CHECK_FAILURE_WARN_MESSAGE_PREFIX =
      "Only active parent controllers are allowed to run ";
  private Admin admin;
  private Route route;

  public VeniceParentControllerRegionStateHandler(Admin admin, Route route) {
    this.admin = admin;
    this.route = route;
  }

  @Override
  public Object handle(Request request, Response response) throws Exception {
    ParentControllerRegionState parentControllerRegionState = admin.getParentControllerRegionState();
    boolean isParent = admin.isParent();
    if (isParent && parentControllerRegionState != ACTIVE) {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      response.status(HttpConstants.SC_MISDIRECTED_REQUEST);
      responseObject.setError(ACTIVE_CHECK_FAILURE_WARN_MESSAGE_PREFIX + request.url());
      responseObject.setErrorType(ErrorType.INCORRECT_CONTROLLER);
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    }
    return route.handle(request, response);
  }
}
