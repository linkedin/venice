package com.linkedin.venice.controller.transport;

import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.controllerapi.ControllerRoute;
import java.util.Map;


/**
 * A utility class to help with route related conversion, mapping and assistance for the
 * {@link com.linkedin.venice.controllerapi.ControllerClient} to route requests in a transport agnostic model.
 */
public class RouteUtils {
  static final Map<ControllerRoute, GrpcRoute> ROUTE_MAP =
      new ImmutableMap.Builder<ControllerRoute, GrpcRoute>().put(ControllerRoute.NEW_STORE, GrpcRoute.CREATE_STORE)
          .build();

  public static GrpcRoute getGrpcRouteFor(ControllerRoute route) {
    return ROUTE_MAP.get(route);
  }
}
