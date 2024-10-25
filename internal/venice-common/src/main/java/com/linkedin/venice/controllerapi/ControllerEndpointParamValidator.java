package com.linkedin.venice.controllerapi;

import com.linkedin.venice.controllerapi.request.NewStoreRequest;
import java.util.Objects;


/**
 * Validates the parameters of the endpoint requests
 * This class should be used on both client and server side to validate the request parameters
 */
public class ControllerEndpointParamValidator {
  public static void validateNewStoreRequest(NewStoreRequest request) {
    Objects.requireNonNull(request.getClusterName(), "Cluster name is mandatory for creating a store");
    Objects.requireNonNull(request.getStoreName(), "Store name is mandatory for creating a store");
    Objects.requireNonNull(request.getKeySchema(), "Key schema is mandatory for creating a store");
    Objects.requireNonNull(request.getValueSchema(), "Value schema is mandatory for creating a store");
  }
}
