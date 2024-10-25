package com.linkedin.venice.controllerapi.request;

public class GetStoreRequest extends ControllerRequest {
  public GetStoreRequest(String clusterName, String storeName) {
    super(clusterName, storeName);
  }
}
