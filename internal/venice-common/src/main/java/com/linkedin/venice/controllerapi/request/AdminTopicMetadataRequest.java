package com.linkedin.venice.controllerapi.request;

public class AdminTopicMetadataRequest extends ControllerRequest {
  public AdminTopicMetadataRequest(String clusterName, String storeName) {
    super(clusterName);
    this.storeName = storeName; // Optional parameter
  }
}
