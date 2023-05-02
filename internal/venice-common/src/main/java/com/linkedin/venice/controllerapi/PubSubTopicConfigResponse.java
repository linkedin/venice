package com.linkedin.venice.controllerapi;

public class PubSubTopicConfigResponse extends ControllerResponse {
  private String topicConfigsResponse;

  public void setTopicConfigsResponse(String topicConfigsResponse) {
    this.topicConfigsResponse = topicConfigsResponse;
  }

  public String getTopicConfigsResponse() {
    return topicConfigsResponse;
  }
}
