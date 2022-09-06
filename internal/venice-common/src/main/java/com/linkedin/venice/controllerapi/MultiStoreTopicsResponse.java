package com.linkedin.venice.controllerapi;

import java.util.List;


public class MultiStoreTopicsResponse extends ControllerResponse {
  private List<String> topics;

  public List<String> getTopics() {
    return topics;
  }

  public void setTopics(List<String> topics) {
    this.topics = topics;
  }
}
