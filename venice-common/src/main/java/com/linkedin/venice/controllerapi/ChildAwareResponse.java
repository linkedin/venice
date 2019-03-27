package com.linkedin.venice.controllerapi;

import java.util.List;


public class ChildAwareResponse extends ControllerResponse {
  private List<String> childControllerUrls;

  public List<String> getChildControllerUrls() {
    return childControllerUrls;
  }

  public void setChildControllerUrls(List<String> childControllerUrls) {
    this.childControllerUrls = childControllerUrls;
  }


  @Override
  public String toString() {
    if (childControllerUrls == null) {
      return super.toString();
    } else {
      return ChildAwareResponse.class.getSimpleName() + "(childControllers: " + childControllerUrls + ", super: "
          + super.toString() + ")";
    }
  }
}
