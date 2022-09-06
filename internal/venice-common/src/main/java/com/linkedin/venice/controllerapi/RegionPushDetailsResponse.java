package com.linkedin.venice.controllerapi;

import com.linkedin.venice.meta.RegionPushDetails;


public class RegionPushDetailsResponse extends ControllerResponse {
  RegionPushDetails pushDetails = null;

  public RegionPushDetailsResponse(RegionPushDetails det) {
    setRegionPushDetails(det);
  }

  public RegionPushDetailsResponse() {
  };

  public void setRegionPushDetails(RegionPushDetails det) {
    pushDetails = det;
  }

  public RegionPushDetails getRegionPushDetails() {
    return pushDetails;
  }
}
