package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;


public class StorageEngineOverheadRatioResponse
    extends ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  double storageEngineOverheadRatio;

  public double getStorageEngineOverheadRatio() {
    return storageEngineOverheadRatio;
  }

  public void setStorageEngineOverheadRatio(double storageEngineOverheadRatio) {
    this.storageEngineOverheadRatio = storageEngineOverheadRatio;
  }

  @JsonIgnore
  public String toString() {
    return this.getClass().getSimpleName() + "(storage_engine_overhead_ratio: " + storageEngineOverheadRatio
        + super.toString() + ")";
  }
}
