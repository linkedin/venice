package com.linkedin.venice.controllerapi;

import com.linkedin.venice.meta.StoreInfo;


public class StoreResponse extends ControllerResponse {
  private StoreInfo store;

  public StoreInfo getStore() {
    return store;
  }

  public void setStore(StoreInfo store) {
    this.store = store;
  }
}
