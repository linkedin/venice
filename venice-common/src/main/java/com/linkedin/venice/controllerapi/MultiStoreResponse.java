package com.linkedin.venice.controllerapi;

public class MultiStoreResponse extends ControllerResponse {
  private String[] stores;

  public String[] getStores() {
    return stores;
  }

  public void setStores(String[] stores) {
    this.stores = stores;
  }
}
