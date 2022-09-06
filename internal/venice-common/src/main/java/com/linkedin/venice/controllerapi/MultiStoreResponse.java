package com.linkedin.venice.controllerapi;

public class MultiStoreResponse
    extends ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  private String[] stores;

  public String[] getStores() {
    return stores;
  }

  public void setStores(String[] stores) {
    this.stores = stores;
  }
}
