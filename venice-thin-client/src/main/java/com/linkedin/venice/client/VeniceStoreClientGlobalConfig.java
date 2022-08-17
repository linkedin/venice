package com.linkedin.venice.client;

import java.util.List;


public class VeniceStoreClientGlobalConfig {
  private List<String> vsonStoreList;

  public void setVsonStoreList(List<String> vsonStoreList) {
    this.vsonStoreList = vsonStoreList;
  }

  public List<String> getVsonStoreList() {
    return vsonStoreList;
  }
}
