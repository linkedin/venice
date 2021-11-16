package com.linkedin.venice.controllerapi;

import com.linkedin.venice.meta.StoreInfo;
import java.util.List;
import java.util.Map;


public class MultiStoreInfoResponse extends ControllerResponse {
  private List<StoreInfo> storeInfoList;

  public List<StoreInfo> getStoreInfoList() { return storeInfoList; }
  public void setStoreInfoList(List<StoreInfo> newList) { storeInfoList = newList; }
}
