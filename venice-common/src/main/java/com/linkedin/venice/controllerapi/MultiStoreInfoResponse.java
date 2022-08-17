package com.linkedin.venice.controllerapi;

import com.linkedin.venice.meta.StoreInfo;
import java.util.ArrayList;
import java.util.List;


public class MultiStoreInfoResponse extends ControllerResponse {
  private ArrayList<StoreInfo> storeInfoList = null;

  public List<StoreInfo> getStoreInfoList() {
    return storeInfoList;
  }

  public void setStoreInfoList(ArrayList<StoreInfo> newList) {
    if (storeInfoList == null)
      storeInfoList = new ArrayList<StoreInfo>();
    storeInfoList.clear();
    for (StoreInfo i: newList) {
      storeInfoList.add(i);
    }
  }
}
