package com.linkedin.venice.meta;

import java.util.List;


/**
 * Interface used to register into metadata repository to listen the change of store list.
 */
public interface StoreListChangedListener {
  public void handleStoreListChanged(List<String> storeNameList);
}
