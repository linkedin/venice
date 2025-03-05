package com.linkedin.venice.controller.stats;

import com.linkedin.venice.meta.StoreInfo;
import java.util.List;


/*
 * Interface for determining dead stores stats
 */
public interface DeadStoreStats {

  /*
   * Intends to only return the dead stores.
   * Side effect operation of populating isStoreDead and reasonsStoreIsDead in the StoreInfo object.
   */
  List<StoreInfo> getDeadStores(List<StoreInfo> storeInfos) throws Exception;
}
