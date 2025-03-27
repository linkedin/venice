package com.linkedin.venice.controller.stats;

import com.linkedin.venice.meta.StoreInfo;
import java.util.List;


/*
 * Interface for determining dead stores stats
 */
public interface DeadStoreStats {
  /**
   * Side effect operation of populating isStoreDead and setStoreDeadStatusReasons in the StoreInfo object and
   * returns back the same list of StoreInfo objects.
   */
  List<StoreInfo> getDeadStores(List<StoreInfo> storeInfos);
}
