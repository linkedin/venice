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

  /**
   * Pre fetches the dead store stats for the given stores, to then be accessible to getDeadStores()
   * In the case where fetching dead store stats is latency intensive, this method can be used to
   * pre-populate dead store stats
   * @param storeInfos List of StoreInfo objects to pre-fetch stats for.
   */
  void preFetchStats(List<StoreInfo> storeInfos);
}
