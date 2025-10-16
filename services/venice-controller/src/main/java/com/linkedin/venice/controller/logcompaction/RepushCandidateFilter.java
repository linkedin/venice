package com.linkedin.venice.controller.logcompaction;

import com.linkedin.venice.meta.StoreInfo;


public interface RepushCandidateFilter {
  /**
   * Takes store info and cluster name and decides whether to filter a store for compaction
   * returns true if store should be compacted
   * */
  boolean apply(String clusterName, StoreInfo store);
}
