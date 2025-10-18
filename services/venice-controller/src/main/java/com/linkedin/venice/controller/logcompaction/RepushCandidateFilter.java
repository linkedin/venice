package com.linkedin.venice.controller.logcompaction;

import com.linkedin.venice.meta.StoreInfo;


/**
 * {@code RepushCandidateFilter} defines a contract for determining whether a given store
 * should be considered a candidate for log compaction and repush operations.
 *
 * <p>The {@link #apply(String, StoreInfo)} method should return {@code true}
 * if the store should be compacted (i.e., not filtered out), and {@code false}
 * otherwise.</p>
 *
 * <p>Example for an implementation can be found here: {@link com.linkedin.venice.controller.logcompaction.StoreRepushCandidateFilter}</p>
 */
public interface RepushCandidateFilter {
  boolean apply(String clusterName, StoreInfo store);
}
