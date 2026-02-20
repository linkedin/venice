package com.linkedin.venice.controller.logcompaction;

import com.linkedin.venice.meta.StoreInfo;


/**
 * {@code RepushCandidateTrigger} defines a condition that can independently justify scheduling
 * a store for log compaction via repush.
 *
 * <p>Unlike {@link RepushCandidateFilter} which uses AND logic (all filters must pass),
 * triggers use OR logic: if <b>any</b> trigger returns {@code true}, the store is scheduled
 * for compaction (provided all prerequisite filters also pass).</p>
 *
 * <p>Examples of triggers include version staleness and high duplicate key ratios.</p>
 *
 * @see RepushCandidateFilter
 * @see CompactionManager#filterStore(StoreInfo, String)
 */
public interface RepushCandidateTrigger {
  boolean apply(String clusterName, StoreInfo store);
}
