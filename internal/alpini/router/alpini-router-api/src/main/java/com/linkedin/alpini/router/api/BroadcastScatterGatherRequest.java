package com.linkedin.alpini.router.api;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;


/**
 * This type of specialized {@link ScatterGatherRequest} does not use keys for routing, and carry some extra state.
 *
 * We have a separate subclass for it to avoid carrying extra overhead in all {@link ScatterGatherRequest} instances.
 */
public class BroadcastScatterGatherRequest<H, K> extends ScatterGatherRequest<H, K> {
  private @Nonnull Set<String> _partitionIdsToQuery = new HashSet<>();

  public BroadcastScatterGatherRequest(List host) {
    super(host, Collections.emptySet());
  }

  public void addPartitionNameToQuery(String partitionName) {
    _partitionIdsToQuery.add(Objects.requireNonNull(partitionName, "partitionName"));
  }

  /** For table level query requests, return the list of partitions that should be queried for this request. */
  public Set<String> getPartitionNamesToQuery() {
    return _partitionIdsToQuery.isEmpty() ? Collections.emptySet() : Collections.unmodifiableSet(_partitionIdsToQuery);
  }
}
