package com.linkedin.venice.router.api;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;


/**
 * Routing mode for Venice Router.
 */
public enum RoutingComputationMode {
  SEQUENTIAL(ArrayList::new, HashMap::new),
  /**
   * This mode will execute the routing computation in parallel among different partitions within the same request.
   */
  PARALLEL(ConcurrentLinkedQueue::new, VeniceConcurrentHashMap::new);

  /**
   * The reason to maintain separate suppliers is mainly to avoid the overhead
   * of thread-safe collections when running in sequential mode.
   */
  private final Supplier<Collection> requestCollectionSupplier;
  private final Supplier<Map> hostMapSupplier;

  RoutingComputationMode(Supplier<Collection> requestCollectionSupplier, Supplier<Map> hostMapSupplier) {
    this.requestCollectionSupplier = requestCollectionSupplier;
    this.hostMapSupplier = hostMapSupplier;
  }

  public Supplier<Collection> getRequestCollectionSupplier() {
    return requestCollectionSupplier;
  }

  public Supplier<Map> getHostMapSupplier() {
    return hostMapSupplier;
  }
}
