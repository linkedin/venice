package com.linkedin.alpini.router.api;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import java.util.List;
import javax.annotation.Nonnull;


/**
 * Provides methods to locate the storage node or nodes for database partitions.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision$
 */
public interface HostFinder<H, R> {
  default HostFinder<H, R> getSnapshot() {
    return this;
  }

  /**
   * Find hosts that can serve the given database and partition with one of the roles passed. Roles are
   * processed in order with roles at the same level being processed as equivalent, such that a request
   * for {{"LEADER"}, {"FOLLOWER"}} would first look for a LEADER, and if none is available then it will
   * look for a FOLLOWER, while a request for {{"LEADER","FOLLOWER"}} would pick a random host that is either
   * a LEADER or a FOLLOWER.
   *
   * Note: This call is expected to be non-blocking
   */
  @Nonnull
  List<H> findHosts(
      @Nonnull String requestMethod,
      @Nonnull String resourceName,
      @Nonnull String partitionName,
      @Nonnull HostHealthMonitor<H> hostHealthMonitor,
      @Nonnull R roles) throws RouterException;

  /**
   * Returns an async future which is completed when a change in the routing table occurs.
   */
  default AsyncFuture<HostFinder<H, R>> getChangeFuture() {
    return AsyncFuture.cancelled();
  }
}
