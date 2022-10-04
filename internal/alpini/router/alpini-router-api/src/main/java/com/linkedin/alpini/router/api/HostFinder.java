package com.linkedin.alpini.router.api;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import java.util.Collection;
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
   * By accepting the String of initialHost, the host used by initial request, this method would enable its overriding
   * method to use the initialHost as part of its host selection algorithm.
   * For the sake of backward compatibility, we made this method default and just ignore the initialHost and call
   * the findHosts without initialHost.
   *
   * Find hosts that can serve the given database and partition with one of the roles passed. Roles are
   * processed in order with roles at the same level being processed as equivalent, such that a request
   * for {{"LEADER"}, {"FOLLOWER"}} would first look for a LEADER, and if none is available then it will
   * look for a FOLLOWER, while a request for {{"LEADER","FOLLOWER"}} would pick a random host that is either
   * a LEADER or a FOLLOWER.
   *
   * Note: This call is expected to be non-blocking
   */
  @Nonnull
  default List<H> findHosts(
      @Nonnull String requestMethod,
      @Nonnull String resourceName,
      @Nonnull String partitionName,
      @Nonnull HostHealthMonitor<H> hostHealthMonitor,
      @Nonnull R roles,
      String initialHost) throws RouterException {
    return findHosts(requestMethod, resourceName, partitionName, hostHealthMonitor, roles);
  };

  /**
   * Find hosts that can serve the given database with all the roles passed. Roles are
   * processed in order, such that a request for {"LEADER", "FOLLOWER"} would first look for a LEADER, and if
   * none is available then it will look for a FOLLOWER.
   *
   * Note: This call is expected to be non-blocking
   */
  default @Nonnull Collection<H> findAllHosts(@Nonnull String resourceName, R roles) throws RouterException {
    return findAllHosts(roles);
  }

  /**
   * Find all hosts that can serve with all the roles passed.
   * @param roles {@code null} means for all roles.
   *
   * Note: This call is expected to be non-blocking
   */
  @Nonnull
  Collection<H> findAllHosts(R roles) throws RouterException;

  /**
   * Returns an async future which is completed when a change in the routing table occurs.
   */
  default AsyncFuture<HostFinder<H, R>> getChangeFuture() {
    return AsyncFuture.cancelled();
  }
}
