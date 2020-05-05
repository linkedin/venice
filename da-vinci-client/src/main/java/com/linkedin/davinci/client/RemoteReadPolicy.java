package com.linkedin.davinci.client;

public enum RemoteReadPolicy {
  /**
   * If a read is issued for a non-local partition, it will throw an exception.
   * This is a good policy for applications that expect local access performance
   * and where a mis-alignment of partitioning strategy ought to fail fast and
   * loudly, rather than silently degrade performance.
   */
  FAIL_FAST,

  /**
   * If a read is issued for a non-local partition, it will fall back to remotely
   * querying the Venice backend. This is a good policy for applications that can
   * tolerate degraded performance while the system warms up. It may also be good
   * for applications where the workload is skewed towards a few partitions, which
   * are subscribed to, yet still needs access to all other partitions as well for
   * a less intensive portion of the workload.
   *
   * N.B.: This setting will not be available at first.
   */
  QUERY_REMOTELY,

  /**
   * If a read is issued for a non-local partition, it will fall back to remotely
   * querying the Venice backend, but will also asynchronously kick off the process
   * of subscribing to that partition. This is a good policy for applications that
   * can tolerate degraded performance while the system warms up, and which do not
   * wish to have fine-grained control of the partition assignment. The downside is
   * that performance characteristics and resource utilization is more opaque.
   *
   * N.B.: This setting will not be available at first.
   */
  SUBSCRIBE_AND_QUERY_REMOTELY;
}
