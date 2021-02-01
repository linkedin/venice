package com.linkedin.davinci.client;

public enum NonLocalAccessPolicy {
  /**
   * If a read is issued for a non-local partition, it will throw {@link NonLocalAccessException}.
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
   */
  QUERY_VENICE,
}
