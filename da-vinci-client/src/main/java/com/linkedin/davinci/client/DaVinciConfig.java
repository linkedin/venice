package com.linkedin.davinci.client;

import com.linkedin.venice.meta.PersistenceType;


public class DaVinciConfig {
  private static final NonLocalReadsPolicy DEFAULT_NON_LOCAL_READS_POLICY = NonLocalReadsPolicy.FAIL_FAST;
  private static final VersionSwapPolicy DEFAULT_VERSION_SWAP_POLICY = VersionSwapPolicy.PER_PARTITION;
  private static final PersistenceType DEFAULT_PERSISTENCE_TYPE = PersistenceType.ROCKS_DB;

  public static DaVinciConfig defaultDaVinciConfig(String dataBasePath) {
    return new DaVinciConfig(DEFAULT_NON_LOCAL_READS_POLICY, DEFAULT_VERSION_SWAP_POLICY,
        DEFAULT_PERSISTENCE_TYPE, dataBasePath);
  }

  private DaVinciConfig(NonLocalReadsPolicy nonLocalReadsPolicy, VersionSwapPolicy versionSwapPolicy,
      PersistenceType persistenceType, String dataBasePath) {
    this.nonLocalReadsPolicy = nonLocalReadsPolicy;
    this.versionSwapPolicy = versionSwapPolicy;
    this.persistenceType = persistenceType;
    this.dataBasePath = dataBasePath;
  }

  /**
   * A specification of what to do if a read is issued for a partition that was
   * never subscribed to.
   */
  private NonLocalReadsPolicy nonLocalReadsPolicy;

  /**
   * A specification of the required level of atomicity for version swaps.
   */
  private VersionSwapPolicy versionSwapPolicy;

  /**
   * An Enum of persistence types in Venice.
   */
  private PersistenceType persistenceType;

  /**
   * A folder to store data files.
   */
  private String dataBasePath;

  public DaVinciConfig setNonLocalReadsPolicy(NonLocalReadsPolicy nonLocalReadsPolicy) {
    this.nonLocalReadsPolicy = nonLocalReadsPolicy;
    return this;
  }

  public NonLocalReadsPolicy getNonLocalReadsPolicy() {
    return this.nonLocalReadsPolicy;
  }

  public DaVinciConfig setVersionSwapPolicy(VersionSwapPolicy versionSwapPolicy) {
    this.versionSwapPolicy = versionSwapPolicy;
    return this;
  }

  public VersionSwapPolicy getVersionSwapPolicy() {
    return this.versionSwapPolicy;
  }

  public void setPersistenceType(PersistenceType persistenceType) {
    this.persistenceType = persistenceType;
  }

  public PersistenceType getPersistenceType() {
    return persistenceType;
  }

  public void setDataBasePath(String dataBasePath) {
    this.dataBasePath = dataBasePath;
  }

  public String getDataBasePath() {
    return dataBasePath;
  }

  enum NonLocalReadsPolicy {
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

  enum VersionSwapPolicy {
    /**
     * The least stringent level of atomicity. Each subscribed partition can swap
     * independently of others. If over-partitioning is used, this enables the
     * application to provision small head room, and load and swap partitions one
     * (or few) at a time.
     */
    PER_PARTITION,

    /**
     * All partitions subscribed by this process will swap at once.
     *
     * This requires having a 2x provisioned capacity in order to fully load all
     * partitions in the background.
     */
    PER_PROCESS,

    /**
     * All partitions of all processes in a given region will swap at once.
     *
     * Like {@link PER_PROCESS}, this requires 2x the provisioned capacity of the
     * table, except that the extra capacity is tied up longer, up to however long
     * it takes for straggler processes to consume all partitions. Tying up
     * resources longer has no significant impact compared to {@link PER_PROCESS}
     * as long as only one Venice table is used. If many tables are used, then
     * tying up resources for one table may prevent other tables from beginning
     * their own background loading and swap, if the provisioned headroom is too
     * small. On the other hand, if many tables are used the necessary headroom
     * can be reduced to just the size of the partitions of the biggest table,
     * rather than the total of all tables.
     *
     * This setting is equivalent to what the Venice backend does.
     *
     * N.B.: This setting will not be available at first.
     */
    PER_REGION,

    /**
     * All partitions of all processes of all regions will swap at once.
     *
     * N.B.: This setting will not be available at first.
     */
    GLOBAL;
  }
}

