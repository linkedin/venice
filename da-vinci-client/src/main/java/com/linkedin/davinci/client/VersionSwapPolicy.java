package com.linkedin.davinci.client;

public enum VersionSwapPolicy {
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
