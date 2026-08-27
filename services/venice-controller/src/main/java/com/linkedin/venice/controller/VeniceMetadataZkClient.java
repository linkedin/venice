package com.linkedin.venice.controller;

import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.stats.ZkClientStatusStats;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import org.apache.helix.zookeeper.impl.client.ZkClient;


/**
 * Owns the single {@link ZkClient} used for Venice metadata (System #2): Stores, Schemas, StoreConfig,
 * OfflinePushStatus, AdminTopicMetadata, StoreGraveyard, Personas, etc.
 *
 * <p>ZooKeeper is used by the controller for two distinct systems:
 * <ol>
 *   <li>Helix cluster coordination (owned by Helix APIs) — see {@link ZkHelixAdminClient} and
 *   {@link VeniceHelixAdmin}'s dedicated {@code helixZkClient}.</li>
 *   <li>Venice metadata (owned by Venice code) — owned by this class.</li>
 * </ol>
 *
 * <p>Giving the Venice-metadata ZK connection a single, explicit owner keeps its lifecycle (construction,
 * connection-status stats, close) in one place, and creates the seam a later HA change needs: repointing
 * Venice metadata at a separate/backup ZK ensemble will only require changing the address passed to this
 * class's constructor, rather than touching any of the Venice-metadata accessor classes (e.g.
 * {@code ZkAllowlistAccessor}, {@code ZkExecutionIdAccessor}, {@code HelixReadOnlyStoreConfigRepository},
 * {@code HelixStoreGraveyard}) that all consume the {@link ZkClient} returned by {@link #getZkClient()}.
 */
public class VeniceMetadataZkClient implements Closeable {
  private final ZkClient zkClient;

  public VeniceMetadataZkClient(String zkAddress, MetricsRepository metricsRepository, String statsNamePrefix) {
    this.zkClient = ZkClientFactory.newZkClient(zkAddress);
    this.zkClient.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, statsNamePrefix));
  }

  /**
   * @return the underlying {@link ZkClient} connected to the Venice-metadata ZK ensemble, for the
   * Venice-metadata accessor classes to consume directly.
   */
  public ZkClient getZkClient() {
    return zkClient;
  }

  @Override
  public void close() {
    zkClient.close();
  }
}
