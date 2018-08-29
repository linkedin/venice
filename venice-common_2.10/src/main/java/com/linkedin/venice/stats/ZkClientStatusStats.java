package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import org.I0Itec.zkclient.IZkStateListener;

import static org.apache.zookeeper.Watcher.Event.KeeperState;


/**
 * The stats keep track of ZK Client status changes. It maps statuses with integers.
 *
 * Common status:
 * {@link KeeperState.Unknown} code: -1 (used as init value)
 * {@link KeeperState.Disconnected} code: 0
 * {@link KeeperState.SyncConnected} code: 3
 */
public class ZkClientStatusStats extends AbstractVeniceStats implements IZkStateListener {
  //since ZKClient establish the connection during CTOR, it's likely to miss the first state update
  private KeeperState clientState = KeeperState.Unknown;
  private final Sensor zkClientDisconnectionSensor;

  public ZkClientStatusStats(MetricsRepository metricsRepository, String zkClientName) {
    super(metricsRepository, zkClientName);

    zkClientDisconnectionSensor = registerSensor("zk_client_disconnection_times", new Count());
    registerSensor("zk_client_status", new Gauge(() -> clientState.getIntValue()));
  }

  @Override
  public void handleStateChanged(KeeperState state) {
    if (state == KeeperState.Disconnected) {
      zkClientDisconnectionSensor.record();
    }

    clientState = state;
  }

  @Override
  public void handleNewSession() {
    //no-op
  }

  @Override
  public void handleSessionEstablishmentError(Throwable error) {
    //no-op
  }
}
