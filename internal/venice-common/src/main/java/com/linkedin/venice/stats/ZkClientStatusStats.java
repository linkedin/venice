package com.linkedin.venice.stats;

import static org.apache.zookeeper.Watcher.Event.KeeperState;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import org.apache.helix.zookeeper.zkclient.IZkStateListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The stats keep track of ZK Client status changes. It maps statuses with integers.
 *
 * Common status:
 * {@link KeeperState#Unknown} code: -1 (used as init value)
 * {@link KeeperState#Disconnected} code: 0
 * {@link KeeperState#SyncConnected} code: 3
 */
public class ZkClientStatusStats extends AbstractVeniceStats implements IZkStateListener {
  private static final Logger LOGGER = LogManager.getLogger(ZkClientStatusStats.class);
  private final Sensor zkClientDisconnectedSensor, zkClientExpiredSensor, zkClientSyncConnectedSensor,
      zkClientNewSessionSensor, zkClientSessionEstablishmentErrorSensor, zkClientReconnectionLatencySensor;

  // since ZKClient establish the connection during CTOR, it's likely to miss the first state update
  private KeeperState clientState = KeeperState.Unknown;
  private long disconnectionTime;

  public ZkClientStatusStats(MetricsRepository metricsRepository, String zkClientName) {
    super(metricsRepository, zkClientName);

    disconnectionTime = System.currentTimeMillis();
    zkClientDisconnectedSensor = registerSensor("zk_client_Disconnected", new Count());
    zkClientExpiredSensor = registerSensor("zk_client_Expired", new Count());
    zkClientSyncConnectedSensor = registerSensor("zk_client_SyncConnected", new Count());
    zkClientNewSessionSensor = registerSensor("zk_client_NewSession", new Count());
    zkClientSessionEstablishmentErrorSensor = registerSensor("zk_client_SessionEstablishmentError", new Count());
    zkClientReconnectionLatencySensor =
        registerSensor("zk_client_reconnection_latency", new Avg(), new Min(), new Max());
    registerSensor(new AsyncGauge((c, t) -> clientState.getIntValue(), "zk_client_status"));
  }

  @Override
  public void handleStateChanged(KeeperState state) {
    long reconnectionLatency = -1;
    switch (state) {
      case Disconnected:
        zkClientDisconnectedSensor.record();
        disconnectionTime = System.currentTimeMillis();
        break;
      case Expired:
        zkClientExpiredSensor.record();
        break;
      case SyncConnected:
        zkClientSyncConnectedSensor.record();
        reconnectionLatency = System.currentTimeMillis() - disconnectionTime;
        zkClientReconnectionLatencySensor.record(reconnectionLatency);
        break;
      default:
        // do nothing
    }

    String logMessage = "KeeperState for '" + getName() + "' changed from " + clientState + " to " + state;
    if (reconnectionLatency > -1) {
      logMessage += " with a reconnection latency of " + reconnectionLatency;
    }

    LOGGER.info(logMessage);

    clientState = state;
  }

  @Override
  public void handleNewSession(String s) {
    LOGGER.info("New session established for '{}', current KeeperState: {}", getName(), clientState);
    zkClientNewSessionSensor.record();
  }

  @Override
  public void handleSessionEstablishmentError(Throwable error) {
    LOGGER.error("Session establishment error for '{}', current KeeperState: {}", getName(), clientState, error);
    zkClientSessionEstablishmentErrorSensor.record();
  }
}
