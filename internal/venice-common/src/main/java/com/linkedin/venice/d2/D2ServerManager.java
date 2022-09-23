package com.linkedin.venice.d2;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.servers.ZooKeeperAnnouncer;
import com.linkedin.d2.balancer.servers.ZooKeeperConnectionManager;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class provides a server manager with start/shutdown methods to be invoked to manage the lifecycle of the server
 * announcer.
 * @author Steven Ihde
 * @version $Revision: $
 */
public class D2ServerManager {
  private static final Logger LOGGER = LogManager.getLogger(D2ServerManager.class);

  private final ZooKeeperConnectionManager _manager;
  private final long _startupTimeoutMillis;
  private final boolean _continueIfStartupFails;
  private final long _shutdownTimeoutMillis;
  private final boolean _continueIfShutdownFails;
  private final boolean _doNotStart;
  private final boolean _delayStart;
  private final boolean _initMarkUp;
  private volatile boolean _started;
  private final D2HealthChecker _d2HealthChecker;

  // if this is true, we will create a daemon thread that pings health check endpoint of this server
  // if we receive 3 consecutive bad status, we will mark down zookeeperAnnouncer to prevent traffic
  // from being sent to this server
  private final boolean _healthCheckEnabled;

  public D2ServerManager(
      ZooKeeperConnectionManager manager,
      long startupTimeoutMillis,
      boolean continueIfStartupFails,
      long shutdownTimeoutMillis,
      boolean continueIfShutdownFails,
      boolean doNotStart) {
    this(
        manager,
        startupTimeoutMillis,
        continueIfStartupFails,
        shutdownTimeoutMillis,
        continueIfShutdownFails,
        doNotStart,
        false,
        false,
        true,
        1000,
        3,
        "",
        null,
        500);
  }

  public D2ServerManager(
      ZooKeeperConnectionManager manager,
      long startupTimeoutMillis,
      boolean continueIfStartupFails,
      long shutdownTimeoutMillis,
      boolean continueIfShutdownFails,
      boolean doNotStart,
      boolean delayStart,
      boolean initMarkUp,
      boolean healthCheckEnabled,
      long healthCheckInterval,
      int healthCheckRetries,
      String healthCheckUrl,
      ScheduledExecutorService scheduledExecutorService,
      int d2HealthCheckerTimeoutMs) {
    _manager = manager;
    _startupTimeoutMillis = startupTimeoutMillis;
    _continueIfStartupFails = continueIfStartupFails;
    _shutdownTimeoutMillis = shutdownTimeoutMillis;
    _continueIfShutdownFails = continueIfShutdownFails;
    _doNotStart = doNotStart;
    _delayStart = delayStart;
    _initMarkUp = initMarkUp;
    _started = false;
    _healthCheckEnabled = healthCheckEnabled;
    if (healthCheckEnabled) {
      if (scheduledExecutorService == null) {
        throw new IllegalArgumentException("ScheduledExecutorService cannot be null if health check is enabled");
      }
      if (healthCheckInterval <= 0) {
        throw new IllegalArgumentException("HealthCheckInterval cannot be <= 0 if health check is enabled");
      }
      if (healthCheckRetries <= 0) {
        throw new IllegalArgumentException("healthCheckRetries cannot be <= 0 if health check is enabled");
      }
      if (d2HealthCheckerTimeoutMs <= 0) {
        throw new IllegalArgumentException("d2HealthCheckerTimeoutMs cannot be <= 0 if health check is enabled");
      }
      _d2HealthChecker = new D2HealthChecker(
          manager,
          healthCheckInterval,
          healthCheckUrl,
          healthCheckRetries,
          scheduledExecutorService,
          d2HealthCheckerTimeoutMs);
    } else {
      _d2HealthChecker = null;
    }
  }

  public void start() throws Exception {
    // we will only announce the server when we are not in QEI and the announcement is not delayed.
    if (!_doNotStart && !_delayStart) {
      doStart();
    } else {
      // we were asked to not start the D2ServerManager, log message
      LOGGER.info("Not starting D2 server now. In QEI: " + _doNotStart + ". Delay start: " + _delayStart);
    }
  }

  public synchronized void shutdown() throws Exception {
    if (_started) {
      if (_healthCheckEnabled) {
        LOGGER.info("Shutting down D2 health-checker");
        _d2HealthChecker.shutdown();
        LOGGER.info("Shut down D2 health-checker is complete");
      }
      LOGGER.info("Deannouncing d2 server");
      FutureCallback<None> callback = new FutureCallback<None>();
      _manager.shutdown(callback);
      try {
        callback.get(_shutdownTimeoutMillis, TimeUnit.MILLISECONDS);
        LOGGER.info("d2 server deannounced");
      } catch (Exception e) {
        LOGGER.error("d2 server deannouncement failed", e);
        if (!_continueIfShutdownFails) {
          throw e;
        }
        LOGGER.warn("Proceeding after d2 server deannouncement failure!");
      }
      _started = false;
    }
  }

  public void forceStart() throws Exception {
    // we will only make the announcement here when we are not in QEI and the automatic announcement is
    // disabled by delayStart
    if (!_doNotStart && _delayStart) {
      doStart();
    } else {
      // we were asked to not start the D2ServerManager, log message
      LOGGER.info("Not starting D2 server now. In QEI: " + _doNotStart + ". Delay start: " + _delayStart);
    }
  }

  public synchronized void doStart() throws Exception {
    if (!_started) {
      LOGGER.info("Starting D2 server");
      if (_initMarkUp) {
        LOGGER.info("Announcing D2 server");
      }
      FutureCallback<None> callback = new FutureCallback<None>() {
        @Override
        public void onSuccess(None none) {
          LOGGER.info("D2 server started");
          if (_initMarkUp) {
            LOGGER.info("D2 server announced");
          }
          if (_healthCheckEnabled) {
            try {
              LOGGER.info("D2 health checker is starting");
              _d2HealthChecker.start();
            } catch (Exception e) {
              LOGGER.error("Cannot start D2 Health Checker", e);
            }
          } else {
            LOGGER.info("D2 health checker is disabled");
          }
          super.onSuccess(none);
        }

        @Override
        public void onError(Throwable e) {
          LOGGER.error("Failed to announce D2 server", e);
          super.onError(e);
        }
      };

      _manager.start(callback);
      try {
        callback.get(_startupTimeoutMillis, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        LOGGER.error("ZooKeeperConnectionManager startup failed", e);
        if (!_continueIfStartupFails) {
          // Since our init will not complete, Spring will not call our destroy.
          // Thus we invoke shutdown here to try to clean up the ZooKeeper threads that
          // would otherwise hang around.
          try {
            shutdown();
          } catch (Exception e2) {
            LOGGER.warn("Ignored error while shutting down after failed startup", e2);
          }
          throw e;
        }
        LOGGER.warn("Proceeding after ZooKeeperConnectionManager startup failure!");
        // The ZooKeeperConnectionManager will continue to try to connect to ZK
        // and start the announcers once ZK eventually shows up.
      }
      _started = true;
    }
  }

  public ZooKeeperAnnouncer[] getZkAnnouncers() {
    if (_started) {
      return _manager.getAnnouncers();
    } else {
      return new ZooKeeperAnnouncer[0];
    }
  }

  public ZooKeeperAnnouncer[] getZkAnnouncersWithoutStartCheck() {
    return _manager.getAnnouncers();
  }

  public String getZkConnectString() {
    return _manager.getZooKeeperConnectString();
  }

  public Set<String> getNodeUris() {
    Set<String> nodeUris = new HashSet<>();
    for (ZooKeeperAnnouncer announcer: _manager.getAnnouncers()) {
      nodeUris.add(announcer.getUri());
    }

    return nodeUris;
  }

  public synchronized boolean isStarted() {
    return _started;
  }

  public synchronized boolean isDelayStart() {
    return _delayStart;
  }

  public synchronized boolean isDoNotStart() {
    return _doNotStart;
  }
}
