package com.linkedin.venice.helix;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.zkclient.IZkStateListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;


/**
 * Listener used to monitor zk connection state change and refresh venice resource once zk connection is reconnected.
 * <p>
 * ZK does not guarantee the delivery of notifications. After ZK client disconnect from one ZK server, it will try to
 * connect to another server. After client is connected again, ZK client will register all of watcher again to the new
 * server. But all of events happen during disconnecting are unknown in client's view. So venice should refresh some
 * resources to sync up local cache with ZK.
 */
public class CachedResourceZkStateListener implements IZkStateListener {
  private final Logger logger;
  public static final int DEFAULT_RETRY_LOAD_ATTEMPTS = 1;
  public static final long DEFAULT_RETRY_LOAD_INTERVAL_IN_MS = TimeUnit.SECONDS.toMillis(10);
  private final VeniceResource resource;
  private final int retryLoadAttempts;
  private final long retryLoadIntervalInMs;
  private volatile boolean disconnected = false;

  public CachedResourceZkStateListener(VeniceResource resource) {
    // By default, we only retry once after connection is reconnected.
    this(resource, DEFAULT_RETRY_LOAD_ATTEMPTS, DEFAULT_RETRY_LOAD_INTERVAL_IN_MS);
  }

  public CachedResourceZkStateListener(VeniceResource resource, int retryLoadAttempts, long retryLoadIntervalInMs) {
    this.resource = resource;
    this.logger = LogManager.getLogger(this.getClass().getSimpleName() + " [" + getResourceName() + "]");
    this.retryLoadAttempts = retryLoadAttempts;
    this.retryLoadIntervalInMs = retryLoadIntervalInMs;
  }

  /**
   * Once the state of zk connection is changed, this function will be called. So it could not be called twice for the
   * same state change.
   */
  @Override
  public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
    if (state.equals(Watcher.Event.KeeperState.Disconnected)) {
      disconnected = true;
      logger.info("ZK connection is disconnected.");
    } else if (state.equals(Watcher.Event.KeeperState.SyncConnected)) {
      if (disconnected) {
        // Set disconnected to false at first. Otherwise, it may be set to true by disconnect event, which happen during
        // refreshing. Then Set to false after refresh is completed. But the correct value should be true because client
        // is disconnected.
        disconnected = false;
        logger.info("ZK connection is reconnected. Will refresh resource.");
        synchronized (this) {
          // If connection is disconnected during refreshing and reconnect again. Synchronized block guarantee that
          // there is only one refresh operation on the fly.
          // As we met the issue that ZK could return partial result just after connection is reconnected.
          // In order to reduce the possibility that we get not-up-to-date data, we keep loading data for
          // retryLoadAttempts with retryLoadIntervalInMs between each two loading.
          // Sleep a random time(no more than retryLoadIntervalInMs) to avoid thunderstorm issue that all nodes are
          // trying to refresh resource at the same time if there is a network issue in that DC.
          Utils.sleep((long) (Math.random() * retryLoadIntervalInMs));
          int attempt = 1;
          while (attempt <= retryLoadAttempts) {
            logger.info(
                "Attempt #{} of {}: Refresh resource after connection is reconnected.",
                attempt,
                retryLoadAttempts);
            try {
              resource.refresh();
              logger.info("Attempt #{} of {}: Refresh completed.", attempt, retryLoadAttempts);
              return;
            } catch (Exception e) {
              logger.error("Can not refresh resource correctly after client is reconnected", e);
              if (attempt < retryLoadAttempts) {
                logger.info("Will retry after {} ms", retryLoadIntervalInMs);
                Utils.sleep(retryLoadIntervalInMs);
              }
              attempt++;
            }
            logger.fatal("Could not refresh resource correctly after {} attempts.", attempt);
          }
        }
      } else {
        logger.info("ZK connection is connected for the first time for resource. Not going to refresh.");
      }
    } else {
      logger.error("handleStateChanged() called with an unexpected state: {}", state);
    }
  }

  @Override
  public void handleNewSession(String s) throws Exception {
    logger.info("handleNewSession() called.");
  }

  @Override
  public void handleSessionEstablishmentError(Throwable error) throws Exception {
    logger.error("handleSessionEstablishmentError() called.", error);
  }

  protected boolean isDisconnected() {
    return disconnected;
  }

  private String getResourceName() {
    return resource.getClass().getSimpleName();
  }
}
