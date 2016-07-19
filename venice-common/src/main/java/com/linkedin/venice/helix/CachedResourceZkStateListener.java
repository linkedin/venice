package com.linkedin.venice.helix;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.exceptions.VeniceException;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.log4j.Logger;
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
  private final static Logger logger = Logger.getLogger(CachedResourceZkStateListener.class);
  private final VeniceResource resource;
  private volatile boolean disconnected = false;

  public CachedResourceZkStateListener(VeniceResource resource) {
    this.resource = resource;
  }

  @Override
  public void handleStateChanged(Watcher.Event.KeeperState state)
      throws Exception {
    if (state.equals(Watcher.Event.KeeperState.Disconnected)) {
      disconnected = true;
      logger.info("ZK connection is disconnected.");
    } else if (state.equals(Watcher.Event.KeeperState.SyncConnected)) {
      if (disconnected) {
        // Set disconnected to false at first. Otherwise, it may be set to true by disconnect event, which happen during
        // refreshing. Then Set to false after refresh is completed. But the correct value should be true because client
        // is disconnected.
        disconnected = false;
        logger.info("ZK connection is reconnected. Refresh resource:" + resource.getClass().getSimpleName());
        synchronized (this) {
          // If connection is disconnected during refreshing and reconnect again. Synchronized block guarantee that
          // there is only one refresh operation on the fly.
          try {
            resource.refresh();
          } catch (VeniceException e) {
            logger.error(
                "Can not refresh resource:" + resource.getClass().getSimpleName() + " after client is reconnected.", e);
          }
        }
      } else {
        logger.info("ZK connection is connected in first time. Do not refresh resource.");
      }
    }
  }

  @Override
  public void handleNewSession()
      throws Exception {
  }

  @Override
  public void handleSessionEstablishmentError(Throwable error)
      throws Exception {
  }

  protected boolean isDisconnected() {
    return disconnected;
  }
}
