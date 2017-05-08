package com.linkedin.venice.router.api;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.SslUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.helix.HelixManager;
import org.apache.helix.model.LiveInstance;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.log4j.Logger;

import static org.apache.http.HttpStatus.*;


public class RouterHeartbeat extends AbstractVeniceService {

  private final Thread heartBeatThread;
  private final HelixManager manager;
  private final CloseableHttpAsyncClient httpClient;
  private volatile List<LiveInstance> liveInstances = new ArrayList<>();
  private static final Logger logger = Logger.getLogger(RouterHeartbeat.class);
  private boolean initialized = false;

  /**
   *
   * @param manager HelixMaanger used to identify which storage nodes need to be queried
   * @param health VeniceHostHealth used to mark unreachable nodes as unhealthy
   * @param cycleTime How frequently to issue a heartbeat
   * @param cycleTimeUnits
   * @param heartbeatTimeoutMillis How long of a timeout we allow for a node to respond to a heartbeat request
   * @param sslFactory if provided, the heartbeat will attempt to use ssl when checking the status of the storage nodes
   */
  public RouterHeartbeat(HelixManager manager, VeniceHostHealth health, long cycleTime, TimeUnit cycleTimeUnits, int heartbeatTimeoutMillis, Optional<SSLEngineComponentFactory> sslFactory){
    this.manager = manager;

    int maxConnectionsPerRoute = 2;
    int maxConnections = 100;
    httpClient = SslUtils.getHttpClient(maxConnectionsPerRoute, maxConnections, sslFactory);
    Runnable runnable = () -> {
      boolean running = true;

      // The heartbeat cannot initialize without a manager that is connected to helix.  In order to reduce dependencies
      // in startup ordering, we support lazy initialization and retry until it can initialize.
      while (running && !initialized) {
        initialized = initialize();
        if (!initialized) {
          // TODO: create a plan to allow the router start without a controller already being present, and refuse to serve requests
          // until it is fully connected.  For now this will loop forever until it connects but wont try and block requests
          logger.warn(
              "Failed to initialize heartbeat, probably because Venice cluster isn't up yet.  Will continue to retry");
          try {
            Thread.sleep(cycleTimeUnits.toMillis(cycleTime));
          } catch (InterruptedException e) {
            logger.info("Heartbeat thread shutting down", e);
            running = false;
          }
        }
      }

      while(running) {
        try {
          for (LiveInstance liveInstance : liveInstances) {
            Instance instance = HelixUtils.getInstanceFromHelixInstanceName(liveInstance.getInstanceName());

            String instanceUrl = instance.getUrl(sslFactory.isPresent());
            final HttpGet get = new HttpGet(instanceUrl + "/" + QueryAction.HEALTH.toString().toLowerCase());
            try {
              HttpResponse response = httpClient.execute(get, null).get((long) (heartbeatTimeoutMillis), TimeUnit.MILLISECONDS);
              int code = response.getStatusLine().getStatusCode();
              if (code != SC_OK) {
                logger.warn("Heartbeat returns " + code + " for " + instanceUrl);
                health.setHostAsUnhealthy(instance);
              }
            } catch (ExecutionException e) {
              logger.warn("Failed to execute heartbeat on " + instanceUrl, e.getCause());
              health.setHostAsUnhealthy(instance);
            } catch (TimeoutException e) {
              logger.warn("Heartbeat timeout for " + instanceUrl);
              health.setHostAsUnhealthy(instance);
            }
          }
          Thread.sleep(cycleTimeUnits.toMillis(cycleTime));
        } catch (InterruptedException e) {
          logger.info("Heartbeat thread shutting down", e);
          running = false;
        }
      }
    };
    heartBeatThread = new Thread(runnable);
  }

  @Override
  public boolean startInner() throws Exception {
    httpClient.start();

    heartBeatThread.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    heartBeatThread.interrupt();
    httpClient.close();
  }

  private boolean initialize(){
    try {
      // Check the connection at first to avoid throwing an exception later.
      if (!this.manager.isConnected()) {
        return false;
      }
      this.manager.addLiveInstanceChangeListener((list, context) -> {
        logger.info("new live instance list passed to change listener: " + list.toString()); //DEBUG, REMOVE
        liveInstances = Collections.unmodifiableList(list);
      });
      return true;
    } catch (Exception e) {
      logger.warn("Failed to register LiveInstanceChangeListener in RouterHeartbeat", e);
      return false;
    }
  }

  // Only used by test to verify whether the thread has been initialized or not.
  protected boolean isInitialized() {
    return initialized;
  }

}
