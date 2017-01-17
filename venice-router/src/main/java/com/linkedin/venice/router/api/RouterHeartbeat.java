package com.linkedin.venice.router.api;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.HelixUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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


  public RouterHeartbeat(HelixManager manager, VeniceHostHealth health, long cycleTime, TimeUnit cycleTimeUnits, int heartbeatTimeoutMillis){
    this.manager = manager;

    int maxConnectionsPerRoute = 2;
    int maxConnections = 100;
    httpClient = HttpAsyncClients.custom()
          .setConnectionReuseStrategy(new DefaultConnectionReuseStrategy())  //Supports connection re-use if able
          .setConnectionManager(VeniceDispatcher.createConnectionManager(maxConnectionsPerRoute, maxConnections))
          .setDefaultRequestConfig(
              RequestConfig.custom()
                  .setSocketTimeout(heartbeatTimeoutMillis)
                  .setConnectTimeout(heartbeatTimeoutMillis)
                  .setConnectionRequestTimeout(heartbeatTimeoutMillis).build() // 10 second sanity timeout.
          )
          .build();
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

            final HttpGet get = new HttpGet(instance.getUrl() + "/" + QueryAction.HEALTH.toString().toLowerCase());
            try {
              // heartbeatTimeout is being used as the socket connection timeout.  By specifying a longer timeout here (* 1.1)
              // we insist that the socket timeout should trigger before this timeout triggers.
              HttpResponse response = httpClient.execute(get, null).get((long) (heartbeatTimeoutMillis * 1.1), TimeUnit.MILLISECONDS);
              int code = response.getStatusLine().getStatusCode();
              if (code != SC_OK) {
                logger.warn("Heartbeat returns " + code + " for " + instance.getUrl());
                health.setHostAsUnhealthy(instance);
              }
            } catch (ExecutionException e) {
              logger.warn("Failed to execute heartbeat on " + instance.getUrl(), e.getCause());
              health.setHostAsUnhealthy(instance);
            } catch (TimeoutException e) {
              logger.warn("Heartbeat timeout for " + instance.getUrl());
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

}
