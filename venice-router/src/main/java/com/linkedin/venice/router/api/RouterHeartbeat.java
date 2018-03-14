package com.linkedin.venice.router.api;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.router.httpclient.HttpClientUtils;
import com.linkedin.venice.service.AbstractVeniceService;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.log4j.Logger;

import static org.apache.http.HttpStatus.*;


public class RouterHeartbeat extends AbstractVeniceService {
  private final Thread heartBeatThread;
  private final CloseableHttpAsyncClient httpClient;
  private static final Logger logger = Logger.getLogger(RouterHeartbeat.class);

  /**
   *
   * @param monitor LiveInstanceMonitor used to identify which storage nodes need to be queried
   * @param health VeniceHostHealth used to mark unreachable nodes as unhealthy
   * @param cycleTime How frequently to issue a heartbeat
   * @param cycleTimeUnits
   * @param heartbeatTimeoutMillis How long of a timeout we allow for a node to respond to a heartbeat request
   * @param sslFactory if provided, the heartbeat will attempt to use ssl when checking the status of the storage nodes
   */
  public RouterHeartbeat(LiveInstanceMonitor monitor, VeniceHostHealth health, long cycleTime, TimeUnit cycleTimeUnits,
      int heartbeatTimeoutMillis, Optional<SSLEngineComponentFactory> sslFactory){
    int maxConnectionsPerRoute = 2;
    int maxConnections = 100;
    httpClient = HttpClientUtils.getMinimalHttpClient(maxConnectionsPerRoute, maxConnections, sslFactory);
    Runnable runnable = () -> {
      boolean running = true;

      while(running) {
        try {
          for (Instance instance : monitor.getAllLiveInstances()) {
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
}
