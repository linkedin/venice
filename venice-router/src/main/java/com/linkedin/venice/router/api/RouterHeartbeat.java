package com.linkedin.venice.router.api;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.httpclient.HttpClientUtils;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
   * @param routerConfig Venice router config
   * @param sslFactory if provided, the heartbeat will attempt to use ssl when checking the status of the storage nodes
   */
  public RouterHeartbeat(LiveInstanceMonitor monitor, VeniceHostHealth health,
      VeniceRouterConfig routerConfig, Optional<SSLEngineComponentFactory> sslFactory){
    int maxConnectionsPerRoute = 2;
    int maxConnections = 100;

    // How long of a timeout we allow for a node to respond to a heartbeat request
    double heartbeatTimeoutMillis = routerConfig.getHeartbeatTimeoutMs();
    long heartbeatCycleMillis = routerConfig.getHeartbeatCycleMs();

    /**
     * Cached dns resolver is empty because we would like to the unhealthy node to be reported correctly
     */
    httpClient = HttpClientUtils.getMinimalHttpClient(1,
                                                      maxConnectionsPerRoute,
                                                      maxConnections,
                                                      (int)heartbeatTimeoutMillis,
                                                      (int)heartbeatTimeoutMillis,
                                                      sslFactory,
                                                      Optional.empty(),
                                                      Optional.empty()
                                                      );
    Runnable runnable = () -> {
      boolean running = true;
      List<Future<HttpResponse>> responseFutures = new ArrayList<>();
      List<Instance> instances = new ArrayList<>();
      while(running) {
        try {
          responseFutures.clear();
          instances.clear();
          // send out all heartbeat requests in parallel
          for (Instance instance : monitor.getAllLiveInstances()) {
            String instanceUrl = instance.getUrl(sslFactory.isPresent());
            final HttpGet get = new HttpGet(instanceUrl + "/" + QueryAction.HEALTH.toString().toLowerCase());
            responseFutures.add(httpClient.execute(get, null));
            instances.add(instance);
          }

          // Invoke the blocking call to wait for heartbeat responses.
          long heartbeatStartTimeInNS = System.nanoTime();
          for (int i = 0; i < responseFutures.size(); i++) {
            Instance instance = instances.get(i);
            String instanceUrl = instance.getUrl(sslFactory.isPresent());

            /**
             * If elapsed time exceeds timeout threshold already, check whether the response future
             * is complete already; if not, the heartbeat request is timeout already.
             *
             * Future.get(0, TimeUnit.MILLISECONDS) will throw TimeoutException immediately if
             * the future is not complete yet.
             */
            double elapsedTime = LatencyUtils.getLatencyInMS(heartbeatStartTimeInNS);
            long timeoutLimit;
            if (elapsedTime >= heartbeatTimeoutMillis) {
              timeoutLimit = 0;
            } else {
              timeoutLimit = (long)(heartbeatTimeoutMillis - elapsedTime);
            }
            try {
              HttpResponse response = responseFutures.get(i).get(timeoutLimit, TimeUnit.MILLISECONDS);
              int code = response.getStatusLine().getStatusCode();
              if (code != SC_OK) {
                logger.warn("Heartbeat returns " + code + " for " + instanceUrl);
                health.setHostAsUnhealthy(instance);
              } else {
                health.setHostAsHealthy(instance);
              }
            } catch (ExecutionException e) {
              logger.warn("Failed to execute heartbeat on " + instanceUrl, e.getCause());
              health.setHostAsUnhealthy(instance);
            } catch (TimeoutException e) {
              logger.warn("Heartbeat timeout for " + instanceUrl);
              health.setHostAsUnhealthy(instance);
            }
          }
          Thread.sleep(heartbeatCycleMillis);
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
