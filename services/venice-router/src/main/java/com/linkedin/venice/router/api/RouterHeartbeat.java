package com.linkedin.venice.router.api;

import static com.linkedin.venice.HttpConstants.HTTP_GET;
import static org.apache.http.HttpStatus.SC_OK;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.httpclient.PortableHttpResponse;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.httpclient.VeniceMetaDataRequest;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@code RouterHeartbeat} is a service that monitors and reports the health of current live instances in the Venice cluster.
 */
public class RouterHeartbeat extends AbstractVeniceService {
  private final Thread heartBeatThread;
  private static final Logger LOGGER = LogManager.getLogger(RouterHeartbeat.class);

  /**
   *
   * @param monitor LiveInstanceMonitor used to identify which storage nodes need to be queried
   * @param health VeniceHostHealth used to mark unreachable nodes as unhealthy
   * @param routerConfig Venice router config
   * @param sslFactory if provided, the heartbeat will attempt to use ssl when checking the status of the storage nodes
   */
  public RouterHeartbeat(
      LiveInstanceMonitor monitor,
      VeniceHostHealth health,
      VeniceRouterConfig routerConfig,
      Optional<SSLFactory> sslFactory,
      StorageNodeClient storageNodeClient) {

    // How long of a timeout we allow for a node to respond to a heartbeat request
    int heartbeatTimeoutMillis = (int) routerConfig.getHeartbeatTimeoutMs();
    long heartbeatCycleMillis = routerConfig.getHeartbeatCycleMs();

    Runnable runnable = () -> {
      boolean running = true;
      List<CompletableFuture<PortableHttpResponse>> responseFutures = new ArrayList<>();
      List<Instance> instances = new ArrayList<>();
      while (running) {
        try {
          responseFutures.clear();
          instances.clear();
          // send out all heartbeat requests in parallel
          for (Instance instance: monitor.getAllLiveInstances()) {
            VeniceMetaDataRequest request = new VeniceMetaDataRequest(
                instance,
                QueryAction.HEALTH.toString().toLowerCase(),
                HTTP_GET,
                sslFactory.isPresent());
            request.setTimeout(heartbeatTimeoutMillis);
            CompletableFuture<PortableHttpResponse> responseFuture = new CompletableFuture<>();
            storageNodeClient.sendRequest(request, responseFuture);
            responseFutures.add(responseFuture);
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
             */
            double elapsedTime = LatencyUtils.getLatencyInMS(heartbeatStartTimeInNS);
            long timeoutLimit;
            if (elapsedTime >= heartbeatTimeoutMillis) {
              timeoutLimit = 0;
            } else {
              timeoutLimit = (long) (heartbeatTimeoutMillis - elapsedTime);
            }
            try {
              PortableHttpResponse response = responseFutures.get(i).get(timeoutLimit, TimeUnit.MILLISECONDS);
              // response might be null during warm-up period when client may not be initialized
              if (response == null) {
                continue;
              }
              int code = response.getStatusCode();
              if (code != SC_OK) {
                LOGGER.warn("Heartbeat returns {} for {}", code, instanceUrl);
                health.setHostAsUnhealthy(instance);
              } else {
                health.setHostAsHealthy(instance);
              }
            } catch (ExecutionException e) {
              LOGGER.warn("Failed to execute heartbeat on {} ", instanceUrl, e.getCause());
              health.setHostAsUnhealthy(instance);
            } catch (TimeoutException e) {
              LOGGER.warn("Heartbeat timeout for {}", instanceUrl);
              health.setHostAsUnhealthy(instance);
            }
          }
          Thread.sleep(heartbeatCycleMillis);
        } catch (InterruptedException e) {
          LOGGER.info("Heartbeat thread shutting down", e);
          running = false;
        }
      }
    };
    heartBeatThread = new Thread(runnable);
  }

  @Override
  public boolean startInner() throws Exception {
    heartBeatThread.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    heartBeatThread.interrupt();
  }
}
