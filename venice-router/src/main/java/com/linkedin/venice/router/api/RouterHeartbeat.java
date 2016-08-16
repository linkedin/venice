package com.linkedin.venice.router.api;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.service.AbstractVeniceService;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.log4j.Logger;

import static org.apache.http.HttpStatus.SC_OK;


public class RouterHeartbeat extends AbstractVeniceService {

  private final Thread heartBeatThread;
  private static final Logger logger = Logger.getLogger(RouterHeartbeat.class);


  public RouterHeartbeat(Map<Instance, CloseableHttpAsyncClient> clientPool, VeniceHostHealth health, long cycleTime, TimeUnit cycleTimeUnits, int heartbeatTimeoutMillis){
    Runnable runnable = () -> {
      final RequestConfig requestConfig = RequestConfig.custom()
          .setSocketTimeout(heartbeatTimeoutMillis)
          .setConnectTimeout(heartbeatTimeoutMillis)
          .setConnectionRequestTimeout(heartbeatTimeoutMillis)
          .build();
      try {
        for (Instance instance : clientPool.keySet()) {
          CloseableHttpAsyncClient client = clientPool.get(instance);
          final HttpGet get = new HttpGet(instance.getUrl() + "/" + QueryAction.HEALTH.toString().toLowerCase());
          get.setConfig(requestConfig);
          try {
            HttpResponse response = client.execute(get, null).get((long)(heartbeatTimeoutMillis * 1.1), TimeUnit.MILLISECONDS); /* Socket timeout should hit first */
            int code = response.getStatusLine().getStatusCode();
            if (code != SC_OK){
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
        logger.info("Heartbeat thread shutting down");
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
