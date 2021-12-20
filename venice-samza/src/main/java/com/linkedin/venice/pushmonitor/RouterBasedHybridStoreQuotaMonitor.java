package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.routerapi.HybridStoreQuotaStatusResponse;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * This push monitor is able to query hybrid store quota status from routers; it will be built for STREAM job.
 */
public class RouterBasedHybridStoreQuotaMonitor implements Closeable {
  private static final Logger logger = LogManager.getLogger(RouterBasedHybridStoreQuotaMonitor.class);

  private static final int POLL_CYCLE_DELAY_MS = 10000;
  private static final long POLL_TIMEOUT_MS = 10000L;

  private final String resourceName;
  private final ExecutorService executor;

  private final HybridQuotaMonitorTask hybridQuotaMonitorTask;
  private HybridStoreQuotaStatus currentStatus = HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED;

  public RouterBasedHybridStoreQuotaMonitor(D2TransportClient transportClient, String resourceName) {
    this.resourceName = resourceName;
    executor = Executors.newSingleThreadExecutor(new DaemonThreadFactory("RouterBasedHybridQuotaMonitor"));
    hybridQuotaMonitorTask = new HybridQuotaMonitorTask(transportClient, resourceName, this);
  }

  public void start() {
    executor.submit(hybridQuotaMonitorTask);
  }

  @Override
  public void close() {
    hybridQuotaMonitorTask.close();
  }

  public void setCurrentStatus(HybridStoreQuotaStatus currentStatus) {
    this.currentStatus = currentStatus;
  }

  public HybridStoreQuotaStatus getCurrentStatus() {
    return this.currentStatus;
  }

  private static class HybridQuotaMonitorTask implements Runnable, Closeable {
    private static ObjectMapper mapper = new ObjectMapper().disable(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);

    private final AtomicBoolean isRunning;
    private final String storeName;
    private final D2TransportClient transportClient;
    private final String requestPath;
    private final RouterBasedHybridStoreQuotaMonitor hybridStoreQuotaMonitorService;

    public HybridQuotaMonitorTask(D2TransportClient transportClient, String resourceName,
        RouterBasedHybridStoreQuotaMonitor hybridStoreQuotaMonitorService) {
      this.transportClient = transportClient;
      if (Version.isVersionTopic(resourceName)) {
        this.storeName = Version.parseStoreFromKafkaTopicName(resourceName);
        this.requestPath = buildStreamReprocessingHybridStoreQuotaRequestPath(resourceName);
      } else {
        this.storeName = resourceName;
        this.requestPath = buildStreamHybridStoreQuotaRequestPath(storeName);
      }

      this.hybridStoreQuotaMonitorService = hybridStoreQuotaMonitorService;
      this.isRunning = new AtomicBoolean(true);
    }

    @Override
    public void run() {
      logger.info("Running " + this.getClass().getSimpleName());
      while (isRunning.get()) {
        try {
          // Get hybrid store quota status
          CompletableFuture<TransportClientResponse> responseFuture = transportClient.get(requestPath);
          TransportClientResponse response = responseFuture.get(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          HybridStoreQuotaStatusResponse quotaStatusResponse = mapper.readValue(response.getBody(), HybridStoreQuotaStatusResponse.class);
          if (quotaStatusResponse.isError()) {
            logger.error("Router was not able to get hybrid quota status: " + quotaStatusResponse.getError());
            continue;
          }
          hybridStoreQuotaMonitorService.setCurrentStatus(quotaStatusResponse.getQuotaStatus());
          switch (quotaStatusResponse.getQuotaStatus()) {
            case QUOTA_VIOLATED:
              logger.info("Hybrid job failed with quota violation for store: " + storeName);
              break;
            default:
              logger.info("Current hybrid job state: " + quotaStatusResponse.getQuotaStatus() + " for store: " + storeName);
          }

          Utils.sleep(POLL_CYCLE_DELAY_MS);
        } catch (Exception e) {
          logger.error("Error when polling push status from router for store version: " + storeName, e);
        }
      }
    }

    @Override
    public void close() {
      isRunning.getAndSet(false);
    }

    private static String buildStreamHybridStoreQuotaRequestPath(String storeName) {
      return "stream_hybrid_store_quota" + "/" + storeName;
    }
    private static String buildStreamReprocessingHybridStoreQuotaRequestPath(String topicName) {
      return "stream_reprocessing_hybrid_store_quota" + "/" + topicName;
    }
  }
}

