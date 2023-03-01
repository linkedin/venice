package com.linkedin.davinci;

import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_CONNECTION_TIMEOUT_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_MODE;
import static com.linkedin.venice.meta.IngestionMode.ISOLATED;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.ServiceFactory;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * DaVinciUserApp is a dummy class that spins up a Da Vinci Client and ingest data from all partitions.
 * It then sleeps for preset seconds before exiting itself, which leaves enough time window for tests to perform actions and checks.
 */
public class DaVinciUserApp {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciUserApp.class);

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    String zkHosts = args[0];
    String baseDataPath = args[1];
    String storeName = args[2];
    int sleepSeconds = Integer.parseInt(args[3]);
    int heartbeatTimeoutSeconds = Integer.parseInt(args[4]);
    D2Client d2Client = new D2ClientBuilder().setZkHosts(zkHosts)
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);

    Map<String, Object> extraBackendConfig = new HashMap<>();
    extraBackendConfig.put(SERVER_INGESTION_MODE, ISOLATED);
    extraBackendConfig.put(SERVER_INGESTION_ISOLATION_CONNECTION_TIMEOUT_SECONDS, heartbeatTimeoutSeconds);
    extraBackendConfig.put(DATA_BASE_PATH, baseDataPath);

    DaVinciTestContext<Integer, Integer> daVinciTestContext =
        ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
            d2Client,
            new MetricsRepository(),
            Optional.empty(),
            zkHosts,
            storeName,
            new DaVinciConfig(),
            extraBackendConfig);
    try (CachingDaVinciClientFactory ignored = daVinciTestContext.getDaVinciClientFactory();
        DaVinciClient<Integer, Integer> client = daVinciTestContext.getDaVinciClient()) {
      client.subscribeAll().get();
      LOGGER.info("Da Vinci client finished subscription.");
      // This guarantees this dummy app process can finish in time and will not linger forever.
      Thread.sleep(TimeUnit.SECONDS.toMillis(sleepSeconds));
      LOGGER.info("Da Vinci user app finished sleeping.");
    }
  }
}
