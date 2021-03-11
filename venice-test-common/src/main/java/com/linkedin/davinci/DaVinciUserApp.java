package com.linkedin.davinci;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.*;
import static com.linkedin.venice.meta.IngestionMode.*;
import static com.linkedin.venice.meta.PersistenceType.*;


/**
 * DaVinciUserApp is a dummy class that spins up a Da Vinci Client and ingest data from all partitions.
 * It then sleep for preset seconds before exiting itself, which leaves enough time window for tests to peform actions and checks.
 */
public class DaVinciUserApp {
  public static void main(String[] args) throws InterruptedException, ExecutionException {
    String zkHosts = args[0];
    String baseDataPath = args[1];
    String storeName = args[2];
    int sleepSeconds = Integer.parseInt(args[3]);
    int heartbeatTimeoutSeconds = Integer.parseInt(args[4]);
    D2Client d2Client = new D2ClientBuilder()
        .setZkHosts(zkHosts)
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);

    MetricsRepository metricsRepository = new MetricsRepository();

    int applicationListenerPort = Utils.getFreePort();
    int servicePort = Utils.getFreePort();
    VeniceProperties backendConfig = new PropertyBuilder()
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(SERVER_INGESTION_MODE, ISOLATED)
        .put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, applicationListenerPort)
        .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, servicePort)
        .put(SERVER_INGESTION_ISOLATION_HEARTBEAT_TIMEOUT_MS, TimeUnit.SECONDS.toMillis(heartbeatTimeoutSeconds))
        .put(D2_CLIENT_ZK_HOSTS_ADDRESS, zkHosts)
        .build();

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig)) {
      DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig());
      client.subscribeAll().get();
      logger.info("Da Vinci client finished subscription.");
      // This guarantees this dummy app process can finish in time and will not lingering forever.
      Thread.sleep(TimeUnit.SECONDS.toMillis(sleepSeconds));
      logger.info("Da Vinci user app finished sleeping.");
    }
  }
}