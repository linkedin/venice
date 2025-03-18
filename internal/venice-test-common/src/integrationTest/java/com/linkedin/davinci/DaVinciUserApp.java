package com.linkedin.davinci;

import static com.linkedin.venice.CommonConfigKeys.SSL_KEYMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_TYPE;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEY_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_SECURE_RANDOM_IMPLEMENTATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_TYPE;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_ACL_ENABLED;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_MANAGER_ENABLED;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT;
import static com.linkedin.venice.ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_CONNECTION_TIMEOUT_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_MODE;
import static com.linkedin.venice.meta.IngestionMode.BUILT_IN;
import static com.linkedin.venice.meta.IngestionMode.ISOLATED;
import static com.linkedin.venice.utils.SslUtils.LOCAL_KEYSTORE_JKS;
import static com.linkedin.venice.utils.SslUtils.LOCAL_PASSWORD;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.endToEnd.TestStringRecordTransformer;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.utils.SslUtils;
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
    boolean ingestionIsolation = Boolean.parseBoolean(args[5]);
    int blobTransferServerPort = Integer.parseInt(args[6]);
    int blobTransferClientPort = Integer.parseInt(args[7]);
    String storageClass = args[8]; // DISK or MEMORY_BACKED_BY_DISK
    boolean recordTransformerEnabled = Boolean.parseBoolean(args[9]);
    boolean blobTransferDaVinciSSLEnabled = Boolean.parseBoolean(args[10]);

    D2Client d2Client = new D2ClientBuilder().setZkHosts(zkHosts)
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);

    Map<String, Object> extraBackendConfig = new HashMap<>();
    extraBackendConfig.put(SERVER_INGESTION_MODE, ingestionIsolation ? ISOLATED : BUILT_IN);
    extraBackendConfig.put(SERVER_INGESTION_ISOLATION_CONNECTION_TIMEOUT_SECONDS, heartbeatTimeoutSeconds);
    extraBackendConfig.put(DATA_BASE_PATH, baseDataPath);
    extraBackendConfig.put(DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT, blobTransferServerPort);
    extraBackendConfig.put(DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT, blobTransferClientPort);
    extraBackendConfig.put(PUSH_STATUS_STORE_ENABLED, true);
    extraBackendConfig.put(BLOB_TRANSFER_MANAGER_ENABLED, true);

    if (blobTransferDaVinciSSLEnabled) {
      extraBackendConfig.put(BLOB_TRANSFER_SSL_ENABLED, true);
      extraBackendConfig.put(BLOB_TRANSFER_ACL_ENABLED, true);

      String keyStorePath = SslUtils.getPathForResource(LOCAL_KEYSTORE_JKS);
      extraBackendConfig.put(SSL_KEYSTORE_TYPE, "JKS");
      extraBackendConfig.put(SSL_KEYSTORE_LOCATION, keyStorePath);
      extraBackendConfig.put(SSL_KEYSTORE_PASSWORD, LOCAL_PASSWORD);
      extraBackendConfig.put(SSL_TRUSTSTORE_TYPE, "JKS");
      extraBackendConfig.put(SSL_TRUSTSTORE_LOCATION, keyStorePath);
      extraBackendConfig.put(SSL_TRUSTSTORE_PASSWORD, LOCAL_PASSWORD);
      extraBackendConfig.put(SSL_KEY_PASSWORD, LOCAL_PASSWORD);
      extraBackendConfig.put(SSL_KEYMANAGER_ALGORITHM, "SunX509");
      extraBackendConfig.put(SSL_TRUSTMANAGER_ALGORITHM, "SunX509");
      extraBackendConfig.put(SSL_SECURE_RANDOM_IMPLEMENTATION, "SHA1PRNG");
    }

    // convert the storage class string to enum
    StorageClass storageClassEnum = StorageClass.valueOf(storageClass);

    DaVinciConfig daVinciConfig = new DaVinciConfig();
    daVinciConfig.setStorageClass(storageClassEnum);

    if (recordTransformerEnabled) {
      DaVinciRecordTransformerConfig recordTransformerConfig =
          new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
              .build();
      daVinciConfig.setRecordTransformerConfig(recordTransformerConfig);
    }

    DaVinciTestContext<Integer, Integer> daVinciTestContext =
        ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
            d2Client,
            new MetricsRepository(),
            Optional.empty(),
            zkHosts,
            storeName,
            daVinciConfig,
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
