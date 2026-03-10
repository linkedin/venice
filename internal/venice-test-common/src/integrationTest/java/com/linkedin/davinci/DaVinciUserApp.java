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
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
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
import io.tehuti.metrics.MetricsRepository;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * DaVinciUserApp is a dummy class that spins up a Da Vinci Client and ingest data from all partitions.
 * It then sleeps for preset seconds before exiting itself, which leaves enough time window for tests to perform actions and checks.
 */
public class DaVinciUserApp {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciUserApp.class);

  /**
   * Writes a marker file to signal that the DaVinci client is fully initialized
   * (ingestion complete + blob transfer server ready). Tests poll for this file
   * instead of using Thread.sleep.
   */
  private static void writeReadyMarker(String markerPath) {
    if (markerPath == null || markerPath.isEmpty()) {
      return;
    }
    try {
      Files.write(Paths.get(markerPath), "ready".getBytes());
      LOGGER.info("Wrote ready marker to {}", markerPath);
    } catch (IOException e) {
      LOGGER.warn("Failed to write ready marker to {}", markerPath, e);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("Expected config file path");
    }

    // Load properties from file
    Properties props = new Properties();
    try (FileInputStream fis = new FileInputStream(args[0])) {
      props.load(fis);
    }

    // Read properties
    String zkHosts = props.getProperty("zk.hosts");
    String baseDataPath = props.getProperty("base.data.path");
    String storeName = props.getProperty("store.name");
    int sleepSeconds = Integer.parseInt(props.getProperty("sleep.seconds"));
    int blobTransferServerPort = Integer.parseInt(props.getProperty("blob.transfer.server.port"));
    int blobTransferClientPort = Integer.parseInt(props.getProperty("blob.transfer.client.port"));
    String storageClass = props.getProperty("storage.class");
    boolean recordTransformerEnabled = Boolean.parseBoolean(props.getProperty("record.transformer.enabled"));
    boolean blobTransferDaVinciManagerEnabled =
        Boolean.parseBoolean(props.getProperty("blob.transfer.manager.enabled"));
    boolean batchPushReportEnabled = Boolean.parseBoolean(props.getProperty("batch.push.report.enabled"));
    String readyMarkerPath = props.getProperty("ready.marker.path");

    D2Client d2Client = new D2ClientBuilder().setZkHosts(zkHosts)
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);

    Map<String, Object> extraBackendConfig = new HashMap<>();
    extraBackendConfig.put(DATA_BASE_PATH, baseDataPath);
    extraBackendConfig.put(PUSH_STATUS_STORE_ENABLED, true);

    if (blobTransferDaVinciManagerEnabled) {
      extraBackendConfig.put(BLOB_TRANSFER_MANAGER_ENABLED, true);
      extraBackendConfig.put(DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT, blobTransferServerPort);
      extraBackendConfig.put(DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT, blobTransferClientPort);

      extraBackendConfig.put(BLOB_TRANSFER_SSL_ENABLED, true);
      extraBackendConfig.put(BLOB_TRANSFER_ACL_ENABLED, true);

      // Use the keystore path passed by the parent test JVM to avoid classpath
      // mismatch: stale fat JARs may contain a different localhost.jks than the
      // one the test JVM loaded, causing SSL handshake failures.
      String keyStorePath = props.getProperty("ssl.keystore.path");
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

    if (batchPushReportEnabled) {
      extraBackendConfig.put(DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS, "10");
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
        DaVinciTestContext.getGenericAvroDaVinciFactoryAndClientWithRetries(
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
      writeReadyMarker(readyMarkerPath);
      // This guarantees this dummy app process can finish in time and will not linger forever.
      Thread.sleep(TimeUnit.SECONDS.toMillis(sleepSeconds));
      LOGGER.info("Da Vinci user app finished sleeping.");
    }
  }
}
