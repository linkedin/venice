package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_UNIQUE_INGESTED_KEY_COUNT_HLL_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_UNIQUE_INGESTED_KEY_COUNT_HLL_LOG2K;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithDuplicateKey;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ALLOW_DUPLICATE_KEY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SPARK_NATIVE_INPUT_FORMAT_ENABLED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.TestVeniceServer;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.KeyAndValueSchemas;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestUniqueKeyCountHll {
  private static final int TEST_TIMEOUT_MS = 120_000;
  private VeniceClusterWrapper cluster;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(0)
        .numberOfRouters(1)
        .replicationFactor(1)
        .build();
    cluster = ServiceFactory.getVeniceCluster(options);

    Properties serverProperties = new Properties();
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_UNIQUE_INGESTED_KEY_COUNT_HLL_ENABLED, "true");
    serverProperties.setProperty(SERVER_UNIQUE_INGESTED_KEY_COUNT_HLL_LOG2K, "13");
    serverProperties.setProperty(VeniceMetricsConfig.OTEL_VENICE_METRICS_ENABLED, "true");
    serverProperties.setProperty(ConfigKeys.SERVER_INGESTION_OTEL_STATS_ENABLED, "true");
    cluster.addVeniceServer(new Properties(), serverProperties);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  /**
   * Push 100 records with duplicate keys (every 10th record reuses key "0"),
   * so there are 91 unique keys. Verify the HLL on the server deduplicates
   * and reports within 5% of the actual unique count.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testHllDeduplicatesDuringBatchPush() throws Exception {
    // writeSimpleAvroFileWithDuplicateKey writes 100 records: keys 0,10,20,...,90 all map to "0"
    // Unique keys = 91 ("0","1","2",...,"9","11",...,"99")
    int expectedUniqueKeys = 91;
    File inputDir = getTempDataDirectory();
    KeyAndValueSchemas schemas = new KeyAndValueSchemas(writeSimpleAvroFileWithDuplicateKey(inputDir));

    String storeName = Utils.getUniqueString("hll-dedup-test");
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(cluster, inputDirPath, storeName);
    props.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    props.setProperty(SPARK_NATIVE_INPUT_FORMAT_ENABLED, "true");
    props.setProperty(ALLOW_DUPLICATE_KEY, "true");

    createStoreForJob(
        cluster.getClusterName(),
        schemas.getKey().toString(),
        schemas.getValue().toString(),
        props,
        new UpdateStoreQueryParams()).close();

    IntegrationTestPushUtils.runVPJ(props);

    String topicName = Version.composeKafkaTopic(storeName, 1);
    cluster.useControllerClient(
        controllerClient -> TestUtils.waitForNonDeterministicPushCompletion(
            topicName,
            controllerClient,
            TEST_TIMEOUT_MS,
            TimeUnit.MILLISECONDS));

    assertHllCount(storeName, topicName, expectedUniqueKeys, 0.01);
  }

  /**
   * Push records with all unique keys (no duplicates).
   * Verify HLL count matches the exact record count within 5%.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testHllAccuracyWithUniqueKeys() throws Exception {
    File inputDir = getTempDataDirectory();
    KeyAndValueSchemas schemas = new KeyAndValueSchemas(writeSimpleAvroFileWithStringToStringSchema(inputDir));

    String storeName = Utils.getUniqueString("hll-unique-test");
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(cluster, inputDirPath, storeName);
    props.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    props.setProperty(SPARK_NATIVE_INPUT_FORMAT_ENABLED, "true");

    createStoreForJob(
        cluster.getClusterName(),
        schemas.getKey().toString(),
        schemas.getValue().toString(),
        props,
        new UpdateStoreQueryParams()).close();

    IntegrationTestPushUtils.runVPJ(props);

    String topicName = Version.composeKafkaTopic(storeName, 1);
    cluster.useControllerClient(
        controllerClient -> TestUtils.waitForNonDeterministicPushCompletion(
            topicName,
            controllerClient,
            TEST_TIMEOUT_MS,
            TimeUnit.MILLISECONDS));

    assertHllCount(storeName, topicName, DEFAULT_USER_DATA_RECORD_COUNT, 0.01);
  }

  /**
   * Push version 1 with 50 unique keys, then push version 2 with 200 unique keys.
   * Verify HLL resets on new version — v2's count should reflect 200, not 250.
   */
  @Test(timeOut = TEST_TIMEOUT_MS * 2)
  public void testHllResetsAcrossVersionPushes() throws Exception {
    int v1KeyCount = 50;
    int v2KeyCount = 200;

    // Create store with v1 data
    File inputDir1 = getTempDataDirectory();
    KeyAndValueSchemas schemas =
        new KeyAndValueSchemas(writeSimpleAvroFileWithStringToStringSchema(inputDir1, v1KeyCount));

    String storeName = Utils.getUniqueString("hll-version-test");
    String inputDirPath1 = "file://" + inputDir1.getAbsolutePath();
    Properties props1 = defaultVPJProps(cluster, inputDirPath1, storeName);
    props1.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    props1.setProperty(SPARK_NATIVE_INPUT_FORMAT_ENABLED, "true");

    createStoreForJob(
        cluster.getClusterName(),
        schemas.getKey().toString(),
        schemas.getValue().toString(),
        props1,
        new UpdateStoreQueryParams()).close();

    // Push version 1
    IntegrationTestPushUtils.runVPJ(props1);
    String topicV1 = Version.composeKafkaTopic(storeName, 1);
    cluster.useControllerClient(
        controllerClient -> TestUtils
            .waitForNonDeterministicPushCompletion(topicV1, controllerClient, TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    assertHllCount(storeName, topicV1, v1KeyCount, 0.01);

    // Push version 2 with different (larger) key set
    File inputDir2 = getTempDataDirectory();
    writeSimpleAvroFileWithStringToStringSchema(inputDir2, v2KeyCount);
    String inputDirPath2 = "file://" + inputDir2.getAbsolutePath();
    Properties props2 = defaultVPJProps(cluster, inputDirPath2, storeName);
    props2.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    props2.setProperty(SPARK_NATIVE_INPUT_FORMAT_ENABLED, "true");

    IntegrationTestPushUtils.runVPJ(props2);
    String topicV2 = Version.composeKafkaTopic(storeName, 2);
    cluster.useControllerClient(
        controllerClient -> TestUtils
            .waitForNonDeterministicPushCompletion(topicV2, controllerClient, TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS));

    // V2 should have its own fresh HLL with ~200 keys, NOT v1's 50 + v2's 200
    assertHllCount(storeName, topicV2, v2KeyCount, 0.01);
  }

  /**
   * Push with HLL feature flag disabled. Verify HLL count is 0 on the SIT
   * and no Tehuti metric is emitted for this store.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testHllDisabledProducesNoCount() throws Exception {
    // Create a separate cluster with HLL disabled
    VeniceClusterCreateOptions disabledOptions = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(0)
        .numberOfRouters(1)
        .replicationFactor(1)
        .build();
    try (VeniceClusterWrapper disabledCluster = ServiceFactory.getVeniceCluster(disabledOptions)) {
      Properties serverProps = new Properties();
      serverProps.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
      serverProps.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
      serverProps.setProperty(SERVER_UNIQUE_INGESTED_KEY_COUNT_HLL_ENABLED, "false");
      disabledCluster.addVeniceServer(new Properties(), serverProps);

      File inputDir = getTempDataDirectory();
      KeyAndValueSchemas schemas = new KeyAndValueSchemas(writeSimpleAvroFileWithStringToStringSchema(inputDir, 50));

      String storeName = Utils.getUniqueString("hll-disabled-test");
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      Properties props = defaultVPJProps(disabledCluster, inputDirPath, storeName);
      props.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
      props.setProperty(SPARK_NATIVE_INPUT_FORMAT_ENABLED, "true");

      createStoreForJob(
          disabledCluster.getClusterName(),
          schemas.getKey().toString(),
          schemas.getValue().toString(),
          props,
          new UpdateStoreQueryParams()).close();

      IntegrationTestPushUtils.runVPJ(props);

      String topicName = Version.composeKafkaTopic(storeName, 1);
      disabledCluster.useControllerClient(
          controllerClient -> TestUtils.waitForNonDeterministicPushCompletion(
              topicName,
              controllerClient,
              TEST_TIMEOUT_MS,
              TimeUnit.MILLISECONDS));

      assertHllCountZero(disabledCluster, topicName);
    }
  }

  /**
   * Push an empty data set (0 records). Verify HLL count is 0, not negative or error.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testHllWithEmptyPush() throws Exception {
    File inputDir = getTempDataDirectory();
    KeyAndValueSchemas schemas = new KeyAndValueSchemas(writeSimpleAvroFileWithStringToStringSchema(inputDir, 0));

    String storeName = Utils.getUniqueString("hll-empty-test");
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(cluster, inputDirPath, storeName);
    props.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    props.setProperty(SPARK_NATIVE_INPUT_FORMAT_ENABLED, "true");

    createStoreForJob(
        cluster.getClusterName(),
        schemas.getKey().toString(),
        schemas.getValue().toString(),
        props,
        new UpdateStoreQueryParams()).close();

    IntegrationTestPushUtils.runVPJ(props);

    String topicName = Version.composeKafkaTopic(storeName, 1);
    cluster.useControllerClient(
        controllerClient -> TestUtils.waitForNonDeterministicPushCompletion(
            topicName,
            controllerClient,
            TEST_TIMEOUT_MS,
            TimeUnit.MILLISECONDS));

    assertHllCountZero(cluster, topicName);
  }

  /**
   * Push data, verify HLL count, stop the server, restart it, and verify the HLL
   * count is preserved. This exercises the full checkpoint persistence path:
   * syncOffset() serialize -> RocksDB metadata -> shutdown -> restart -> read OffsetRecord -> heapify()
   */
  @Test(timeOut = TEST_TIMEOUT_MS * 2)
  public void testHllSurvivesServerRestart() throws Exception {
    int keyCount = 150;
    File inputDir = getTempDataDirectory();
    KeyAndValueSchemas schemas =
        new KeyAndValueSchemas(writeSimpleAvroFileWithStringToStringSchema(inputDir, keyCount));

    String storeName = Utils.getUniqueString("hll-restart-test");
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(cluster, inputDirPath, storeName);
    props.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    props.setProperty(SPARK_NATIVE_INPUT_FORMAT_ENABLED, "true");

    createStoreForJob(
        cluster.getClusterName(),
        schemas.getKey().toString(),
        schemas.getValue().toString(),
        props,
        new UpdateStoreQueryParams()).close();

    IntegrationTestPushUtils.runVPJ(props);

    String topicName = Version.composeKafkaTopic(storeName, 1);
    cluster.useControllerClient(
        controllerClient -> TestUtils.waitForNonDeterministicPushCompletion(
            topicName,
            controllerClient,
            TEST_TIMEOUT_MS,
            TimeUnit.MILLISECONDS));

    // Verify HLL count before restart
    assertHllCount(storeName, topicName, keyCount, 0.01);

    // Stop and restart the server
    int port = cluster.getVeniceServers().get(0).getPort();
    cluster.stopVeniceServer(port);
    cluster.restartVeniceServer(port);

    // Verify HLL count survives restart (restored from checkpoint)
    assertHllCount(storeName, topicName, keyCount, 0.01);
  }

  private void assertHllCountZero(VeniceClusterWrapper targetCluster, String topicName) {
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      for (VeniceServerWrapper serverWrapper: targetCluster.getVeniceServers()) {
        TestVeniceServer veniceServer = serverWrapper.getVeniceServer();
        StoreIngestionTask sit = veniceServer.getKafkaStoreIngestionService().getStoreIngestionTask(topicName);
        if (sit != null) {
          assertEquals(sit.getEstimatedUniqueIngestedKeyCount(null), 0L, "HLL should be 0 for topic " + topicName);
        }
      }
    });
  }

  private void assertHllCount(String storeName, String topicName, int expectedUniqueKeys, double maxErrorRate) {
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Verify SIT HLL count directly (use null filter = all replicas, since in test the
      // single server may be leader or follower depending on timing)
      long totalHllCount = 0;
      for (VeniceServerWrapper serverWrapper: cluster.getVeniceServers()) {
        TestVeniceServer veniceServer = serverWrapper.getVeniceServer();
        StoreIngestionTask sit = veniceServer.getKafkaStoreIngestionService().getStoreIngestionTask(topicName);
        if (sit != null) {
          totalHllCount += sit.getEstimatedUniqueIngestedKeyCount(null);
        }
      }
      assertTrue(totalHllCount > 0, "HLL count should be non-zero after push");

      // Verify OTel metric value — filter by store name and CURRENT role for exact match
      long otelTotal = 0;
      AttributeKey<String> storeKey = AttributeKey.stringKey("venice.store.name");
      AttributeKey<String> roleKey = AttributeKey.stringKey("venice.version.role");
      for (VeniceServerWrapper sw: cluster.getVeniceServers()) {
        MetricsRepository repo = sw.getMetricsRepository();
        if (repo instanceof VeniceMetricsRepository) {
          VeniceMetricsRepository vmr = (VeniceMetricsRepository) repo;
          InMemoryMetricReader reader =
              (InMemoryMetricReader) vmr.getVeniceMetricsConfig().getOtelAdditionalMetricsReader();
          if (reader != null) {
            for (MetricData md: reader.collectAllMetrics()) {
              if (md.getName().contains("unique_ingested_key")) {
                otelTotal += md.getLongGaugeData()
                    .getPoints()
                    .stream()
                    .filter(
                        p -> storeName.equals(p.getAttributes().get(storeKey))
                            && "current".equals(p.getAttributes().get(roleKey)))
                    .mapToLong(p -> p.getValue())
                    .sum();
              }
            }
          }
        }
      }
      double otelError = Math.abs((double) (otelTotal - expectedUniqueKeys)) / expectedUniqueKeys;
      assertTrue(
          otelError < maxErrorRate,
          "OTel metric " + otelTotal + " for " + expectedUniqueKeys + " expected keys has error "
              + String.format("%.2f%%", otelError * 100));
      double errorRate = Math.abs((double) (totalHllCount - expectedUniqueKeys)) / expectedUniqueKeys;
      assertTrue(
          errorRate < maxErrorRate,
          "HLL estimate " + totalHllCount + " for " + expectedUniqueKeys + " unique keys has error "
              + String.format("%.2f%%", errorRate * 100) + " exceeding " + (maxErrorRate * 100) + "%");
    });
  }
}
