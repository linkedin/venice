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
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.TestVeniceServer;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.KeyAndValueSchemas;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestUniqueKeyCountHll {
  private static final int TEST_TIMEOUT = 120_000;
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
  @Test(timeOut = TEST_TIMEOUT)
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
        controllerClient -> TestUtils
            .waitForNonDeterministicPushCompletion(topicName, controllerClient, TEST_TIMEOUT, TimeUnit.MILLISECONDS));

    assertHllCount(topicName, expectedUniqueKeys, 0.05);
  }

  /**
   * Push records with all unique keys (no duplicates).
   * Verify HLL count matches the exact record count within 5%.
   */
  @Test(timeOut = TEST_TIMEOUT)
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
        controllerClient -> TestUtils
            .waitForNonDeterministicPushCompletion(topicName, controllerClient, TEST_TIMEOUT, TimeUnit.MILLISECONDS));

    assertHllCount(topicName, DEFAULT_USER_DATA_RECORD_COUNT, 0.05);
  }

  private void assertHllCount(String topicName, int expectedUniqueKeys, double maxErrorRate) {
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      long totalHllCount = 0;
      for (VeniceServerWrapper serverWrapper: cluster.getVeniceServers()) {
        TestVeniceServer veniceServer = serverWrapper.getVeniceServer();
        StoreIngestionTask sit = veniceServer.getKafkaStoreIngestionService().getStoreIngestionTask(topicName);
        if (sit != null) {
          totalHllCount += sit.getEstimatedUniqueIngestedKeyCount(LeaderFollowerStateType.LEADER);
        }
      }
      assertTrue(totalHllCount > 0, "HLL count should be non-zero after push");
      double errorRate = Math.abs((double) (totalHllCount - expectedUniqueKeys)) / expectedUniqueKeys;
      assertTrue(
          errorRate < maxErrorRate,
          "HLL estimate " + totalHllCount + " for " + expectedUniqueKeys + " unique keys has error "
              + String.format("%.2f%%", errorRate * 100) + " exceeding " + (maxErrorRate * 100) + "%");
    });
  }
}
