package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_FABRIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test for repush with cross-fabric input and Native Replication.
 *
 * <p>Tests that a repush reading from a different fabric than the NR source correctly writes
 * data to the NR source fabric's Kafka. Parameterized for both Spark and Hadoop MR engines.
 *
 * <h2>Bug (Spark only)</h2>
 * {@link DataWriterSparkJob#getKafkaInputDataFrame()} calls
 * {@link com.linkedin.venice.spark.datawriter.jobs.AbstractDataWriterSparkJob#setInputConf} with
 * {@code PUBSUB_BROKER_ADDRESS = kafkaInputBrokerUrl}, which writes to both the DataFrameReader
 * AND the SparkSession config. The SparkSession config leaks into executor broadcast properties.
 * {@code VeniceWriterFactory.lookupBrokerAddress()} checks {@code PUBSUB_BROKER_ADDRESS} before
 * {@code KAFKA_BOOTSTRAP_SERVERS}, so executors produce data to the input fabric instead of the
 * NR source fabric. The Hadoop MR path does not have this bug.
 *
 * <h2>Test design</h2>
 * <ol>
 *   <li>2 DCs with separate Kafka brokers. NR source = dc-0.</li>
 *   <li>Batch push writes records; v1 is current in both DCs.</li>
 *   <li>Repush reads v1 from dc-1 ({@code KAFKA_INPUT_FABRIC = dc-1}). The controller returns
 *       dc-0 as the NR source. The repush should write v2 data to dc-0.</li>
 *   <li>For Spark: the bug causes data to go to dc-1 → dc-0 reads return null (expected failure).
 *       For Hadoop MR: data correctly goes to dc-0 → reads succeed.</li>
 * </ol>
 */
public class TestRepushWithNativeReplicationSourceOverride extends AbstractMultiRegionTest {
  private static final Logger LOGGER = LogManager.getLogger(TestRepushWithNativeReplicationSourceOverride.class);
  private static final int TEST_TIMEOUT_MS = 3 * Time.MS_PER_MINUTE;
  private static final int RECORD_COUNT = 50;
  private static final int PARTITION_COUNT = 2;

  private String[] dcNames;

  @Override
  protected Properties getExtraServerProperties() {
    Properties props = new Properties();
    props.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    return props;
  }

  @Override
  @BeforeClass(alwaysRun = true)
  public void setUp() {
    super.setUp();
    dcNames = multiRegionMultiClusterWrapper.getChildRegionNames().toArray(new String[0]);
  }

  /**
   * @param useSparkCompute true = Spark (DataWriterSparkJob), false = Hadoop MR (default)
   */
  @Test(timeOut = TEST_TIMEOUT_MS, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testRepushWithCrossFabricInputAndNativeReplication(boolean useSparkCompute) throws Exception {
    String engine = useSparkCompute ? "Spark" : "MR";
    String clusterName = CLUSTER_NAME;
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, RECORD_COUNT);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("repush-nr-" + engine.toLowerCase() + "-test");
    String parentControllerUrls = multiRegionMultiClusterWrapper.getControllerConnectString();
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    VeniceMultiClusterWrapper dc0 = childDatacenters.get(0);
    VeniceMultiClusterWrapper dc1 = childDatacenters.get(1);

    String dc0KafkaUrl = dc0.getKafkaBrokerWrapper().getAddress();
    String dc1KafkaUrl = dc1.getKafkaBrokerWrapper().getAddress();

    LOGGER.info("[{}] dc-0 (NR source) Kafka={}, dc-1 (repush input) Kafka={}", engine, dc0KafkaUrl, dc1KafkaUrl);
    assertNotEquals(dc0KafkaUrl, dc1KafkaUrl, "DCs must have different Kafka brokers");

    // --- Step 1: Create store with NR source = dc-0, A/A enabled ---
    Properties batchProps =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    batchProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);

    createStoreForJob(
        clusterName,
        keySchemaStr,
        valueSchemaStr,
        batchProps,
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(PARTITION_COUNT)
            .setNativeReplicationEnabled(true)
            .setActiveActiveReplicationEnabled(true)
            .setNativeReplicationSourceFabric(dcNames[0])).close();

    // --- Step 2: Batch push v1 ---
    try (VenicePushJob batchPush = new VenicePushJob("Batch push v1 (" + engine + ")", batchProps)) {
      batchPush.run();
      LOGGER.info("[{}] Batch push completed. kafkaUrl={}", engine, batchPush.getKafkaUrl());
    }
    try (ControllerClient parentClient = new ControllerClient(clusterName, parentControllerUrls)) {
      waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        for (int version: parentClient.getStore(storeName).getStore().getColoToCurrentVersions().values()) {
          assertEquals(version, 1);
        }
      });
    }
    verifyDataPresent(storeName, clusterName, RECORD_COUNT, 0);
    verifyDataPresent(storeName, clusterName, RECORD_COUNT, 1);
    LOGGER.info("[{}] Batch push (v1) verified in both DCs", engine);

    // --- Step 3: Repush reading v1 from dc-1 ---
    Properties repushProps =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    repushProps.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    repushProps.put(SOURCE_KAFKA, "true");
    repushProps.put(KAFKA_INPUT_BROKER_URL, dc1KafkaUrl);
    repushProps.put(KAFKA_INPUT_FABRIC, dcNames[1]);
    repushProps.put(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5000");
    if (useSparkCompute) {
      repushProps.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    }

    LOGGER.info("[{}] Starting repush: input from dc-1 ({}), NR source dc-0 ({})", engine, dc1KafkaUrl, dc0KafkaUrl);

    try (VenicePushJob repushJob = new VenicePushJob(engine + " repush from dc-1", repushProps)) {
      repushJob.run();
      LOGGER.info("[{}] Repush completed. kafkaUrl (driver)={}", engine, repushJob.getKafkaUrl());
    }

    // --- Step 4: Wait for v2 to become current ---
    try (ControllerClient parentClient = new ControllerClient(clusterName, parentControllerUrls)) {
      waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        for (int version: parentClient.getStore(storeName).getStore().getColoToCurrentVersions().values()) {
          assertEquals(version, 2, "All DCs should be on v2 after repush");
        }
      });
    }

    // --- Step 5: Verify data ---
    if (useSparkCompute) {
      // BUG (unfixed): Spark executor writes data to dc-1 instead of dc-0 because
      // DataWriterSparkJob.getKafkaInputDataFrame() leaks PUBSUB_BROKER_ADDRESS = dc-1
      // into the SparkSession config, which overrides the correct KAFKA_BOOTSTRAP_SERVERS.
      // dc-0's version topic has only SOP/EOP control messages — no data records.
      // Reads from dc-0 return null.
      verifyDataAbsent(storeName, clusterName, 0);
      LOGGER.info("[Spark] Confirmed: dc-0 has no data after repush (PUBSUB_BROKER_ADDRESS leak bug)");
    } else {
      // Hadoop MR does not have the PUBSUB_BROKER_ADDRESS leak.
      // Data is correctly written to dc-0 (NR source). All reads succeed.
      verifyDataPresent(storeName, clusterName, RECORD_COUNT, 0);
      verifyDataPresent(storeName, clusterName, RECORD_COUNT, 1);
      LOGGER.info("[MR] Repush (v2) data verified in both DCs — no bug");
    }
  }

  /**
   * Assert that all records [1..recordCount] are present and correct in the given DC.
   */
  private void verifyDataPresent(String storeName, String clusterName, int recordCount, int dcIndex) {
    VeniceMultiClusterWrapper dc = childDatacenters.get(dcIndex);
    VeniceClusterWrapper cluster = dc.getClusters().get(clusterName);
    String routerUrl = cluster.getRandomRouterURL();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      try (AvroGenericStoreClient<String, Object> client = ClientFactory
          .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        for (int i = 1; i <= recordCount; i++) {
          Object value = client.get(Integer.toString(i)).get();
          assertNotNull(
              value,
              "Key " + i + " is null in dc-" + dcIndex
                  + ". If this is dc-0 (NR source), it means the repush wrote data to the wrong Kafka "
                  + "(the input fabric dc-1 instead of NR source dc-0). Root cause: "
                  + "DataWriterSparkJob.getKafkaInputDataFrame() leaks PUBSUB_BROKER_ADDRESS into SparkSession.");
          assertEquals(value.toString(), "test_name_" + i);
        }
      }
    });
    LOGGER.info("Data present and correct in dc-{}", dcIndex);
  }

  /**
   * Assert that data is absent in the NR source DC (dc-0) after a buggy Spark repush.
   * The version topic has SOP/EOP but no data records, so reads return null.
   */
  private void verifyDataAbsent(String storeName, String clusterName, int dcIndex) {
    VeniceMultiClusterWrapper dc = childDatacenters.get(dcIndex);
    VeniceClusterWrapper cluster = dc.getClusters().get(clusterName);
    String routerUrl = cluster.getRandomRouterURL();
    try (AvroGenericStoreClient<String, Object> client = ClientFactory
        .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
      // Check a sample of keys — they should all be null because the data went to the wrong Kafka
      for (int i = 1; i <= 5; i++) {
        Object value = client.get(Integer.toString(i)).get();
        assertNull(
            value,
            "Key " + i + " should be null in dc-" + dcIndex + " due to the PUBSUB_BROKER_ADDRESS leak bug, "
                + "but got: " + value + ". If this assertion fails, the bug may have been fixed.");
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to verify data absence in dc-" + dcIndex, e);
    }
    LOGGER.info("Confirmed: data is absent in dc-{} (expected due to PUBSUB_BROKER_ADDRESS leak)", dcIndex);
  }
}
