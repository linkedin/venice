package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_KAFKA_PRODUCER_ENABLED;
import static com.linkedin.venice.hadoop.VenicePushJob.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.SharedKafkaProducerConfig.SHARED_KAFKA_PRODUCER_BATCH_SIZE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * The purpose of this test is to run parallel push job in Native replication mode with shared producer pool size of 1
 * This is to make sure multiple ingestion task can use one kafka producer to produce the message on local VT and completes
 * the ingestion.
 *
 */
public class TestPushJobWithNativeReplicationSharedProducer {
  private static final Logger LOGGER = LogManager.getLogger(TestPushJobWithNativeReplicationSharedProducer.class);
  private static final int TEST_TIMEOUT = 140_000; // ms

  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  // ["venice-cluster0", "venice-cluster1", ...];

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;

  @DataProvider(name = "storeSize")
  public static Object[][] storeSize() {
    return new Object[][] { { 1000, 2 } };
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    /**
     * Reduce leader promotion delay to 3 seconds;
     * Create a testing environment with 1 parent fabric and 2 child fabrics;
     * Set server and replication factor to 2 to ensure at least 1 leader replica and 1 follower replica;
     */
    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");

    // shared producer related configs.
    serverProperties.put(SERVER_SHARED_KAFKA_PRODUCER_ENABLED, "true");
    serverProperties.put(SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER, "1");
    // this is to make sure config override works for shared producer.
    serverProperties.put(SHARED_KAFKA_PRODUCER_BATCH_SIZE, 32864);

    Properties controllerProps = new Properties();
    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        2,
        1,
        2,
        Optional.of(new VeniceProperties(controllerProps)),
        Optional.of(controllerProps),
        Optional.of(new VeniceProperties(serverProperties)),
        false);
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiRegionMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "storeSize")
  public void testNativeReplicationForBatchPush(int recordCount, int partitionCount) throws Exception {
    int storeCount = 3;
    String[] storeNames = new String[storeCount];
    Thread[] threads = new Thread[storeCount];
    Properties[] storeProps = new Properties[storeCount];
    ControllerClient[] parentControllerClients = new ControllerClient[storeCount];

    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    String parentControllerUrls = multiRegionMultiClusterWrapper.getControllerConnectString();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();

    try {
      for (int i = 0; i < storeCount; i++) {
        String storeName = Utils.getUniqueString("store");
        storeNames[i] = storeName;
        Properties props =
            IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
        storeProps[i] = props;
        props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);

        Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithUserSchema(inputDir, true, recordCount);
        String keySchemaStr = recordSchema.getField(VenicePushJob.DEFAULT_KEY_FIELD_PROP).schema().toString();
        String valueSchemaStr = recordSchema.getField(VenicePushJob.DEFAULT_VALUE_FIELD_PROP).schema().toString();

        UpdateStoreQueryParams updateStoreParams =
            new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
                .setPartitionCount(partitionCount)
                .setNativeReplicationEnabled(true);

        ControllerClient parentControllerClient = null;

        parentControllerClient = createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams);
        parentControllerClients[i] = parentControllerClient;

        try (
            ControllerClient dc0Client =
                new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
            ControllerClient dc1Client =
                new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString())) {

          // verify the update store command has taken effect before starting the push job.
          NativeReplicationTestUtils.verifyDCConfigNativeRepl(Arrays.asList(dc0Client, dc1Client), storeName, true);
        }
      }

      LOGGER.info("Finished setting up stores");
      for (int i = 0; i < storeCount; i++) {
        int id = i;
        Thread pushJobThread =
            new Thread(() -> TestWriteUtils.runPushJob("Test push job " + id, storeProps[id]), "PushJob-" + i);
        threads[i] = pushJobThread;
      }

      LOGGER.info("Starting push job threads");
      for (int i = 0; i < storeCount; i++) {
        threads[i].start();
      }

      LOGGER.info("Waiting for push job threads to complete");
      for (int i = 0; i < storeCount; i++) {
        threads[i].join(45 * 1000);
      }

      for (int i = 0; i < storeCount; i++) {
        if (threads[i].isAlive()) {
          LOGGER.info("push job thread {} didn't complete", threads[i].getName());
        } else {
          LOGGER.info("push job thread {} completed", threads[i].getName());
        }
      }

      try (ControllerClient parentControllerClient =
          ControllerClient.constructClusterControllerClient(clusterName, parentControllerUrls)) {
        for (int i = 0; i < storeCount; i++) {
          String storeName = storeNames[i];
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
            // Current version should become 1
            for (int version: parentControllerClient.getStore(storeName)
                .getStore()
                .getColoToCurrentVersions()
                .values()) {
              Assert.assertEquals(version, 1);
            }

            // Verify the data in the second child fabric which consumes remotely
            VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(NUMBER_OF_CHILD_DATACENTERS - 1);
            String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
            try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
                ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
              for (int j = 1; j <= recordCount; ++j) {
                String expected = "test_name_" + j;
                String actual = client.get(Integer.toString(j)).get().toString();
                Assert.assertEquals(actual, expected);
              }
            }
          });
        }
      }
    } finally {
      for (int i = 0; i < storeCount; i++) {
        TestUtils.shutdownThread(threads[i]);
      }

      ControllerResponse[] deleteStoreResponses = new ControllerResponse[parentControllerClients.length];
      for (int i = 0; i < storeCount; i++) {
        if (parentControllerClients[i] != null) {
          deleteStoreResponses[i] = parentControllerClients[i].disableAndDeleteStore(storeNames[i]);
          Utils.closeQuietlyWithErrorLogged(parentControllerClients[i]);
        }
      }
      for (int i = 0; i < storeCount; i++) {
        if (parentControllerClients[i] != null) {
          Assert.assertFalse(
              deleteStoreResponses[i].isError(),
              "Failed to delete the test store: " + deleteStoreResponses[i].getError());
        }
      }
      FileUtils.deleteDirectory(inputDir);
    }
  }
}
