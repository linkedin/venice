package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.LF_MODEL_DEPENDENCY_CHECK_DISABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_KAFKA_PRODUCER_ENABLED;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.hadoop.VenicePushJob.SOURCE_GRID_FABRIC;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestPushUtils.getTempDataDirectory;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestPushJobWithSourceGridFabricSelection {
  private static final int TEST_TIMEOUT_MS = 90_000; // 90 seconds

  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0",
                                                                                                         // "venice-cluster1",
                                                                                                         // ...];

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;

  @DataProvider(name = "storeSize")
  public static Object[][] storeSize() {
    return new Object[][] { { 50, 2 } };
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {

    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.put(SERVER_SHARED_KAFKA_PRODUCER_ENABLED, "true");
    serverProperties.put(SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER, "1");

    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 1000);
    controllerProps.put(LF_MODEL_DEPENDENCY_CHECK_DISABLED, "true");

    multiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
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
    childDatacenters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiColoMultiClusterWrapper.close();
  }

  /**
   * Verify that grid source fabric config overrides the store level NR source fabric
   */
  @Test(timeOut = TEST_TIMEOUT_MS, dataProvider = "storeSize")
  public void TestPushJobWithSourceGridFabricSelection(int recordCount, int partitionCount) throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestPushUtils.writeSimpleAvroFileWithUserSchema(inputDir, true, recordCount);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    String parentControllerUrls =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));

    // Enable NR in all colos and A/A in parent colo and 1 child colo only. The NR source fabric cluster level config is
    // dc-0 by default.
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerUrls)) {
      Assert.assertFalse(
          parentControllerClient
              .configureNativeReplicationForCluster(
                  true,
                  VeniceUserStoreType.BATCH_ONLY.toString(),
                  Optional.empty(),
                  Optional.of("dc-parent-0.parent,dc-0,dc-1"))
              .isError());
      Assert.assertFalse(
          parentControllerClient
              .configureActiveActiveReplicationForCluster(
                  true,
                  VeniceUserStoreType.BATCH_ONLY.toString(),
                  Optional.of("dc-parent-0.parent,dc-0"))
              .isError());
    }

    Properties props = TestPushUtils.defaultVPJProps(parentControllerUrls, inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    props.put(SOURCE_GRID_FABRIC, "dc-1");

    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    // Enable L/F and native replication features.
    UpdateStoreQueryParams updateStoreParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(partitionCount)
            .setLeaderFollowerModel(true)
            .setNativeReplicationEnabled(true)
            .setNativeReplicationSourceFabric("dc-0");

    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams).close();

    // Start a batch push specifying SOURCE_GRID_FABRIC as dc-1. This should be ignored as A/A is not enabled in all
    // colo.
    try (VenicePushJob job = new VenicePushJob("Test push job 1", props)) {
      job.run();
      // Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
      Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
    }

    // Enable A/A in all colo now start another batch push. Verify the batch push source address is dc-1.
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerUrls)) {
      // Enable hybrid config, Leader/Follower state model and A/A replication policy
      Assert.assertFalse(
          parentControllerClient
              .configureActiveActiveReplicationForCluster(
                  true,
                  VeniceUserStoreType.BATCH_ONLY.toString(),
                  Optional.of("dc-parent-0.parent,dc-0,dc-1"))
              .isError());
    }

    try (VenicePushJob job = new VenicePushJob("Test push job 2", props)) {
      job.run();
      // Verify the kafka URL being returned to the push job is the same as dc-1 kafka url.
      Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(1).getKafkaBrokerWrapper().getAddress());
    }

    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerUrls)) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        // Current version should become 2
        for (int version: parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions().values()) {
          Assert.assertEquals(version, 2);
        }

        // Verify the data in the first child fabric which consumes remotely
        VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(0);
        String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
        try (AvroGenericStoreClient<String, Object> client = ClientFactory
            .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
          for (int i = 1; i <= recordCount; ++i) {
            String expected = "test_name_" + i;
            String actual = client.get(Integer.toString(i)).get().toString();
            Assert.assertEquals(actual, expected);
          }
        }
      });
    }
  }
}
