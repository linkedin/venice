package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.hadoop.VenicePushJob.*;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.*;


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
import com.linkedin.venice.status.BatchJobHeartbeatConfigs;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestPushJobWithHeartBeatSystemStore {
  private static final int TEST_TIMEOUT_MS = 90_000; // 90 seconds

  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String VPJ_HEARTBEAT_STORE_NAME = "venice_system_store_BATCH_JOB_HEARTBEAT";
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0",
  // "venice-cluster1",
  private static final String VPJ_HEARTBEAT_STORE_CLUSTER = CLUSTER_NAMES[0]; // "venice-cluster0" // ...];

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
    controllerProps
        .put(BatchJobHeartbeatConfigs.HEARTBEAT_STORE_CLUSTER_CONFIG.getConfigName(), VPJ_HEARTBEAT_STORE_CLUSTER);
    controllerProps.put(BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG.getConfigName(), true);
    controllerProps.put(ENABLE_LEADER_FOLLOWER_AS_DEFAULT_FOR_ALL_STORES, true);

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
   * Verify that push job heartbeat store is able to receive heartbeat signal from store version push.
   */
  @Test(timeOut = TEST_TIMEOUT_MS, invocationCount = 5, dataProvider = "storeSize")
  public void TestPushJobWithHeartBeatStoreActiveActiveEnabled(int recordCount, int partitionCount) throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = TestWriteUtils.getTempDataDirectory();
    String parentControllerUrls =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props = TestWriteUtils.defaultVPJProps(parentControllerUrls, inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    props.put(
        VENICE_CHILD_CONTROLLER_PROP,
        childDatacenters.get(0).getControllerConnectString() + ','
            + childDatacenters.get(1).getControllerConnectString());

    UpdateStoreQueryParams updateStoreParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(partitionCount)
            .setIncrementalPushEnabled(true);

    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithUserSchema(inputDir, true, recordCount);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    try {
      createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams).close();
      childDatacenters.get(0)
          .getClusters()
          .get(clusterName)
          .useControllerClient(
              dc0Client -> childDatacenters.get(1)
                  .getClusters()
                  .get(clusterName)
                  .useControllerClient(
                      dc1Client -> TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
                        // verify the update store command has taken effect before starting the push job.
                        Assert.assertEquals(
                            dc0Client.getStore(storeName).getStore().getStorageQuotaInByte(),
                            Store.UNLIMITED_STORAGE_QUOTA);
                        Assert.assertEquals(
                            dc1Client.getStore(storeName).getStore().getStorageQuotaInByte(),
                            Store.UNLIMITED_STORAGE_QUOTA);
                      })));

      try (ControllerClient parentControllerClient =
          ControllerClient.constructClusterControllerClient(clusterName, parentControllerUrls)) {
        // Enable VPJ to send liveness heartbeat.
        props.put(BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG.getConfigName(), true);
        props.put(BatchJobHeartbeatConfigs.HEARTBEAT_STORE_NAME_CONFIG.getConfigName(), VPJ_HEARTBEAT_STORE_NAME);
        // Prevent heartbeat from being deleted when the VPJ run finishes.
        props.put(BatchJobHeartbeatConfigs.HEARTBEAT_LAST_HEARTBEAT_IS_DELETE_CONFIG.getConfigName(), false);

        parentControllerClient.updateStore(VPJ_HEARTBEAT_STORE_NAME, new UpdateStoreQueryParams().setLeaderFollowerModel(true).setActiveActiveReplicationEnabled(true));
        /*
        updateStore(
            clusterName,
            VPJ_HEARTBEAT_STORE_NAME,
            parentControllerClient,
            new UpdateStoreQueryParams().setLeaderFollowerModel(true).setActiveActiveReplicationEnabled(true));
         */

        parentControllerClient.emptyPush(VPJ_HEARTBEAT_STORE_NAME, "new push", 1L);
        childDatacenters.get(0)
            .getClusters()
            .get(clusterName)
            .useControllerClient(
                dc0Client -> childDatacenters.get(1).getClusters().get(clusterName).useControllerClient(dc1Client ->
                // verify the update store command has taken effect before starting the push job.
                NativeReplicationTestUtils
                    .verifyDCConfigNativeRepl(Arrays.asList(dc0Client, dc1Client), VPJ_HEARTBEAT_STORE_NAME, true)));

        // Start new push job and then verify heart beat store will get push job heartbeat signal.
        try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
          job.run();
          // Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
          Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
        }

        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
          // Current version should become 1
          for (int version: parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions().values()) {
            Assert.assertEquals(version, 1);
          }

          // Verify that the data are in all child fabrics including the first child fabric which consumes remotely.
          for (VeniceMultiClusterWrapper childDataCenter: childDatacenters) {
            String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
            // Verify that user store data can be read in all fabrics.
            try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
                ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
              for (int i = 1; i <= recordCount; ++i) {
                String expected = "test_name_" + i;
                String actual = client.get(Integer.toString(i)).get().toString();
                Assert.assertEquals(actual, expected);
              }
            }

            // Try to read the latest heartbeat value generated from the user store VPJ push in this fabric/datacenter.
            try (AvroGenericStoreClient<BatchJobHeartbeatKey, BatchJobHeartbeatValue> client =
                ClientFactory.getAndStartGenericAvroClient(
                    ClientConfig.defaultGenericClientConfig(VPJ_HEARTBEAT_STORE_NAME).setVeniceURL(routerUrl))) {

              final BatchJobHeartbeatKey key = new BatchJobHeartbeatKey();
              key.storeName = storeName;
              key.storeVersion = 1; // User store should be on version one.
              GenericRecord heartbeatValue = client.get(key).get();
              Assert.assertNotNull(heartbeatValue);
              Assert.assertEquals(heartbeatValue.getSchema(), BatchJobHeartbeatValue.getClassSchema());
            }
          }
        });
      }
    } finally {
      FileUtils.deleteDirectory(inputDir);
    }
  }
}
