package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_GRID_FABRIC;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestPushJobWithSourceGridFabricSelection extends AbstractMultiRegionTest {
  private static final int TEST_TIMEOUT_MS = 90 * Time.MS_PER_SECOND;

  private String[] clusterNames;
  private String[] dcNames;

  @DataProvider(name = "storeSize")
  public static Object[][] storeSize() {
    return new Object[][] { { 50, 2 } };
  }

  @Override
  protected Properties getExtraServerProperties() {
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    return serverProperties;
  }

  @Override
  @BeforeClass(alwaysRun = true)
  public void setUp() {
    super.setUp();
    clusterNames = multiRegionMultiClusterWrapper.getClusterNames();
    dcNames = multiRegionMultiClusterWrapper.getChildRegionNames().toArray(new String[0]);
  }

  /**
   * Verify that grid source fabric config overrides the store level NR source fabric
   */
  @Test(timeOut = TEST_TIMEOUT_MS, dataProvider = "storeSize")
  public void testPushJobWithSourceGridFabricSelection(int recordCount, int partitionCount) throws Exception {
    String clusterName = clusterNames[0];
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, recordCount);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    String parentControllerUrls = multiRegionMultiClusterWrapper.getControllerConnectString();

    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    props.put(SOURCE_GRID_FABRIC, dcNames[1]);

    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    // Enable L/F and native replication features.
    // Enable A/A in parent region and 1 child region only
    // The NR source fabric cluster level config is dc-0 by default.
    UpdateStoreQueryParams updateStoreParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(partitionCount)
            .setNativeReplicationEnabled(true)
            .setNativeReplicationSourceFabric(dcNames[0]);

    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams).close();

    updateStoreParams = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true);
    // get dc-0 and enable A/A replication to verify the push job source fabric selection later.
    try (ControllerClient dc0ControllerClient = new ControllerClient(
        clusterName,
        multiRegionMultiClusterWrapper.getChildRegions().get(0).getControllerConnectString())) {
      TestUtils.assertCommand(
          dc0ControllerClient.updateStore(storeName, updateStoreParams),
          "Failed to enable A/A replication on dc-0 from child controller");
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        // Verify A/A replication is enabled on dc-0
        Assert.assertTrue(dc0ControllerClient.getStore(storeName).getStore().isActiveActiveReplicationEnabled());
      });
    }

    // verify A/A is not enabled on dc-1
    try (ControllerClient dc1ControllerClient = new ControllerClient(
        clusterName,
        multiRegionMultiClusterWrapper.getChildRegions().get(1).getControllerConnectString())) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        Assert.assertFalse(dc1ControllerClient.getStore(storeName).getStore().isActiveActiveReplicationEnabled());
      });
    }

    // Start a batch push specifying SOURCE_GRID_FABRIC as dc-1. This should be ignored as A/A is not enabled in all
    // region.
    try (VenicePushJob job = new VenicePushJob("Test push job 1", props)) {
      job.run();
      // Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
      Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
    }

    updateStoreParams = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true);
    // Enable A/A in all regions now start another batch push. Verify the batch push source address is dc-1.
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerUrls)) {
      TestUtils.assertCommand(
          parentControllerClient.updateStore(storeName, updateStoreParams),
          "Failed to enable A/A replication on all regions from parent controller");
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
