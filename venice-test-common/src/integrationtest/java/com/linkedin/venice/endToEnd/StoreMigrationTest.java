package com.linkedin.venice.endToEnd;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.AdminTool;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreMigrationResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.utils.TestPushUtils.*;

public class StoreMigrationTest {
  private static final String STORE_NAME = "testStore";
  private static final String OWNER = "";
  private static final String NEW_OWNER = "tester";
  private VeniceTwoLayerMultiColoMultiClusterWrapper twoLayerMultiColoMultiClusterWrapper;
  private List<VeniceMultiClusterWrapper> multiClusterWrappers;
  private String srcClusterName;
  private String destClusterName;
  private String parentControllerUrl;

  @BeforeClass
  public void setup() {
    Properties properties = new Properties();
    // Disable topic cleanup since parent and child are sharing the same kafka cluster.
    properties.setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    twoLayerMultiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(2, 2,
        3, 3, 1, 1, veniceProperties);
    multiClusterWrappers = twoLayerMultiColoMultiClusterWrapper.getClusters();
    String[] clusterNames = multiClusterWrappers.get(0).getClusterNames();
    Arrays.sort(clusterNames);
    srcClusterName = clusterNames[0];
    destClusterName = clusterNames[1];
    parentControllerUrl = twoLayerMultiColoMultiClusterWrapper.getParentControllers().stream()
        .map(veniceControllerWrapper -> veniceControllerWrapper.getControllerUrl())
        .collect(Collectors.joining(","));

    // Create and populate testStore in src cluster
    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
    srcControllerClient.createNewStore(STORE_NAME, OWNER, "\"string\"", "\"string\"");
    srcControllerClient.updateStore(STORE_NAME, new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));

    // Populate store
    populateStore(parentControllerUrl, STORE_NAME, 1);
  }

  @AfterClass
  public void cleanup() throws Exception {
    swapSrcAndDest();
    String[] deleteStoreDest = {"--delete-store",
        "--url", parentControllerUrl,
        "--cluster", destClusterName,
        "--store", STORE_NAME};
    AdminTool.main(deleteStoreDest);
    twoLayerMultiColoMultiClusterWrapper.close();
  }

  @Test
  public void testMigrateStoreWithExistingVersions() throws Exception {
    String[] startMigrationArgs = {"--migrate-store",
        "--url", parentControllerUrl,
        "--store", STORE_NAME,
        "--cluster-src", srcClusterName,
        "--cluster-dest", destClusterName};
    AdminTool.main(startMigrationArgs);

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Store discovery should point to the new cluster after migration
      ControllerClient destControllerClient = new ControllerClient(destClusterName, parentControllerUrl);
      ControllerResponse discoveryResponse = destControllerClient.discoverCluster(STORE_NAME);
      String newCluster = discoveryResponse.getCluster();
      Assert.assertEquals(newCluster, destClusterName);
    });

    endMigration();
  }

  @Test
  public void testMigrateStoreWithNewPushes() throws Exception {
    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
    ControllerClient destControllerClient = new ControllerClient(destClusterName, parentControllerUrl);

    StoreMigrationResponse storeMigrationResponse = srcControllerClient.migrateStore(STORE_NAME, destClusterName);
    Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());
    // Ensure migration status is updated in source parent controller
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
      Assert.assertTrue(srcControllerClient.getStore(STORE_NAME).getStore().isMigrating());
    });

    // Push v2
    populateStore(parentControllerUrl, STORE_NAME, 2);

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Store discovery should point to the new cluster after migration
      ControllerResponse discoveryResponse = destControllerClient.discoverCluster(STORE_NAME);
      String newCluster = discoveryResponse.getCluster();
      Assert.assertEquals(newCluster, destClusterName);

      // Largest used version number in dest cluster store should be 2
      StoreResponse storeResponse = destControllerClient.getStore(STORE_NAME);
      int largestUsedVersionNumber = storeResponse.getStore().getLargestUsedVersionNumber();
      Assert.assertEquals(largestUsedVersionNumber, 2);

      // Largest used version number in src cluster store should be 2
      storeResponse = srcControllerClient.getStore(STORE_NAME);
      largestUsedVersionNumber = storeResponse.getStore().getLargestUsedVersionNumber();
      Assert.assertEquals(largestUsedVersionNumber, 2);
    });

    endMigration();
  }

  @Test
  public void testMigrateStoreWithWithStoreUpdates() throws Exception {
    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
    ControllerClient destControllerClient = new ControllerClient(destClusterName, parentControllerUrl);

    StoreMigrationResponse storeMigrationResponse = srcControllerClient.migrateStore(STORE_NAME, destClusterName);
    Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());
    // Ensure migration status is updated in source parent controller
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
      Assert.assertTrue(srcControllerClient.getStore(STORE_NAME).getStore().isMigrating());
    });

    String cluster = srcControllerClient.discoverCluster(STORE_NAME).getCluster();
    String[] updateStoreArgs = {"--update-store",
        "--url", parentControllerUrl,
        "--cluster", cluster,
        "--store", STORE_NAME,
        "--owner", NEW_OWNER};
    AdminTool.main(updateStoreArgs);

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Store discovery should point to the new cluster after migration
      ControllerResponse discoveryResponse = destControllerClient.discoverCluster(STORE_NAME);
      String newCluster = discoveryResponse.getCluster();
      Assert.assertEquals(newCluster, destClusterName);

      // Owner of dest cluster store should be updated
      StoreResponse storeResponse = destControllerClient.getStore(STORE_NAME);
      String owner = storeResponse.getStore().getOwner();
      Assert.assertEquals(owner, NEW_OWNER);

      // Owner of src cluster store should be updated
      storeResponse = srcControllerClient.getStore(STORE_NAME);
      owner = storeResponse.getStore().getOwner();
      Assert.assertEquals(owner, NEW_OWNER);
    });

    endMigration();
  }

  @Test
  public void testMigrateStoreWithClientAutoUpdate() throws Exception {
    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
    ControllerClient destControllerClient = new ControllerClient(destClusterName, parentControllerUrl);

    D2Client d2Client = D2TestUtils.getAndStartD2Client(multiClusterWrappers.get(0).getClusters().get(srcClusterName).getZk().getAddress());
    AvroGenericStoreClient<String, String> client = ClientFactory.getAndStartGenericAvroClient(ClientConfig
        .defaultGenericClientConfig(STORE_NAME)
        .setD2ServiceName(D2TestUtils.DEFAULT_TEST_SERVICE_NAME)
        .setD2Client(d2Client));

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      String value = client.get("key").get();
      Assert.assertNull(value);
    });

    // Migrate store from src to dest cluster
    StoreMigrationResponse storeMigrationResponse = srcControllerClient.migrateStore(STORE_NAME, destClusterName);
    Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Store discovery should point to the new cluster after migration
      ControllerResponse discoveryResponse = destControllerClient.discoverCluster(STORE_NAME);
      String newCluster = discoveryResponse.getCluster();
      Assert.assertEquals(newCluster, destClusterName);
    });

    endMigration();

    // No exception since request will be redirected
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      String value = client.get("key").get();
      Assert.assertNull(value);
    });
  }

  private void populateStore(String controllerUrl, String storeName, int expectedVersion) {
    Utils.thisIsLocalhost();
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    Properties props = defaultH2VProps(controllerUrl, inputDirPath, storeName);

    try (KafkaPushJob job = new KafkaPushJob("Test push job", props)) {
      writeSimpleAvroFileWithUserSchema(inputDir);
      job.run();

      // Verify job properties
      Assert.assertEquals(job.getKafkaTopic(), Version.composeKafkaTopic(storeName, expectedVersion));
      Assert.assertEquals(job.getInputDirectory(), inputDirPath);
      String schema = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
      Assert.assertEquals(job.getFileSchemaString(), schema);
      Assert.assertEquals(job.getKeySchemaString(), STRING_SCHEMA);
      Assert.assertEquals(job.getValueSchemaString(), STRING_SCHEMA);
      Assert.assertEquals(job.getInputFileDataSize(), 3872);
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  private void endMigration() throws Exception {
    String[] endMigration = {"--end-migration",
        "--url", parentControllerUrl,
        "--store", STORE_NAME,
        "--cluster-src", srcClusterName,
        "--cluster-dest", destClusterName};
    AdminTool.main(endMigration);

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Store should be deleted in source cluster
      ControllerClient srcControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
      StoreResponse storeResponse = srcControllerClient.getStore(STORE_NAME);
      Assert.assertNull(storeResponse.getStore());
    });
    swapSrcAndDest();
  }

  private void swapSrcAndDest() {
    // Swap src and dest for next test
    String tmp = srcClusterName;
    srcClusterName = destClusterName;
    destClusterName = tmp;
  }
}
