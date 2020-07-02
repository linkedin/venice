package com.linkedin.venice.endToEnd;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.AdminTool;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.StatTrackingStoreClient;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
  private static final String FABRIC0 = "dc-0";
  private static final String FABRIC1 = "dc-1";
  private static final boolean[] ABORT_MIGRATION_PROMPTS_OVERRIDE = {false, true, true};

  private VeniceTwoLayerMultiColoMultiClusterWrapper twoLayerMultiColoMultiClusterWrapper;
  private List<VeniceMultiClusterWrapper> multiClusterWrappers;
  private String srcClusterName;
  private String destClusterName;
  private String parentControllerUrl;
  private String childControllerUrl0;

  @BeforeClass
  public void setup() {
    Properties properties = new Properties();
    // Disable topic cleanup since parent and child are sharing the same kafka cluster.
    properties.setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    twoLayerMultiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(2, 2,
        3, 3, 1, 1, veniceProperties, true);
    multiClusterWrappers = twoLayerMultiColoMultiClusterWrapper.getClusters();
    String[] clusterNames = multiClusterWrappers.get(0).getClusterNames();
    Arrays.sort(clusterNames);
    srcClusterName = clusterNames[0];
    destClusterName = clusterNames[1];
    parentControllerUrl = twoLayerMultiColoMultiClusterWrapper.getParentControllers().stream()
        .map(veniceControllerWrapper -> veniceControllerWrapper.getControllerUrl())
        .collect(Collectors.joining(","));
    childControllerUrl0 = multiClusterWrappers.get(0).getControllerConnectString();

    // Create and populate testStore in src cluster
    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
    srcControllerClient.createNewStore(STORE_NAME, OWNER, "\"string\"", "\"string\"");
    srcControllerClient.updateStore(STORE_NAME, new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));

    // Populate store
    populateStore(parentControllerUrl, STORE_NAME, 1);

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      StoreResponse storeResponse = srcControllerClient.getStore(STORE_NAME);
      Assert.assertNotNull(storeResponse.getStore());
    });
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

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testMigrateStoreMultiDatacenter() throws Exception {
    String srcD2ServiceName = "venice-" + srcClusterName.substring(srcClusterName.length() - 1);
    String destD2ServiceName = "venice-" + destClusterName.substring(destClusterName.length() - 1);
    D2Client d2Client = D2TestUtils.getAndStartD2Client(multiClusterWrappers.get(0).getClusters().get(srcClusterName).getZk().getAddress());
    AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(ClientConfig
        .defaultGenericClientConfig(STORE_NAME)
        .setD2ServiceName(srcD2ServiceName)
        .setD2Client(d2Client));

    // Force refreshing router repositories to ensure that client connects to the correct cluster during the initialization
    refreshAllRouterMetaData();
    verifyStoreData(client);

    startMigration(parentControllerUrl);
    completeMigration();

    try {
      verifyStoreData(client);
      AbstractAvroStoreClient<String, Object> castClient =
          (AbstractAvroStoreClient<String, Object>) ((StatTrackingStoreClient<String, Object>) client).getInnerStoreClient();
      // Client d2ServiceName should be updated
      Assert.assertTrue(castClient.toString().contains(destD2ServiceName));
    } finally {
      endMigration(parentControllerUrl);
    }
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testMigrateStoreWithPushesAndUpdatesMultiDatacenter() throws Exception {
    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
    ControllerClient destControllerClient = new ControllerClient(destClusterName, parentControllerUrl);

    startMigration(parentControllerUrl);
    // Ensure migration status is updated in source parent controller
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
      Assert.assertTrue(srcControllerClient.getStore(STORE_NAME).getStore().isMigrating());
    });

    // Push v2
    populateStore(parentControllerUrl, STORE_NAME, 2);
    // Update store
    srcControllerClient.updateStore(STORE_NAME, new UpdateStoreQueryParams().setOwner(NEW_OWNER));

    completeMigration();

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Largest used version number in dest cluster store should be 2
      StoreResponse storeResponse = destControllerClient.getStore(STORE_NAME);
      int largestUsedVersionNumber = storeResponse.getStore().getLargestUsedVersionNumber();
      Assert.assertEquals(largestUsedVersionNumber, 2);

      // Largest used version number in src cluster store should be 2
      storeResponse = srcControllerClient.getStore(STORE_NAME);
      largestUsedVersionNumber = storeResponse.getStore().getLargestUsedVersionNumber();
      Assert.assertEquals(largestUsedVersionNumber, 2);

      // Owner of dest cluster store should be updated
      storeResponse = destControllerClient.getStore(STORE_NAME);
      String owner = storeResponse.getStore().getOwner();
      Assert.assertEquals(owner, NEW_OWNER);

      // Owner of src cluster store should be updated
      storeResponse = srcControllerClient.getStore(STORE_NAME);
      owner = storeResponse.getStore().getOwner();
      Assert.assertEquals(owner, NEW_OWNER);
    });

    endMigration(parentControllerUrl);
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testCompleteMigrationMultiDatacenter() throws Exception {
    ControllerClient srcParentControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);

    startMigration(parentControllerUrl);
    String[] completeMigration0 = {"--complete-migration",
        "--url", parentControllerUrl,
        "--store", STORE_NAME,
        "--cluster-src", srcClusterName,
        "--cluster-dest", destClusterName,
        "--fabric", FABRIC0};

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Store discovery should point to the new cluster in fabric0
      AdminTool.main(completeMigration0);
      Map<String, String> childClusterMap = srcParentControllerClient.listChildControllers(srcClusterName).getChildClusterMap();
      ControllerClient srcChildControllerClient = new ControllerClient(srcClusterName, childClusterMap.get(FABRIC0));
      ControllerResponse discoveryResponse = srcChildControllerClient.discoverCluster(STORE_NAME);
      Assert.assertEquals(discoveryResponse.getCluster(), destClusterName);

      // Store discovery should still point to the old cluster in fabric1
      srcChildControllerClient = new ControllerClient(srcClusterName, childClusterMap.get(FABRIC1));
      discoveryResponse = srcChildControllerClient.discoverCluster(STORE_NAME);
      Assert.assertEquals(discoveryResponse.getCluster(), srcClusterName);
    });

    String[] completeMigration1 = {"--complete-migration",
        "--url", parentControllerUrl,
        "--store", STORE_NAME,
        "--cluster-src", srcClusterName,
        "--cluster-dest", destClusterName,
        "--fabric", FABRIC1};

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Store discovery should point to the new cluster in fabric1
      AdminTool.main(completeMigration1);
      Map<String, String> childClusterMap = srcParentControllerClient.listChildControllers(srcClusterName).getChildClusterMap();
      ControllerClient srcChildControllerClient = new ControllerClient(srcClusterName, childClusterMap.get(FABRIC1));
      ControllerResponse discoveryResponse = srcChildControllerClient.discoverCluster(STORE_NAME);
      Assert.assertEquals(discoveryResponse.getCluster(), destClusterName);

      // Store discovery should point to the new cluster in parent
      discoveryResponse = srcParentControllerClient.discoverCluster(STORE_NAME);
      Assert.assertEquals(discoveryResponse.getCluster(), destClusterName);
    });

    endMigration(parentControllerUrl);
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testAbortMigrationMultiDatacenter() throws Exception {
    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
    ControllerClient destControllerClient = new ControllerClient(destClusterName, parentControllerUrl);

    // Test 1. Abort immediately
    startMigration(parentControllerUrl);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
      Assert.assertTrue(srcControllerClient.getStore(STORE_NAME).getStore().isMigrating());
    });

    abortMigration(parentControllerUrl, false);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      checkStatusAfterAbortMigration(srcControllerClient, destControllerClient);
    });
    checkChildStatusAfterAbortMigration();

    // Test 2. Migrate again and wait until cloned store being created in both parent and child
    // cluster discovery is not updated yet
    startMigration(parentControllerUrl);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
      Assert.assertNotNull(destControllerClient.getStore(STORE_NAME).getStore());
      for (VeniceMultiClusterWrapper multiClusterWrapper : multiClusterWrappers) {
        ControllerClient destChildControllerClient = new ControllerClient(destClusterName, multiClusterWrapper.getControllerConnectString());
        Assert.assertNotNull(destChildControllerClient.getStore(STORE_NAME).getStore());
      }
    });

    abortMigration(parentControllerUrl, false);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      checkStatusAfterAbortMigration(srcControllerClient, destControllerClient);
    });
    checkChildStatusAfterAbortMigration();

    // Test 3. Migrate again and wait until cloned store being created in both parent and child
    // cluster discovery points to dest cluster in both parent and child
    startMigration(parentControllerUrl);
    completeMigration();

    abortMigration(parentControllerUrl, true);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      checkStatusAfterAbortMigration(srcControllerClient, destControllerClient);
    });
    checkChildStatusAfterAbortMigration();
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testMigrateStoreSingleDatacenter() throws Exception {
    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, childControllerUrl0);
    ControllerClient destControllerClient = new ControllerClient(destClusterName, childControllerUrl0);

    // Ensure store in child fabric is not migrating. StartMigration will terminate early is store is in migrating.
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      Assert.assertFalse(srcControllerClient.getStore(STORE_NAME).getStore().isMigrating());
    });

    // Preforms store migration on child controller
    startMigration(childControllerUrl0);
    String[] completeMigration0 = {"--complete-migration",
        "--url", childControllerUrl0,
        "--store", STORE_NAME,
        "--cluster-src", srcClusterName,
        "--cluster-dest", destClusterName,
        "--fabric", FABRIC0};

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Store discovery should point to the new cluster in datacenter0
      AdminTool.main(completeMigration0);
      ControllerResponse discoveryResponse = destControllerClient.discoverCluster(STORE_NAME);
      Assert.assertEquals(discoveryResponse.getCluster(), destClusterName);
    });

    // Test single datacenter abort migration
    abortMigration(childControllerUrl0, true);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      checkStatusAfterAbortMigration(srcControllerClient, destControllerClient);
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

  private void startMigration(String controllerUrl) throws Exception {
    String[] startMigrationArgs = {"--migrate-store",
        "--url", controllerUrl,
        "--store", STORE_NAME,
        "--cluster-src", srcClusterName,
        "--cluster-dest", destClusterName};
    AdminTool.main(startMigrationArgs);
  }

  private void completeMigration() throws Exception {
    String[] completeMigration0 = {"--complete-migration",
        "--url", parentControllerUrl,
        "--store", STORE_NAME,
        "--cluster-src", srcClusterName,
        "--cluster-dest", destClusterName,
        "--fabric", FABRIC0};
    String[] completeMigration1 = {"--complete-migration",
        "--url", parentControllerUrl,
        "--store", STORE_NAME,
        "--cluster-src", srcClusterName,
        "--cluster-dest", destClusterName,
        "--fabric", FABRIC1};

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Store discovery should point to the new cluster after completing migration
      AdminTool.main(completeMigration0);
      AdminTool.main(completeMigration1);
      ControllerClient destControllerClient = new ControllerClient(destClusterName, parentControllerUrl);
      ControllerResponse discoveryResponse = destControllerClient.discoverCluster(STORE_NAME);
      Assert.assertEquals(discoveryResponse.getCluster(), destClusterName);
    });
  }

  private void endMigration(String controllerUrl) throws Exception {
    String[] endMigration = {"--end-migration",
        "--url", controllerUrl,
        "--store", STORE_NAME,
        "--cluster-src", srcClusterName,
        "--cluster-dest", destClusterName};
    AdminTool.main(endMigration);

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Store should be deleted in source cluster. Store in destination cluster should not be migrating.
      ControllerClient srcControllerClient = new ControllerClient(srcClusterName, controllerUrl);
      StoreResponse storeResponse = srcControllerClient.getStore(STORE_NAME);
      Assert.assertNull(storeResponse.getStore());
      ControllerClient destControllerClient = new ControllerClient(destClusterName, controllerUrl);
      storeResponse = destControllerClient.getStore(STORE_NAME);
      Assert.assertNotNull(storeResponse.getStore());
      Assert.assertFalse(storeResponse.getStore().isMigrating());
    });
    swapSrcAndDest();
  }

  private void swapSrcAndDest() {
    // Swap src and dest for next test
    String tmp = srcClusterName;
    srcClusterName = destClusterName;
    destClusterName = tmp;
  }

  private void verifyStoreData(AvroGenericStoreClient<String, Object> client) throws Exception {
    for (int i = 1; i <= 100; ++i) {
      String expected = "test_name_" + i;
      String actual = client.get(Integer.toString(i)).get().toString();
      Assert.assertEquals(actual, expected);
    }
  }

  private void abortMigration(String controllerUrl, boolean force) {
    AdminTool.abortMigration(controllerUrl, STORE_NAME, srcClusterName, destClusterName, force, ABORT_MIGRATION_PROMPTS_OVERRIDE);
  }

  private void checkStatusAfterAbortMigration(ControllerClient srcControllerClient, ControllerClient destControllerClient) {
    // Migration flag should be false
    // Store should be deleted in dest cluster
    // Cluster discovery should point to src cluster
    StoreResponse storeResponse = srcControllerClient.getStore(STORE_NAME);
    Assert.assertNotNull(storeResponse.getStore());
    Assert.assertFalse(storeResponse.getStore().isMigrating());
    storeResponse = destControllerClient.getStore(STORE_NAME);
    Assert.assertNull(storeResponse.getStore());
    ControllerResponse discoveryResponse = destControllerClient.discoverCluster(STORE_NAME);
    Assert.assertEquals(discoveryResponse.getCluster(), srcClusterName);
  }

  private void checkChildStatusAfterAbortMigration() {
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
      for (VeniceMultiClusterWrapper multiClusterWrapper : multiClusterWrappers) {
        ControllerClient srcChildControllerClient = new ControllerClient(srcClusterName, multiClusterWrapper.getControllerConnectString());
        ControllerClient destChildControllerClient = new ControllerClient(destClusterName, multiClusterWrapper.getControllerConnectString());
        checkStatusAfterAbortMigration(srcChildControllerClient, destChildControllerClient);
      }
    });
  }

  private void refreshAllRouterMetaData() {
    for (VeniceMultiClusterWrapper multiClusterWrapper : multiClusterWrappers) {
      for (VeniceClusterWrapper clusterWrapper : multiClusterWrapper.getClusters().values()) {
        clusterWrapper.refreshAllRouterMetaData();
      }
    }
  }
}
