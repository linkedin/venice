package com.linkedin.venice.endToEnd;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.AdminTool;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.StatTrackingStoreClient;
import com.linkedin.venice.common.MetadataStoreUtils;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.MirrorMakerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.systemstore.schemas.StoreAttributes;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataKey;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataValue;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.system.store.MetaStoreWriter.*;
import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;

/**
 * The suite includes integration tests for all store migration tools, including migrate-store, complete-migration,
 * end-migration and abort-migration. It tests both multi-colo and single-colo store migration.
 *
 * The test store created in {@link StoreMigrationTest#setup()} is a hybrid store and is used by all tests to reduce
 * the running time of this suite. When each migration ends, source cluster name and dest cluster name are swapped to
 * prepare for the next test.
 *
 * The suite also uses constructor {@link StoreMigrationTest#StoreMigrationTest(boolean)} to test both Online/Offline
 * model and Leader/Follower model.
 */
@Test(groups = "flaky")
public class StoreMigrationTest {
  private static final String STORE_NAME = "testStore";
  private static final String OWNER = "";
  private static final String NEW_OWNER = "tester";
  private static final String FABRIC0 = "dc-0";
  private static final String FABRIC1 = "dc-1";
  private static final boolean[] ABORT_MIGRATION_PROMPTS_OVERRIDE = {false, true, true};
  private static final int STREAMING_RECORD_SIZE = 1024;

  private VeniceTwoLayerMultiColoMultiClusterWrapper twoLayerMultiColoMultiClusterWrapper;
  private List<VeniceMultiClusterWrapper> multiClusterWrappers;
  private String srcClusterName;
  private String destClusterName;
  private String parentControllerUrl;
  private String childControllerUrl0;
  private int zkSharedStoreVersion = 1;
  private boolean isLeaderFollowerModel;

  @Factory(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public StoreMigrationTest(boolean isLeaderFollowerModel) {
    this.isLeaderFollowerModel = isLeaderFollowerModel;
  }

  @BeforeClass(timeOut = 180 * Time.MS_PER_SECOND)
  public void setup() {
    Properties parentControllerProperties = new Properties();
    // Disable topic cleanup since parent and child are sharing the same kafka cluster.
    parentControllerProperties.setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    // Required by metadata system store
    parentControllerProperties.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");

    Properties childControllerProperties = new Properties();
    // Required by metadata system store
    childControllerProperties.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");

    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);

    // RF 2 with 2 servers to test both the leader and follower code paths
    twoLayerMultiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
        2,
        2,
        1,
        1,
        2,
        1,
        2,
        Optional.of(new VeniceProperties(parentControllerProperties)),
        Optional.of(childControllerProperties),
        Optional.of(new VeniceProperties(serverProperties)),
        true,
        MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST);

    multiClusterWrappers = twoLayerMultiColoMultiClusterWrapper.getClusters();
    String[] clusterNames = multiClusterWrappers.get(0).getClusterNames();
    Arrays.sort(clusterNames);
    srcClusterName = clusterNames[0];
    destClusterName = clusterNames[1];
    parentControllerUrl = twoLayerMultiColoMultiClusterWrapper.getParentControllers().stream()
        .map(VeniceControllerWrapper::getControllerUrl)
        .collect(Collectors.joining(","));
    childControllerUrl0 = multiClusterWrappers.get(0).getControllerConnectString();

    // Create and populate testStore in src cluster
    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
    srcControllerClient.createNewStore(STORE_NAME, OWNER, "\"string\"", "\"string\"");
    srcControllerClient.updateStore(STORE_NAME, new UpdateStoreQueryParams()
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setHybridRewindSeconds(10L)
        .setHybridOffsetLagThreshold(2L)
        .setLeaderFollowerModel(isLeaderFollowerModel));

    // Populate store
    populateStore(parentControllerUrl, STORE_NAME, 1);

    // Create and configure the Zk shared store for metadata system stores.
    String zkSharedStoreName;
    for (String clusterName : clusterNames) {
      ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerUrl);
      zkSharedStoreName = VeniceSystemStoreUtils.getSharedZkNameForMetadataStore(clusterName);
      Assert.assertFalse(parentControllerClient.createNewZkSharedStoreWithDefaultConfigs(
          zkSharedStoreName, "test").isError(), "Failed to create the Zk shared store");
      Assert.assertFalse(parentControllerClient.newZkSharedStoreVersion(zkSharedStoreName).isError(),
          "Failed to create new Zk shared store version");
      Assert.assertEquals(parentControllerClient.getStore(zkSharedStoreName).getStore().getCurrentVersion(),
          zkSharedStoreVersion);
    }

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
  public void testMigrateStoreWithMetadataSystemStoreMultiDatacenter() throws Exception {
    // materialize metadata store in the source cluster
    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
    ControllerClient destControllerClient = new ControllerClient(destClusterName, parentControllerUrl);
    Assert.assertFalse(srcControllerClient.materializeMetadataStoreVersion(STORE_NAME, zkSharedStoreVersion).isError());
    String srcD2ServiceName = "venice-" + srcClusterName.substring(srcClusterName.length() - 1);
    D2Client d2Client = D2TestUtils.getAndStartD2Client(multiClusterWrappers.get(0).getClusters().get(srcClusterName).getZk().getAddress());
    ClientConfig<StoreMetadataValue> clientConfig = ClientConfig.defaultSpecificClientConfig(
        VeniceSystemStoreUtils.getMetadataStoreName(STORE_NAME), StoreMetadataValue.class)
        .setD2ServiceName(srcD2ServiceName)
        .setD2Client(d2Client)
        .setStoreName(VeniceSystemStoreUtils.getMetadataStoreName(STORE_NAME));
    // Refresh router repositories to ensure client connects to the correct cluster during initialization
    refreshAllRouterMetaData();
    try (AvroSpecificStoreClient<StoreMetadataKey, StoreMetadataValue> client =
        ClientFactory.getAndStartSpecificAvroClient(clientConfig)) {
      StoreMetadataKey storeAttributesKey = MetadataStoreUtils.getStoreAttributesKey(STORE_NAME);
      String metadataStoreTopic =
          Version.composeKafkaTopic(VeniceSystemStoreUtils.getMetadataStoreName(STORE_NAME), zkSharedStoreVersion);
      TestUtils.waitForNonDeterministicPushCompletion(metadataStoreTopic, srcControllerClient, 30, TimeUnit.SECONDS,
          Optional.empty());
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        Assert.assertNotNull(client.get(storeAttributesKey).get());
      });

      startMigration(parentControllerUrl);
      completeMigration();

      try {
        // Verify the metadata store is materialized in the destination cluster and contains correct values.
        TestUtils.waitForNonDeterministicPushCompletion(metadataStoreTopic, destControllerClient, 30, TimeUnit.SECONDS,
            Optional.empty());
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          Assert.assertNotNull(client.get(storeAttributesKey).get());
        });
        StoreAttributes storeAttributes = (StoreAttributes) client.get(storeAttributesKey).get().metadataUnion;
        Assert.assertEquals(storeAttributes.sourceCluster.toString(), destClusterName,
            "Unexpected source cluster post store migration");
      } finally {
        endMigration(parentControllerUrl);
      }
    }
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testMigrateStoreWithMetaSystemStoreMultiDatacenter() throws Exception {
    // materialize metadata store in the source cluster
    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
    // Enable the meta system store
    String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(STORE_NAME);
    VersionCreationResponse versionCreationResponseForMetaSystemStore = srcControllerClient.emptyPush(metaSystemStoreName,
        "test_meta_system_store_push_1", 10000);
    assertFalse(versionCreationResponseForMetaSystemStore.isError(), "New version creation for meta system store: "
        + metaSystemStoreName + " should success, but got error: " + versionCreationResponseForMetaSystemStore.getError());
    TestUtils.waitForNonDeterministicPushCompletion(versionCreationResponseForMetaSystemStore.getKafkaTopic(), srcControllerClient, 30,
        TimeUnit.SECONDS, Optional.empty());

    ControllerClient destControllerClient = new ControllerClient(destClusterName, parentControllerUrl);
    String srcD2ServiceName = "venice-" + srcClusterName.substring(srcClusterName.length() - 1);
    D2Client d2Client = D2TestUtils.getAndStartD2Client(multiClusterWrappers.get(0).getClusters().get(srcClusterName).getZk().getAddress());

    ClientConfig<StoreMetaValue> clientConfig = ClientConfig.defaultSpecificClientConfig(
        metaSystemStoreName, StoreMetaValue.class)
        .setD2ServiceName(srcD2ServiceName)
        .setD2Client(d2Client)
        .setStoreName(metaSystemStoreName);
    // Refresh router repositories to ensure client connects to the correct cluster during initialization
    refreshAllRouterMetaData();
    try (AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> client =
        ClientFactory.getAndStartSpecificAvroClient(clientConfig)) {
      StoreMetaKey storePropertiesKey = MetaStoreDataType.STORE_PROPERTIES.getStoreMetaKey(new HashMap<String, String>() {{
        put(KEY_STRING_STORE_NAME, STORE_NAME);
        put(KEY_STRING_CLUSTER_NAME, srcClusterName);
      }});
      StoreMetaValue storeProperties = client.get(storePropertiesKey).get();
      assertTrue(storeProperties != null && storeProperties.storeProperties != null);

      startMigration(parentControllerUrl);
      completeMigration();

      refreshAllRouterMetaData();
      try {
        // Verify the meta system store is materialized in the destination cluster and contains correct values.
        StoreMetaKey storePropertiesKeyInDestCluster = MetaStoreDataType.STORE_PROPERTIES.getStoreMetaKey(new HashMap<String, String>() {{
          put(KEY_STRING_STORE_NAME, STORE_NAME);
          put(KEY_STRING_CLUSTER_NAME, destClusterName);
        }});
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          try {
            StoreMetaValue storePropertiesInDestCluster = client.get(storePropertiesKeyInDestCluster).get();
            assertTrue(storePropertiesInDestCluster != null && storePropertiesInDestCluster.storeProperties != null);
          } catch (Exception e) {
            fail("Exception is not unexpected: " + e.getMessage());
          }
        });
      } finally {
        endMigration(parentControllerUrl);
      }
    }
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

    SystemProducer veniceProducer0 = null;
    SystemProducer veniceProducer1 = null;

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

      // Write streaming records
      veniceProducer0 = getSamzaProducer(multiClusterWrappers.get(0).getClusters().get(srcClusterName), storeName, Version.PushType.STREAM);
      veniceProducer1 = getSamzaProducer(multiClusterWrappers.get(1).getClusters().get(srcClusterName), storeName, Version.PushType.STREAM);
      for (int i = 1; i <= 10; i++) {
        sendCustomSizeStreamingRecord(veniceProducer0, storeName, i, STREAMING_RECORD_SIZE);
        sendCustomSizeStreamingRecord(veniceProducer1, storeName, i, STREAMING_RECORD_SIZE);
      }
    } catch (Exception e) {
      throw new VeniceException(e);
    } finally {
      if (null != veniceProducer0) {
        veniceProducer0.stop();
      }
      if (null != veniceProducer1) {
        veniceProducer1.stop();
      }
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
      Assert.assertFalse(storeResponse.getStore().isMigrationDuplicateStore());
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
    for (int i = 1; i <= 10; i++) {
      String key = Integer.toString(i);
      String value = client.get(key).get().toString();
      assertEquals(value.length(), STREAMING_RECORD_SIZE);

      String expectedChar = Integer.toString(i).substring(0, 1);
      for (int j = 0; j < value.length(); j++) {
        assertEquals(value.substring(j, j + 1), expectedChar);
      }
    }

    for (int i = 11; i <= 100; ++i) {
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
