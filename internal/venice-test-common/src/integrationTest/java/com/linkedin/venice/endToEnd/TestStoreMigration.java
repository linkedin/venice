package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.OFFLINE_JOB_START_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_CLUSTER_NAME;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.venice.AdminTool;
import com.linkedin.venice.AdminTool.PrintFunction;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.StatTrackingStoreClient;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * The suite includes integration tests for all store migration tools, including migrate-store, complete-migration,
 * end-migration and abort-migration. Test cases cover store migration on parent controller and child controller.
 *
 * Test stores are hybrid stores created in {@link TestStoreMigration#createAndPushStore(String, String)}.
 * Test stores enable L/F in child datacenter dc-0 and disable L/F in parent datacenter.
 */
@Test(singleThreaded = true)
public class TestStoreMigration {
  private static final int TEST_TIMEOUT = 120 * Time.MS_PER_SECOND;
  private static final int RECORD_COUNT = 20;
  private static final String NEW_OWNER = "newtest@linkedin.com";
  private static final String FABRIC0 = "dc-0";
  private static final boolean[] ABORT_MIGRATION_PROMPTS_OVERRIDE = { false, true, true };

  private VeniceTwoLayerMultiRegionMultiClusterWrapper twoLayerMultiRegionMultiClusterWrapper;
  private VeniceMultiClusterWrapper multiClusterWrapper;
  private String srcClusterName;
  private String destClusterName;
  private String parentControllerUrl;
  private String childControllerUrl0;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties parentControllerProperties = new Properties();
    // Disable topic cleanup since parent and child are sharing the same kafka cluster.
    parentControllerProperties
        .setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    parentControllerProperties.setProperty(OFFLINE_JOB_START_TIMEOUT_MS, "180000");

    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);

    // 1 parent controller, 1 child region, 2 clusters per child region, 2 servers per cluster
    // RF=2 to test both leader and follower SNs
    twoLayerMultiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        1,
        2,
        1,
        1,
        2,
        1,
        2,
        Optional.of(parentControllerProperties),
        Optional.empty(),
        Optional.of(serverProperties),
        false);

    multiClusterWrapper = twoLayerMultiRegionMultiClusterWrapper.getChildRegions().get(0);
    String[] clusterNames = multiClusterWrapper.getClusterNames();
    Arrays.sort(clusterNames);
    srcClusterName = clusterNames[0]; // venice-cluster0
    destClusterName = clusterNames[1]; // venice-cluster1
    parentControllerUrl = twoLayerMultiRegionMultiClusterWrapper.getControllerConnectString();
    childControllerUrl0 = multiClusterWrapper.getControllerConnectString();

    for (String cluster: clusterNames) {
      try (ControllerClient controllerClient = new ControllerClient(cluster, childControllerUrl0)) {
        // Verify the participant store is up and running in child region
        String participantStoreName = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(cluster);
        TestUtils.waitForNonDeterministicPushCompletion(
            Version.composeKafkaTopic(participantStoreName, 1),
            controllerClient,
            5,
            TimeUnit.MINUTES);
      }
    }
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(twoLayerMultiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStoreMigration() throws Exception {
    String storeName = Utils.getUniqueString("test");
    createAndPushStore(srcClusterName, storeName);

    String srcD2ServiceName = multiClusterWrapper.getClusterToD2().get(srcClusterName);
    String destD2ServiceName = multiClusterWrapper.getClusterToD2().get(destClusterName);
    D2Client d2Client =
        D2TestUtils.getAndStartD2Client(multiClusterWrapper.getClusters().get(srcClusterName).getZk().getAddress());
    ClientConfig clientConfig =
        ClientConfig.defaultGenericClientConfig(storeName).setD2ServiceName(srcD2ServiceName).setD2Client(d2Client);

    try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(clientConfig)) {
      readFromStore(client);
      startMigration(parentControllerUrl, storeName);
      completeMigration(parentControllerUrl, storeName);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        // StoreConfig in router might not be up-to-date. Keep reading from the store. Finally, router will find that
        // cluster discovery changes and redirect the request to dest store. Client's d2ServiceName will be updated.
        readFromStore(client);
        AbstractAvroStoreClient<String, Object> castClient =
            (AbstractAvroStoreClient<String, Object>) ((StatTrackingStoreClient<String, Object>) client)
                .getInnerStoreClient();
        Assert.assertTrue(castClient.toString().contains(destD2ServiceName));
      });
    }

    // Test abort migration on parent controller
    try (ControllerClient srcParentControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
        ControllerClient destParentControllerClient = new ControllerClient(destClusterName, parentControllerUrl)) {
      abortMigration(parentControllerUrl, storeName, true);
      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          () -> checkStatusAfterAbortMigration(srcParentControllerClient, destParentControllerClient, storeName));
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStoreMigrationWithNewPushesAndUpdates() throws Exception {
    String storeName = Utils.getUniqueString("testWithNewPushesAndUpdates");
    Properties props = createAndPushStore(srcClusterName, storeName);

    try (ControllerClient srcParentControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
        ControllerClient destParentControllerClient = new ControllerClient(destClusterName, parentControllerUrl)) {
      startMigration(parentControllerUrl, storeName);
      // Ensure migration status is updated in source parent controller
      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          () -> Assert.assertTrue(srcParentControllerClient.getStore(storeName).getStore().isMigrating()));

      // Push v2
      TestWriteUtils.runPushJob("Test push job 2", props);
      // Update store
      srcParentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setOwner(NEW_OWNER));

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo srcStore = srcParentControllerClient.getStore(storeName).getStore();
        StoreInfo destStore = destParentControllerClient.getStore(storeName).getStore();
        Assert.assertNotNull(srcStore);
        Assert.assertNotNull(destStore);

        // Largest used version number in src and dest cluster stores should be 2
        Assert.assertEquals(srcStore.getLargestUsedVersionNumber(), 2);
        Assert.assertEquals(destStore.getLargestUsedVersionNumber(), 2);

        // Owner of src and dest cluster stores should be updated
        Assert.assertEquals(srcStore.getOwner(), NEW_OWNER);
        Assert.assertEquals(destStore.getOwner(), NEW_OWNER);

        // Test replication metadata version id is updated
        Assert.assertEquals(srcStore.getVersions().get(1).getRmdVersionId(), 1);
        Assert.assertEquals(destStore.getVersions().get(1).getRmdVersionId(), 1);
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStoreMigrationWithMetaSystemStore() throws Exception {
    String storeName = Utils.getUniqueString("testWithMetaSystemStore");
    createAndPushStore(srcClusterName, storeName);

    // Meta system store is enabled by default. Check if it has online version.
    try (ControllerClient srcChildControllerClient = new ControllerClient(srcClusterName, childControllerUrl0)) {
      String systemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = srcChildControllerClient.getStore(systemStoreName);
        Assert.assertFalse(storeResponse.isError());
        Assert.assertTrue(storeResponse.getStore().getCurrentVersion() > 0, systemStoreName + " is not ready");
      });
    }

    String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    String srcD2ServiceName = multiClusterWrapper.getClusterToD2().get(srcClusterName);
    D2Client d2Client =
        D2TestUtils.getAndStartD2Client(multiClusterWrapper.getClusters().get(srcClusterName).getZk().getAddress());
    ClientConfig<StoreMetaValue> clientConfig =
        ClientConfig.defaultSpecificClientConfig(metaSystemStoreName, StoreMetaValue.class)
            .setD2ServiceName(srcD2ServiceName)
            .setD2Client(d2Client)
            .setStoreName(metaSystemStoreName);

    try (AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> client =
        ClientFactory.getAndStartSpecificAvroClient(clientConfig)) {
      StoreMetaKey storePropertiesKey =
          MetaStoreDataType.STORE_PROPERTIES.getStoreMetaKey(new HashMap<String, String>() {
            {
              put(KEY_STRING_STORE_NAME, storeName);
              put(KEY_STRING_CLUSTER_NAME, srcClusterName);
            }
          });

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreMetaValue storeProperties = client.get(storePropertiesKey).get();
        Assert.assertTrue(storeProperties != null && storeProperties.storeProperties != null);
      });

      startMigration(parentControllerUrl, storeName);
      completeMigration(parentControllerUrl, storeName);

      // Verify the meta system store is materialized in the destination cluster and contains correct values.
      StoreMetaKey storePropertiesKeyInDestCluster =
          MetaStoreDataType.STORE_PROPERTIES.getStoreMetaKey(new HashMap<String, String>() {
            {
              put(KEY_STRING_STORE_NAME, storeName);
              put(KEY_STRING_CLUSTER_NAME, destClusterName);
            }
          });
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        try {
          StoreMetaValue storePropertiesInDestCluster = client.get(storePropertiesKeyInDestCluster).get();
          Assert
              .assertTrue(storePropertiesInDestCluster != null && storePropertiesInDestCluster.storeProperties != null);
        } catch (Exception e) {
          Assert.fail("Exception is not unexpected: " + e.getMessage());
        }
      });
      // Test end migration
      endMigration(parentControllerUrl, storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStoreMigrationWithDaVinciPushStatusSystemStore() throws Exception {
    String storeName = Utils.getUniqueString("testWithDaVinciPushStatusSystemStore");
    createAndPushStore(srcClusterName, storeName);

    // DaVinci push status system store is enabled by default. Check if it has online version.
    String systemStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName);
    try (ControllerClient srcChildControllerClient = new ControllerClient(srcClusterName, childControllerUrl0)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = srcChildControllerClient.getStore(systemStoreName);
        Assert.assertFalse(storeResponse.isError());
        Assert.assertTrue(storeResponse.getStore().getCurrentVersion() > 0, systemStoreName + " is not ready");
      });
    }

    VeniceProperties backendConfig =
        DaVinciTestContext.getDaVinciPropertyBuilder(multiClusterWrapper.getZkServerWrapper().getAddress())
            .put(PUSH_STATUS_STORE_ENABLED, true)
            .put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 5)
            .build();
    D2Client d2Client =
        D2TestUtils.getAndStartD2Client(multiClusterWrapper.getClusters().get(srcClusterName).getZk().getAddress());
    PushStatusStoreReader pushStatusStoreReader = new PushStatusStoreReader(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        TimeUnit.MINUTES.toSeconds(10));
    try (DaVinciClient daVinciClient = ServiceFactory.getGenericAvroDaVinciClient(
        storeName,
        multiClusterWrapper.getClusters().get(srcClusterName),
        new DaVinciConfig(),
        backendConfig)) {
      daVinciClient.subscribeAll().get();
      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          () -> Assert
              .assertEquals(pushStatusStoreReader.getPartitionStatus(storeName, 1, 0, Optional.empty()).size(), 1));

      startMigration(parentControllerUrl, storeName);

      // Store migration status output via closure PrintFunction
      Set<String> statusOutput = new HashSet<String>();
      PrintFunction printFunction = (message) -> {
        statusOutput.add(message.trim());
        System.err.println(message);
      };

      checkMigrationStatus(parentControllerUrl, storeName, printFunction);

      // Check that store and system store exists in both source and destination cluster
      Assert.assertTrue(
          statusOutput.contains(
              String.format(
                  "%s belongs to cluster venice-cluster0 according to cluster discovery",
                  storeName,
                  srcClusterName)));
      Assert
          .assertTrue(statusOutput.contains(String.format("%s exists in this cluster %s", storeName, destClusterName)));
      Assert.assertTrue(
          statusOutput.contains(String.format("%s exists in this cluster %s", systemStoreName, srcClusterName)));
      Assert.assertTrue(
          statusOutput.contains(String.format("%s exists in this cluster %s", systemStoreName, destClusterName)));

      completeMigration(parentControllerUrl, storeName);

      // Verify the da vinci push status system store is materialized in dest cluster and contains the same value
      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          () -> Assert
              .assertEquals(pushStatusStoreReader.getPartitionStatus(storeName, 1, 0, Optional.empty()).size(), 1));

      // Verify that store and system store only exist in destination cluster after ending migration
      statusOutput.clear();
      endMigration(parentControllerUrl, storeName);
      checkMigrationStatus(parentControllerUrl, storeName, printFunction);

      Assert
          .assertFalse(statusOutput.contains(String.format("%s exists in this cluster %s", storeName, srcClusterName)));
      Assert
          .assertTrue(statusOutput.contains(String.format("%s exists in this cluster %s", storeName, destClusterName)));
      Assert.assertFalse(
          statusOutput.contains(String.format("%s exists in this cluster %s", systemStoreName, srcClusterName)));
      Assert.assertTrue(
          statusOutput.contains(String.format("%s exists in this cluster %s", systemStoreName, destClusterName)));
    } finally {
      Utils.closeQuietlyWithErrorLogged(pushStatusStoreReader);
      D2ClientUtils.shutdownClient(d2Client);
    }

    // Verify that meta and da vinci push status system store flags in parent region are set to true
    try (ControllerClient destParentControllerClient = new ControllerClient(destClusterName, parentControllerUrl)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = destParentControllerClient.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        Assert.assertTrue(storeResponse.getStore().isStoreMetaSystemStoreEnabled());
        Assert.assertTrue(storeResponse.getStore().isDaVinciPushStatusStoreEnabled());
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testVersionConfigsRemainSameAfterStoreMigration() throws Exception {
    String storeName = Utils.getUniqueString("test");
    createAndPushStore(srcClusterName, storeName);

    // Get the source current version config
    StoreInfo sourceStoreInfo = getStoreConfig(childControllerUrl0, srcClusterName, storeName);
    Optional<Version> sourceCurrentVersion = sourceStoreInfo.getVersion(sourceStoreInfo.getCurrentVersion());
    Assert.assertTrue(sourceCurrentVersion.isPresent());
    final Version expectedVersionConfig = sourceCurrentVersion.get();

    // Update some version level configs like compression strategy, chunking, etc.
    final boolean expectedChunkingEnabledValue = sourceStoreInfo.isChunkingEnabled();
    // Will update the store config in the source cluster to the unexpected value; after store migration, the current
    // version
    // config in the destination cluster should match the expected value instead of the unexpected value.
    final boolean unexpectedChunkingEnabledValue = !expectedChunkingEnabledValue;

    final CompressionStrategy expectedCompressionStrategy = sourceStoreInfo.getCompressionStrategy();
    final CompressionStrategy unexpectedCompressionStrategy = expectedCompressionStrategy == CompressionStrategy.NO_OP
        ? CompressionStrategy.ZSTD_WITH_DICT
        : CompressionStrategy.NO_OP;

    // Update the store config in the source cluster to the unexpected values
    try (ControllerClient srcParentControllerClient = new ControllerClient(srcClusterName, parentControllerUrl)) {
      UpdateStoreQueryParams updateStoreQueryParams =
          new UpdateStoreQueryParams().setChunkingEnabled(unexpectedChunkingEnabledValue)
              .setCompressionStrategy(unexpectedCompressionStrategy);
      ControllerResponse updateStoreResponse = srcParentControllerClient.updateStore(storeName, updateStoreQueryParams);
      Assert.assertFalse(updateStoreResponse.isError());
    }

    String srcD2ServiceName = multiClusterWrapper.getClusterToD2().get(srcClusterName);
    String destD2ServiceName = multiClusterWrapper.getClusterToD2().get(destClusterName);
    D2Client d2Client =
        D2TestUtils.getAndStartD2Client(multiClusterWrapper.getClusters().get(srcClusterName).getZk().getAddress());
    ClientConfig clientConfig =
        ClientConfig.defaultGenericClientConfig(storeName).setD2ServiceName(srcD2ServiceName).setD2Client(d2Client);

    try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(clientConfig)) {
      readFromStore(client);
      startMigration(parentControllerUrl, storeName);
      completeMigration(parentControllerUrl, storeName);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        // StoreConfig in router might not be up-to-date. Keep reading from the store. Finally, router will find that
        // cluster discovery changes and redirect the request to dest store. Client's d2ServiceName will be updated.
        readFromStore(client);
        AbstractAvroStoreClient<String, Object> castClient =
            (AbstractAvroStoreClient<String, Object>) ((StatTrackingStoreClient<String, Object>) client)
                .getInnerStoreClient();
        Assert.assertTrue(castClient.toString().contains(destD2ServiceName));
      });
    }

    // Test current version config in the destination cluster is the same as the version config from the source cluster,
    // and they don't match any of the unexpected values.
    StoreInfo destStoreInfo = getStoreConfig(childControllerUrl0, destClusterName, storeName);
    Optional<Version> destCurrentVersion = destStoreInfo.getVersion(destStoreInfo.getCurrentVersion());
    Assert.assertTrue(destCurrentVersion.isPresent());
    final Version destVersionConfig = destCurrentVersion.get();
    Assert.assertEquals(destVersionConfig, expectedVersionConfig);
    // Check chunking config at version level shouldn't change
    Assert.assertEquals(destVersionConfig.isChunkingEnabled(), expectedChunkingEnabledValue);
    Assert.assertNotEquals(destVersionConfig.isChunkingEnabled(), unexpectedChunkingEnabledValue);
    // Check compression strategy at version level shouldn't change
    Assert.assertEquals(destVersionConfig.getCompressionStrategy(), expectedCompressionStrategy);
    Assert.assertNotEquals(destVersionConfig.getCompressionStrategy(), unexpectedCompressionStrategy);
  }

  private Properties createAndPushStore(String clusterName, String storeName) throws Exception {
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(twoLayerMultiRegionMultiClusterWrapper, inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, RECORD_COUNT);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    UpdateStoreQueryParams updateStoreQueryParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setHybridRewindSeconds(TEST_TIMEOUT)
            .setHybridOffsetLagThreshold(2L);
    IntegrationTestPushUtils.createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreQueryParams)
        .close();

    // Verify store is created in dc-0
    try (ControllerClient childControllerClient0 = new ControllerClient(clusterName, childControllerUrl0)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse response = childControllerClient0.getStore(storeName);
        StoreInfo storeInfo = response.getStore();
        Assert.assertNotNull(storeInfo);
      });
    }

    SystemProducer veniceProducer0 = null;
    try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
      job.run();

      // Write streaming records
      veniceProducer0 =
          getSamzaProducer(multiClusterWrapper.getClusters().get(clusterName), storeName, Version.PushType.STREAM);
      for (int i = 1; i <= 10; i++) {
        sendStreamingRecord(veniceProducer0, storeName, i);
      }
    } catch (Exception e) {
      throw new VeniceException(e);
    } finally {
      if (veniceProducer0 != null) {
        veniceProducer0.stop();
      }
    }

    return props;
  }

  private void startMigration(String controllerUrl, String storeName) throws Exception {
    String[] startMigrationArgs = { "--migrate-store", "--url", controllerUrl, "--store", storeName, "--cluster-src",
        srcClusterName, "--cluster-dest", destClusterName };
    AdminTool.main(startMigrationArgs);
  }

  private void checkMigrationStatus(String controllerUrl, String storeName, PrintFunction printFunction)
      throws Exception {
    String[] checkMigrationStatusArgs = { "--migration-status", "--url", controllerUrl, "--store", storeName,
        "--cluster-src", srcClusterName, "--cluster-dest", destClusterName };
    AdminTool.checkMigrationStatus(AdminTool.getCommandLine(checkMigrationStatusArgs), printFunction);
  }

  private void completeMigration(String controllerUrl, String storeName) {
    String[] completeMigration0 = { "--complete-migration", "--url", controllerUrl, "--store", storeName,
        "--cluster-src", srcClusterName, "--cluster-dest", destClusterName, "--fabric", FABRIC0 };

    try (ControllerClient destParentControllerClient = new ControllerClient(destClusterName, controllerUrl)) {
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        AdminTool.main(completeMigration0);
        // Store discovery should point to the new cluster after completing migration
        ControllerResponse discoveryResponse = destParentControllerClient.discoverCluster(storeName);
        Assert.assertEquals(discoveryResponse.getCluster(), destClusterName);
      });
    }
  }

  private void endMigration(String controllerUrl, String storeName) throws Exception {
    String[] endMigration = { "--end-migration", "--url", controllerUrl, "--store", storeName, "--cluster-src",
        srcClusterName, "--cluster-dest", destClusterName };
    AdminTool.main(endMigration);

    try (ControllerClient srcControllerClient = new ControllerClient(srcClusterName, controllerUrl);
        ControllerClient destControllerClient = new ControllerClient(destClusterName, controllerUrl)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        // Store should be deleted in source cluster. Store in destination cluster should not be migrating.
        StoreResponse storeResponse = srcControllerClient.getStore(storeName);
        Assert.assertNull(storeResponse.getStore());

        storeResponse = destControllerClient.getStore(storeName);
        Assert.assertNotNull(storeResponse.getStore());
        Assert.assertFalse(storeResponse.getStore().isMigrating());
        Assert.assertFalse(storeResponse.getStore().isMigrationDuplicateStore());
      });
    }
  }

  private void readFromStore(AvroGenericStoreClient<String, Object> client) {
    int key = ThreadLocalRandom.current().nextInt(RECORD_COUNT) + 1;
    client.get(Integer.toString(key));
  }

  private void abortMigration(String controllerUrl, String storeName, boolean force) {
    AdminTool.abortMigration(
        controllerUrl,
        storeName,
        srcClusterName,
        destClusterName,
        force,
        ABORT_MIGRATION_PROMPTS_OVERRIDE);
  }

  private void checkStatusAfterAbortMigration(
      ControllerClient srcControllerClient,
      ControllerClient destControllerClient,
      String storeName) {
    // Migration flag should be false
    // Store should be deleted in dest cluster
    // Cluster discovery should point to src cluster
    StoreResponse storeResponse = srcControllerClient.getStore(storeName);
    Assert.assertNotNull(storeResponse.getStore());
    Assert.assertFalse(storeResponse.getStore().isMigrating());
    storeResponse = destControllerClient.getStore(storeName);
    Assert.assertNull(storeResponse.getStore());
    ControllerResponse discoveryResponse = destControllerClient.discoverCluster(storeName);
    Assert.assertEquals(discoveryResponse.getCluster(), srcClusterName);
  }

  private StoreInfo getStoreConfig(String controllerUrl, String clusterName, String storeName) {
    try (ControllerClient controllerClient = new ControllerClient(clusterName, controllerUrl)) {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      if (storeResponse.isError()) {
        throw new VeniceException(
            "Failed to get store configs for store " + storeName + " from cluster " + clusterName + ". Error: "
                + storeResponse.getError());
      }
      return storeResponse.getStore();
    }
  }
}
