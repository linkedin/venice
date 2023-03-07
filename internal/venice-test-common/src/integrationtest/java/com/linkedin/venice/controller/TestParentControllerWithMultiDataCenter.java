package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.DEFAULT_NUMBER_OF_PARTITION_FOR_HYBRID;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestParentControllerWithMultiDataCenter {
  private static final int TEST_TIMEOUT = 90_000; // ms
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0",
                                                                                                         // "venice-cluster1",
                                                                                                         // ...];

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;

  private static final String BASIC_USER_SCHEMA_STRING_WITH_DEFAULT = "{" + "  \"namespace\" : \"example.avro\",  "
      + "  \"type\": \"record\",   " + "  \"name\": \"User\",     " + "  \"fields\": [           "
      + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"\"}  " + "  ] " + " } ";

  @BeforeClass
  public void setUp() {
    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_NUMBER_OF_PARTITION_FOR_HYBRID, 2);
    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        1,
        1,
        1,
        Optional.of(new VeniceProperties(controllerProps)),
        Optional.of(controllerProps),
        Optional.empty());

    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testUpdateStore() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("store");

    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    try (ControllerClient parentControllerClient =
        ControllerClient.constructClusterControllerClient(clusterName, parentControllerURLs)) {
      /**
       * Create a test store
       */
      NewStoreResponse newStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
      Assert.assertFalse(
          newStoreResponse.isError(),
          "The NewStoreResponse returned an error: " + newStoreResponse.getError());

      /**
       * Send UpdateStore to parent controller to update a store config
       */
      final long expectedHybridRewindSeconds = 100;
      final long expectedHybridOffsetLagThreshold = 100;
      final BufferReplayPolicy expectedHybridBufferReplayPolicy = BufferReplayPolicy.REWIND_FROM_SOP;
      final UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setHybridRewindSeconds(expectedHybridRewindSeconds)
              .setHybridOffsetLagThreshold(expectedHybridOffsetLagThreshold)
              .setHybridBufferReplayPolicy(expectedHybridBufferReplayPolicy)
              .setChunkingEnabled(true)
              .setRmdChunkingEnabled(true)
              .setAmplificationFactor(2);

      TestWriteUtils.updateStore(storeName, parentControllerClient, updateStoreParams);

      ControllerClient[] controllerClients = new ControllerClient[childDatacenters.size() + 1];
      controllerClients[0] = parentControllerClient;
      for (int i = 0; i < childDatacenters.size(); i++) {
        controllerClients[i + 1] =
            new ControllerClient(clusterName, childDatacenters.get(i).getControllerConnectString());
      }

      /**
       * Verify parent controller and all child controllers have updated the config
       */
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        for (ControllerClient controllerClient: controllerClients) {
          StoreResponse storeResponse = controllerClient.getStore(storeName);
          Assert.assertFalse(storeResponse.isError());
          StoreInfo storeInfo = storeResponse.getStore();

          Assert.assertNotNull(storeInfo.getHybridStoreConfig());
          Assert.assertEquals(
              storeInfo.getHybridStoreConfig().getOffsetLagThresholdToGoOnline(),
              expectedHybridOffsetLagThreshold);
          Assert.assertEquals(storeInfo.getHybridStoreConfig().getRewindTimeInSeconds(), expectedHybridRewindSeconds);
          Assert
              .assertEquals(storeInfo.getHybridStoreConfig().getBufferReplayPolicy(), expectedHybridBufferReplayPolicy);
          Assert.assertNotNull(storeInfo.getPartitionerConfig());
          Assert.assertEquals(storeInfo.getPartitionerConfig().getAmplificationFactor(), 2);
          Assert.assertTrue(storeInfo.isChunkingEnabled());
          Assert.assertTrue(storeInfo.isRmdChunkingEnabled());
          Assert.assertEquals(storeInfo.getPartitionCount(), 2);
        }
      });

      // Turn off hybrid config so we can update the partitioner config.
      final UpdateStoreQueryParams updateStoreParams2 =
          new UpdateStoreQueryParams().setHybridRewindSeconds(-1).setHybridOffsetLagThreshold(-1);
      TestWriteUtils.updateStore(storeName, parentControllerClient, updateStoreParams2);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        for (ControllerClient controllerClient: controllerClients) {
          StoreResponse storeResponse = controllerClient.getStore(storeName);
          Assert.assertFalse(storeResponse.isError());
          StoreInfo storeInfo = storeResponse.getStore();
          Assert.assertNull(storeInfo.getHybridStoreConfig());
        }
      });

      // Update partitioner parameters make sure new update is in and other fields of partitioner config is not reset.
      final UpdateStoreQueryParams updateStoreParams3 =
          new UpdateStoreQueryParams().setPartitionerParams(Collections.singletonMap("key", "val"));
      TestWriteUtils.updateStore(storeName, parentControllerClient, updateStoreParams3);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        for (ControllerClient controllerClient: controllerClients) {
          StoreResponse storeResponse = controllerClient.getStore(storeName);
          Assert.assertFalse(storeResponse.isError());
          StoreInfo storeInfo = storeResponse.getStore();
          Assert.assertNotNull(storeInfo.getPartitionerConfig());
          Assert.assertEquals(storeInfo.getPartitionerConfig().getAmplificationFactor(), 2);
          Assert.assertEquals(
              storeInfo.getPartitionerConfig().getPartitionerParams(),
              Collections.singletonMap("key", "val"));
        }
      });

      // New incremental push store. Verify that store is converted to hybrid and partition count is enforced.
      String incPushStoreName = Utils.getUniqueString("incPushStore");
      newStoreResponse = parentControllerClient
          .retryableRequest(5, c -> c.createNewStore(incPushStoreName, "", "\"string\"", "\"string\""));
      Assert.assertFalse(
          newStoreResponse.isError(),
          "The NewStoreResponse returned an error: " + newStoreResponse.getError());
      TestWriteUtils.updateStore(
          incPushStoreName,
          parentControllerClient,
          new UpdateStoreQueryParams().setIncrementalPushEnabled(true));
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        for (ControllerClient controllerClient: controllerClients) {
          StoreResponse storeResponse = controllerClient.getStore(incPushStoreName);
          Assert.assertFalse(storeResponse.isError());
          StoreInfo storeInfo = storeResponse.getStore();
          Assert.assertNotNull(storeInfo.getHybridStoreConfig());
          Assert.assertEquals(storeInfo.getPartitionCount(), 2);
        }
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testUpdateStoreOnParentDoNotChangeIrrelevantConfig() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("store");

    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    try (ControllerClient parentControllerClient =
        ControllerClient.constructClusterControllerClient(clusterName, parentControllerURLs)) {
      /**
       * Create a test store
       */
      NewStoreResponse newStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
      Assert.assertFalse(
          newStoreResponse.isError(),
          "The NewStoreResponse returned an error: " + newStoreResponse.getError());

      /**
       * Send UpdateStore to child controller in the first data center; update 3 configs:
       * 1. Storage quota set to 9527;
       * 2. NR set to true.
       */
      long expectedStorageQuotaInDC0 = 9527;
      boolean expectedNativeReplicationConfigInDC0 = true;
      ControllerClient dc0Client =
          new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(expectedStorageQuotaInDC0)
              .setNativeReplicationEnabled(expectedNativeReplicationConfigInDC0);
      TestWriteUtils.updateStore(storeName, dc0Client, updateStoreParams);

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = dc0Client.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        StoreInfo storeInfo = storeResponse.getStore();
        Assert.assertEquals(storeInfo.getStorageQuotaInByte(), expectedStorageQuotaInDC0);
        Assert.assertEquals(storeInfo.isNativeReplicationEnabled(), expectedNativeReplicationConfigInDC0);
      });

      /**
       * Send UpdateStore to parent controller to update a store config that is irrelevant to the above 3 configs
       */
      long expectedReadQuota = 2021;
      UpdateStoreQueryParams updateStoreParamsOnParent =
          new UpdateStoreQueryParams().setReadQuotaInCU(expectedReadQuota);
      TestWriteUtils.updateStore(storeName, parentControllerClient, updateStoreParamsOnParent);

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = dc0Client.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        StoreInfo storeInfo = storeResponse.getStore();
        /**
         * First, wait for the above UpdateStore to be propagated from parent to child
         */
        Assert.assertEquals(storeInfo.getReadQuotaInCU(), expectedReadQuota);

        /**
         * By default, changing read quota in parent should not propagate the other store configs from parent to child;
         * so the below 3 configs in DC0 should remain unchanged.
         */
        Assert.assertEquals(storeInfo.getStorageQuotaInByte(), expectedStorageQuotaInDC0);
        Assert.assertEquals(storeInfo.isNativeReplicationEnabled(), expectedNativeReplicationConfigInDC0);
      });

      /**
       * Now, let's try the "replicate-all-configs" flag which will force the parent controller to propagate all store
       * configs to child
       */
      StoreResponse parentStoreResponse = parentControllerClient.retryableRequest(5, c -> c.getStore(storeName));
      Assert.assertFalse(parentStoreResponse.isError());

      /**
       * Get the default value for 1. storage quota 2. L/F model config 3. NR config in parent
       */
      StoreInfo parentStoreInfo = parentStoreResponse.getStore();
      long storageQuotaInParent = parentStoreInfo.getStorageQuotaInByte();
      boolean nativeReplicationInParent = parentStoreInfo.isNativeReplicationEnabled();

      /**
       * Send an UpdateStore command to parent with "replicate-all-configs" flag turned on.
       */
      long newReadQuotaInParent = 116;
      UpdateStoreQueryParams forceUpdateStoreParamsOnParent =
          new UpdateStoreQueryParams().setReadQuotaInCU(newReadQuotaInParent).setReplicateAllConfigs(true);
      TestWriteUtils.updateStore(storeName, parentControllerClient, forceUpdateStoreParamsOnParent);

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = dc0Client.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        StoreInfo storeInfo = storeResponse.getStore();
        /**
         * First, wait for the above UpdateStore to be propagated from parent to child
         */
        Assert.assertEquals(storeInfo.getReadQuotaInCU(), newReadQuotaInParent);

        /**
         * Store configs in child datacenter should be identical to the store configs in parent
         */
        Assert.assertEquals(storeInfo.getStorageQuotaInByte(), storageQuotaInParent);
        Assert.assertEquals(storeInfo.isNativeReplicationEnabled(), nativeReplicationInParent);
      });

      /**
       * Last check; UpdateStore command without setting value to any configs or turning on the "replicate-all-configs"
       * flag should fail.
       */
      UpdateStoreQueryParams failUpdateStoreParamsOnParent = new UpdateStoreQueryParams();
      ControllerResponse failedUpdateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, failUpdateStoreParamsOnParent));
      Assert.assertTrue(failedUpdateStoreResponse.isError());
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testEnableActiveActiveReplicationSchema() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("store");
    String valueRecordSchemaStr1 = BASIC_USER_SCHEMA_STRING_WITH_DEFAULT;
    String valueRecordSchemaStr2 = TestWriteUtils.USER_SCHEMA_STRING_SIMPLE_WITH_DEFAULT;
    String valueRecordSchemaStr3 = TestWriteUtils.USER_SCHEMA_STRING_WITH_DEFAULT;

    Schema rmdSchema1 = RmdSchemaGenerator.generateMetadataSchema(valueRecordSchemaStr1, 1);
    Schema rmdSchema2 = RmdSchemaGenerator.generateMetadataSchema(valueRecordSchemaStr2, 1);
    Schema rmdSchema3 = RmdSchemaGenerator.generateMetadataSchema(valueRecordSchemaStr3, 1);

    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs)) {
      /**
       * Create a test store
       */
      NewStoreResponse newStoreResponse = parentControllerClient
          .retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", valueRecordSchemaStr1));
      Assert.assertFalse(
          newStoreResponse.isError(),
          "The NewStoreResponse returned an error: " + newStoreResponse.getError());

      SchemaResponse schemaResponse2 =
          parentControllerClient.retryableRequest(5, c -> c.addValueSchema(storeName, valueRecordSchemaStr2));
      Assert.assertFalse(schemaResponse2.isError(), "addValeSchema returned error: " + schemaResponse2.getError());

      // Enable AA on store
      UpdateStoreQueryParams updateStoreToEnableAARepl =
          new UpdateStoreQueryParams().setNativeReplicationEnabled(true).setActiveActiveReplicationEnabled(true);
      TestWriteUtils.updateStore(storeName, parentControllerClient, updateStoreToEnableAARepl);
      /**
       * Test Active/Active replication config enablement generates the active active metadata schema.
       */
      try (ControllerClient dc0Client =
          new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString())) {
        TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, false, true, () -> {
          StoreResponse storeResponse = dc0Client.getStore(storeName);
          Assert.assertFalse(storeResponse.isError());
          StoreInfo storeInfo = storeResponse.getStore();
          Assert.assertTrue(storeInfo.isActiveActiveReplicationEnabled());
        });

        Admin veniceHelixAdmin = childDatacenters.get(0).getControllers().values().iterator().next().getVeniceAdmin();
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
          Collection<RmdSchemaEntry> replicationMetadataSchemas =
              veniceHelixAdmin.getReplicationMetadataSchemas(clusterName, storeName);
          // Expect two RMD schemas because there were 2 value schemas when AA was enabled on this store.
          Assert.assertEquals(replicationMetadataSchemas.size(), 2);
          Iterator<RmdSchemaEntry> iterator = replicationMetadataSchemas.iterator();
          Assert.assertEquals(iterator.next().getSchema(), rmdSchema1);
          Assert.assertEquals(iterator.next().getSchema(), rmdSchema2);
        });

        // Add a new value schema for the store and make sure the corresponding new metadata schema is generated.
        SchemaResponse schemaResponse3 =
            parentControllerClient.retryableRequest(5, c -> c.addValueSchema(storeName, valueRecordSchemaStr3));
        Assert.assertFalse(schemaResponse3.isError(), "addValeSchema returned error: " + schemaResponse3.getError());

        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          // N.B.: The value schema and RMD schema are added by the parent, so we cannot expect that the child will
          // find out about it immediately, hence the retries.
          Collection<RmdSchemaEntry> replicationMetadataSchemas =
              veniceHelixAdmin.getReplicationMetadataSchemas(clusterName, storeName);
          Assert.assertEquals(replicationMetadataSchemas.size(), 3);
          Iterator<RmdSchemaEntry> iterator = replicationMetadataSchemas.iterator();
          Assert.assertEquals(iterator.next().getSchema(), rmdSchema1);
          Assert.assertEquals(iterator.next().getSchema(), rmdSchema2);
          Assert.assertEquals(iterator.next().getSchema(), rmdSchema3);
        });

        // Add a new version for the store and make sure all new metadata schema are generated.
        VersionCreationResponse vcr =
            parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-push"), 1L);
        assertEquals(vcr.getVersion(), 1, "requesting a topic for a push should provide version number 1");

        TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, false, true, () -> {
          StoreResponse storeResponse = dc0Client.getStore(storeName);
          Assert.assertFalse(storeResponse.isError());
          StoreInfo storeInfo = storeResponse.getStore();

          List<Version> versions = storeInfo.getVersions();
          Assert.assertNotNull(versions);
          Assert.assertEquals(versions.size(), 1);
          Assert.assertTrue(versions.get(0).isActiveActiveReplicationEnabled());
          Assert.assertEquals(versions.get(0).getRmdVersionId(), 1);
        });

        Collection<RmdSchemaEntry> replicationMetadataSchemas =
            veniceHelixAdmin.getReplicationMetadataSchemas(clusterName, storeName);
        Assert.assertEquals(replicationMetadataSchemas.size(), 3);

        Iterator<RmdSchemaEntry> iterator = replicationMetadataSchemas.iterator();
        Assert.assertEquals(iterator.next().getSchema(), rmdSchema1);
        Assert.assertEquals(iterator.next().getSchema(), rmdSchema2);
        Assert.assertEquals(iterator.next().getSchema(), rmdSchema3);
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStoreRollbackToBackupVersion() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("testStoreRollbackToBackupVersion");
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
        ControllerClient dc0Client =
            new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client =
            new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString())) {

      NewStoreResponse newStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
      Assert.assertFalse(
          newStoreResponse.isError(),
          "The NewStoreResponse returned an error: " + newStoreResponse.getError());

      List<ControllerClient> childControllerClients = new ArrayList<>();
      childControllerClients.add(dc0Client);
      childControllerClients.add(dc1Client);
      emptyPushToStore(parentControllerClient, childControllerClients, storeName, 1);
      // Rollback should fail since there is no backup version
      ControllerResponse response = parentControllerClient.rollbackToBackupVersion(storeName);
      Assert.assertTrue(response.isError());

      emptyPushToStore(parentControllerClient, childControllerClients, storeName, 2);
      // Should roll back to version 1
      response = parentControllerClient.rollbackToBackupVersion(storeName);
      Assert.assertFalse(response.isError());
      for (ControllerClient childControllerClient: childControllerClients) {
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
          StoreResponse storeResponse = childControllerClient.getStore(storeName);
          Assert.assertFalse(storeResponse.isError());
          StoreInfo storeInfo = storeResponse.getStore();
          Assert.assertEquals(storeInfo.getCurrentVersion(), 1);
        });
      }
    }
  }

  private void emptyPushToStore(
      ControllerClient parentControllerClient,
      List<ControllerClient> childControllerClients,
      String storeName,
      int expectedVersion) {
    VersionCreationResponse vcr = parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-push"), 1L);
    Assert.assertFalse(vcr.isError());
    assertEquals(
        vcr.getVersion(),
        expectedVersion,
        "requesting a topic for a push should provide version number " + expectedVersion);
    for (ControllerClient childControllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = childControllerClient.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        StoreInfo storeInfo = storeResponse.getStore();
        Assert.assertEquals(storeInfo.getCurrentVersion(), expectedVersion);
      });
    }
  }
}
