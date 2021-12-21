package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaGenerator;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaEntry;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TestParentControllerWithMultiDataCenter {
  private static final int TEST_TIMEOUT = 90_000; // ms

  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0", "venice-cluster1", ...];

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;

  private static final String BASIC_USER_SCHEMA_STRING_WITH_DEFAULT = "{" +
      "  \"namespace\" : \"example.avro\",  " +
      "  \"type\": \"record\",   " +
      "  \"name\": \"User\",     " +
      "  \"fields\": [           " +
      "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"\"}  " +
      "  ] " +
      " } ";

  @BeforeClass
  public void setUp() {
    multiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        1,
        1);

    childDatacenters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiColoMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testUpdateStore() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("store");

    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    ControllerClient parentControllerClient = ControllerClient.constructClusterControllerClient(clusterName, parentController.getControllerUrl());

    /**
     * Create a test store
     */
    NewStoreResponse newStoreResponse = parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName,
        "", "\"string\"", "\"string\""));
    Assert.assertFalse(newStoreResponse.isError(), "The NewStoreResponse returned an error: " + newStoreResponse.getError());

    /**
     * Send UpdateStore to parent controller to update a store config
     */
    final long expectedHybridRewindSeconds = 100;
    final long expectedHybridOffsetLagThreshold = 100;
    final BufferReplayPolicy expectedHybridBufferReplayPolicy = BufferReplayPolicy.REWIND_FROM_SOP;
    final UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams()
        .setHybridRewindSeconds(expectedHybridRewindSeconds)
        .setHybridOffsetLagThreshold(expectedHybridOffsetLagThreshold)
        .setHybridBufferReplayPolicy(expectedHybridBufferReplayPolicy)
        .setLeaderFollowerModel(true) // Enable L/F to update amplification factor.
        .setAmplificationFactor(2);

    TestPushUtils.updateStore(clusterName, storeName, parentControllerClient, updateStoreParams);

    ControllerClient[] controllerClients = new ControllerClient[childDatacenters.size() + 1];
    controllerClients[0] = parentControllerClient;
    for (int i = 0; i < childDatacenters.size(); i++) {
      controllerClients[i + 1] = new ControllerClient(clusterName, childDatacenters.get(i).getControllerConnectString());
    }

    /**
     * Verify parent controller and all child controllers have updated the config
     */
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
      for (ControllerClient controllerClient : controllerClients) {
        StoreResponse storeResponse = controllerClient.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        StoreInfo storeInfo = storeResponse.getStore();

        Assert.assertNotNull(storeInfo.getHybridStoreConfig());
        Assert.assertEquals(storeInfo.getHybridStoreConfig().getOffsetLagThresholdToGoOnline(),
            expectedHybridOffsetLagThreshold);
        Assert.assertEquals(storeInfo.getHybridStoreConfig().getRewindTimeInSeconds(), expectedHybridRewindSeconds);
        Assert.assertEquals(storeInfo.getHybridStoreConfig().getBufferReplayPolicy(), expectedHybridBufferReplayPolicy);
        Assert.assertTrue(storeInfo.isLeaderFollowerModelEnabled());
        Assert.assertNotNull(storeInfo.getPartitionerConfig());
        Assert.assertEquals(storeInfo.getPartitionerConfig().getAmplificationFactor(), 2);
      }
    });

    // Turn off hybrid config so we can update the partitioner config.
    final UpdateStoreQueryParams updateStoreParams2 = new UpdateStoreQueryParams()
        .setHybridRewindSeconds(-1)
        .setHybridOffsetLagThreshold(-1);
    TestPushUtils.updateStore(clusterName, storeName, parentControllerClient, updateStoreParams2);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
      for (ControllerClient controllerClient : controllerClients) {
        StoreResponse storeResponse = controllerClient.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        StoreInfo storeInfo = storeResponse.getStore();
        Assert.assertNull(storeInfo.getHybridStoreConfig());
      }
    });

    // Update partitioner parameters make sure new update is in and other fields of partitioner config is not reset.
    final UpdateStoreQueryParams updateStoreParams3 = new UpdateStoreQueryParams().setPartitionerParams(Collections.singletonMap("key", "val"));
    TestPushUtils.updateStore(clusterName, storeName, parentControllerClient, updateStoreParams3);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
      for (ControllerClient controllerClient : controllerClients) {
        StoreResponse storeResponse = controllerClient.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        StoreInfo storeInfo = storeResponse.getStore();
        Assert.assertNotNull(storeInfo.getPartitionerConfig());
        Assert.assertEquals(storeInfo.getPartitionerConfig().getAmplificationFactor(), 2);
        Assert.assertEquals(storeInfo.getPartitionerConfig().getPartitionerParams(), Collections.singletonMap("key", "val"));
      }
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testUpdateStoreOnParentDoNotChangeIrrelevantConfig() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("store");

    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());

    /**
     * Create a test store
     */
    NewStoreResponse newStoreResponse = parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName,
        "", "\"string\"", "\"string\""));
    Assert.assertFalse(newStoreResponse.isError(), "The NewStoreResponse returned an error: " + newStoreResponse.getError());

    /**
     * Send UpdateStore to child controller in the first data center; update 3 configs:
     * 1. Storage quota set to 9527;
     * 2. L/F set to true;
     * 3. NR set to true.
     */
    long expectedStorageQuotaInDC0 = 9527;
    boolean expectedLeaderFollowerConfigInDC0 = true;
    boolean expectedNativeReplicationConfigInDC0 = true;
    ControllerClient dc0Client = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams()
        .setStorageQuotaInByte(expectedStorageQuotaInDC0)
        .setLeaderFollowerModel(expectedLeaderFollowerConfigInDC0)
        .setNativeReplicationEnabled(expectedNativeReplicationConfigInDC0);
    TestPushUtils.updateStore(clusterName, storeName, dc0Client, updateStoreParams);

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = dc0Client.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      StoreInfo storeInfo = storeResponse.getStore();
      Assert.assertEquals(storeInfo.getStorageQuotaInByte(), expectedStorageQuotaInDC0);
      Assert.assertEquals(storeInfo.isLeaderFollowerModelEnabled(), expectedLeaderFollowerConfigInDC0);
      Assert.assertEquals(storeInfo.isNativeReplicationEnabled(), expectedNativeReplicationConfigInDC0);
    });

    /**
     * Send UpdateStore to parent controller to update a store config that is irrelevant to the above 3 configs
     */
    long expectedReadQuota = 2021;
    UpdateStoreQueryParams updateStoreParamsOnParent = new UpdateStoreQueryParams().setReadQuotaInCU(expectedReadQuota);
    TestPushUtils.updateStore(clusterName, storeName, parentControllerClient, updateStoreParamsOnParent);

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
      Assert.assertEquals(storeInfo.isLeaderFollowerModelEnabled(), expectedLeaderFollowerConfigInDC0);
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
    boolean leaderFollowerModelInParent = parentStoreInfo.isLeaderFollowerModelEnabled();
    boolean nativeReplicationInParent = parentStoreInfo.isNativeReplicationEnabled();

    /**
     * Send an UpdateStore command to parent with "replicate-all-configs" flag turned on.
     */
    long newReadQuotaInParent = 116;
    UpdateStoreQueryParams forceUpdateStoreParamsOnParent = new UpdateStoreQueryParams()
        .setReadQuotaInCU(newReadQuotaInParent)
        .setReplicateAllConfigs(true);
    TestPushUtils.updateStore(clusterName, storeName, parentControllerClient, forceUpdateStoreParamsOnParent);

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
      Assert.assertEquals(storeInfo.isLeaderFollowerModelEnabled(), leaderFollowerModelInParent);
      Assert.assertEquals(storeInfo.isNativeReplicationEnabled(), nativeReplicationInParent);
    });

    /**
     * Last check; UpdateStore command without setting value to any configs or turning on the "replicate-all-configs"
     * flag should fail.
     */
    UpdateStoreQueryParams failUpdateStoreParamsOnParent = new UpdateStoreQueryParams();
    ControllerResponse failedUpdateStoreResponse = parentControllerClient.retryableRequest(5,
        c -> c.updateStore(storeName, failUpdateStoreParamsOnParent));
    Assert.assertTrue(failedUpdateStoreResponse.isError());
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testEnableActiveActiveReplicationSchema() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("store");
    String recordSchemaStr1 = BASIC_USER_SCHEMA_STRING_WITH_DEFAULT;
    String recordSchemaStr2 = TestPushUtils.USER_SCHEMA_STRING_SIMPLE_WITH_DEFAULT;
    String recordSchemaStr3 = TestPushUtils.USER_SCHEMA_STRING_WITH_DEFAULT;

    Schema replicationMetadataSchema1 = ReplicationMetadataSchemaGenerator.generateMetadataSchema(recordSchemaStr1, 1);
    Schema replicationMetadataSchema2 = ReplicationMetadataSchemaGenerator.generateMetadataSchema(recordSchemaStr2, 1);
    Schema replicationMetadataSchema3 = ReplicationMetadataSchemaGenerator.generateMetadataSchema(recordSchemaStr3, 1);

    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();

    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl())) {
      /**
       * Create a test store
       */
      NewStoreResponse newStoreResponse = parentControllerClient.retryableRequest(5,
          c -> c.createNewStore(storeName, "", "\"string\"", recordSchemaStr1));
      Assert.assertFalse(newStoreResponse.isError(), "The NewStoreResponse returned an error: " + newStoreResponse.getError());

      SchemaResponse schemaResponse2 = parentControllerClient.retryableRequest(5,
          c -> c.addValueSchema(storeName, recordSchemaStr2));
      Assert.assertFalse(schemaResponse2.isError(), "addValeSchema returned error: " + schemaResponse2.getError());

      UpdateStoreQueryParams updateStoreToEnableAARepl =
          new UpdateStoreQueryParams().setLeaderFollowerModel(true).setNativeReplicationEnabled(true).setActiveActiveReplicationEnabled(true);
      TestPushUtils.updateStore(clusterName, storeName, parentControllerClient, updateStoreToEnableAARepl);
      /**
       * Test Active/Active replication config enablement generates the active active metadata schema.
       */
      try (ControllerClient dc0Client = new ControllerClient(clusterName,
          childDatacenters.get(0).getControllerConnectString())) {
        TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, false, true, () -> {
          StoreResponse storeResponse = dc0Client.getStore(storeName);
          Assert.assertFalse(storeResponse.isError());
          StoreInfo storeInfo = storeResponse.getStore();

          Assert.assertTrue(storeInfo.isLeaderFollowerModelEnabled());
          Assert.assertTrue(storeInfo.isActiveActiveReplicationEnabled());
        });

        Admin veniceHelixAdmin = childDatacenters.get(0).getControllers().values().iterator().next().getVeniceAdmin();
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
              Collection<ReplicationMetadataSchemaEntry> replicationMetadataSchemas = veniceHelixAdmin.getReplicationMetadataSchemas(clusterName, storeName);
              Assert.assertEquals(replicationMetadataSchemas.size(), 1);
              Assert.assertEquals(replicationMetadataSchemas.iterator().next().getSchema(), replicationMetadataSchema2);
        });

        //Add a new value schema for the store and make sure the corresponding new metadata schema is generated.
        SchemaResponse schemaResponse3 =
            parentControllerClient.retryableRequest(5, c -> c.addValueSchema(storeName, recordSchemaStr3));
        Assert.assertFalse(schemaResponse3.isError(), "addValeSchema returned error: " + schemaResponse3.getError());

        Collection<ReplicationMetadataSchemaEntry> replicationMetadataSchemas = veniceHelixAdmin.getReplicationMetadataSchemas(clusterName, storeName);
        Assert.assertEquals(replicationMetadataSchemas.size(), 2);

        Iterator<ReplicationMetadataSchemaEntry> iterator = replicationMetadataSchemas.iterator();
        Assert.assertEquals(iterator.next().getSchema(), replicationMetadataSchema2);
        Assert.assertEquals(iterator.next().getSchema(), replicationMetadataSchema3);

        //Add a new version for the store and make sure all new metadata schema are generated.
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
          Assert.assertTrue(versions.get(0).isLeaderFollowerModelEnabled());
          Assert.assertTrue(versions.get(0).isActiveActiveReplicationEnabled());
          Assert.assertEquals(versions.get(0).getReplicationMetadataVersionId(), 1);
        });

        replicationMetadataSchemas = veniceHelixAdmin.getReplicationMetadataSchemas(clusterName, storeName);
        Assert.assertEquals(replicationMetadataSchemas.size(), 3);

        Iterator<ReplicationMetadataSchemaEntry> iterator2 = replicationMetadataSchemas.iterator();
        Assert.assertEquals(iterator2.next().getSchema(), replicationMetadataSchema1);
        Assert.assertEquals(iterator2.next().getSchema(), replicationMetadataSchema2);
        Assert.assertEquals(iterator2.next().getSchema(), replicationMetadataSchema3);
      }
    }
  }
}
