package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_NUMBER_OF_PARTITION_FOR_HYBRID;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFER_VERSION_SWAP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.fail;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.VeniceETLStrategy;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
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

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private List<VeniceMultiClusterWrapper> childDatacenters;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;

  private static final String BASIC_USER_SCHEMA_STRING_WITH_DEFAULT = "{" + "  \"namespace\" : \"example.avro\",  "
      + "  \"type\": \"record\",   " + "  \"name\": \"User\",     " + "  \"fields\": [           "
      + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"\"}  " + "  ] " + " } ";

  private static final Logger LOGGER = LogManager.getLogger(TestParentControllerWithMultiDataCenter.class);

  @BeforeClass
  public void setUp() {
    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_NUMBER_OF_PARTITION_FOR_HYBRID, 2);
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 3);
    controllerProps.put(DEFAULT_PARTITION_SIZE, 1024);
    Properties serverProps = new Properties();
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(2)
            .numberOfRouters(1)
            .replicationFactor(2)
            .forkServer(false)
            .parentControllerProperties(controllerProps)
            .childControllerProperties(controllerProps)
            .serverProperties(serverProps);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());

    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRTTopicDeletionWithHybridAndIncrementalVersions() {
    String storeName = Utils.getUniqueString("testRTTopicDeletion");
    String clusterName = CLUSTER_NAMES[0];
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    ControllerClient parentControllerClient =
        ControllerClient.constructClusterControllerClient(clusterName, parentControllerURLs);
    ControllerClient[] childControllerClients = new ControllerClient[childDatacenters.size()];
    for (int i = 0; i < childDatacenters.size(); i++) {
      childControllerClients[i] =
          new ControllerClient(clusterName, childDatacenters.get(i).getControllerConnectString());
    }

    NewStoreResponse newStoreResponse =
        parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
    Assert.assertFalse(
        newStoreResponse.isError(),
        "The NewStoreResponse returned an error: " + newStoreResponse.getError());

    TestUtils.assertCommand(parentControllerClient.getStore(storeName));

    String metaSystemStoreTopic =
        Version.composeKafkaTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName), 1);
    TestUtils.waitForNonDeterministicPushCompletion(metaSystemStoreTopic, parentControllerClient, 30, TimeUnit.SECONDS);

    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams();
    updateStoreParams.setBackupStrategy(BackupStrategy.KEEP_MIN_VERSIONS)
        .setActiveActiveReplicationEnabled(true)
        .setIncrementalPushEnabled(true)
        .setNumVersionsToPreserve(2)
        .setHybridRewindSeconds(1000)
        .setHybridOffsetLagThreshold(1000);
    TestWriteUtils.updateStore(storeName, parentControllerClient, updateStoreParams);

    // create new version by doing an empty push
    ControllerResponse response = parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);
    PubSubTopic versionPubsubTopic = getVersionPubsubTopic(storeName, response);

    List<TopicManager> topicManagers = new ArrayList<>(2);
    topicManagers
        .add(childDatacenters.get(0).getControllers().values().iterator().next().getVeniceAdmin().getTopicManager());
    topicManagers
        .add(childDatacenters.get(1).getControllers().values().iterator().next().getVeniceAdmin().getTopicManager());

    StoreInfo store = parentControllerClient.getStore(storeName).getStore();
    String rtTopicName = Utils.getRealTimeTopicName(store);
    PubSubTopic rtPubSubTopic = pubSubTopicRepository.getTopic(rtTopicName);

    for (TopicManager topicManager: topicManagers) {
      Assert.assertTrue(topicManager.containsTopic(versionPubsubTopic));
      Assert.assertTrue(topicManager.containsTopic(rtPubSubTopic));
    }

    for (ControllerClient controllerClient: childControllerClients) {
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 1);
    }

    // create new version by doing an empty push
    response = parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);
    versionPubsubTopic = getVersionPubsubTopic(storeName, response);

    for (TopicManager topicManager: topicManagers) {
      Assert.assertTrue(topicManager.containsTopic(versionPubsubTopic));
      Assert.assertTrue(topicManager.containsTopic(rtPubSubTopic));
    }
    for (ControllerClient controllerClient: childControllerClients) {
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 2);
    }

    // change store from hybrid to batch-only
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setHybridRewindSeconds(-1).setHybridTimeLagThreshold(-1).setHybridOffsetLagThreshold(-1);
    TestWriteUtils.updateStore(storeName, parentControllerClient, params);

    // create new version by doing an empty push
    response = parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);

    // at this point, the current version should be batch-only, but the older version should be hybrid, so rt topic
    // should not get deleted
    versionPubsubTopic = getVersionPubsubTopic(storeName, response);

    for (ControllerClient controllerClient: childControllerClients) {
      StoreInfo storeInfo = controllerClient.getStore(storeName).getStore();
      int currentVersion = storeInfo.getCurrentVersion();
      Assert.assertEquals(currentVersion, 3);
      Assert.assertNull(storeInfo.getVersion(currentVersion).get().getHybridStoreConfig());
      Assert.assertNotNull(storeInfo.getVersion(currentVersion - 1).get().getHybridStoreConfig());
    }

    for (TopicManager topicManager: topicManagers) {
      Assert.assertTrue(topicManager.containsTopic(versionPubsubTopic));
      Assert.assertTrue(topicManager.containsTopic(rtPubSubTopic));
    }

    // create new version by doing an empty push
    response = parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);
    versionPubsubTopic = getVersionPubsubTopic(storeName, response);
    for (ControllerClient controllerClient: childControllerClients) {
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 4);
    }

    // now both the versions should be batch-only, so rt topic should get deleted by TopicCleanupService
    for (TopicManager topicManager: topicManagers) {
      Assert.assertTrue(topicManager.containsTopic(versionPubsubTopic));
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
        StoreInfo finalStore = parentControllerClient.getStore(storeName).getStore();
        String finalRtTopicName = Utils.getRealTimeTopicName(finalStore);
        PubSubTopic finalRtPubSubTopic = pubSubTopicRepository.getTopic(finalRtTopicName);
        Assert.assertFalse(topicManager.containsTopic(finalRtPubSubTopic));
      });
    }

    /*
     todo - RT topics are not used in parent controller in the current architecture, so we can ignore any RT topics in parent
     controller that exist because they are still on old architecture.
    
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true,  true, () -> {
        Assert.assertFalse(parentTopicManager.containsTopic(rtPubSubTopic));
      }
     );
    */
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
      final UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams().setStorageQuotaInByte(1)
          .setHybridRewindSeconds(expectedHybridRewindSeconds)
          .setHybridOffsetLagThreshold(expectedHybridOffsetLagThreshold)
          .setHybridBufferReplayPolicy(expectedHybridBufferReplayPolicy)
          .setChunkingEnabled(true)
          .setRmdChunkingEnabled(true)
          .setStorageNodeReadQuotaEnabled(true);

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
          assertEquals(
              storeInfo.getHybridStoreConfig().getOffsetLagThresholdToGoOnline(),
              expectedHybridOffsetLagThreshold);
          assertEquals(storeInfo.getHybridStoreConfig().getRewindTimeInSeconds(), expectedHybridRewindSeconds);
          assertEquals(storeInfo.getHybridStoreConfig().getBufferReplayPolicy(), expectedHybridBufferReplayPolicy);
          Assert.assertNotNull(storeInfo.getPartitionerConfig());
          Assert.assertTrue(storeInfo.isChunkingEnabled());
          Assert.assertTrue(storeInfo.isRmdChunkingEnabled());
          assertEquals(storeInfo.getPartitionCount(), 2); // hybrid partition count from the config
          Assert.assertTrue(storeInfo.isStorageNodeReadQuotaEnabled());
        }
      });

      // Turn off hybrid config, so we can update the partitioner config.
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
          assertEquals(storeInfo.getPartitionerConfig().getPartitionerParams(), Collections.singletonMap("key", "val"));
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
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setHybridRewindSeconds(expectedHybridRewindSeconds)
              .setHybridOffsetLagThreshold(expectedHybridOffsetLagThreshold)
              .setHybridBufferReplayPolicy(expectedHybridBufferReplayPolicy)
              .setActiveActiveReplicationEnabled(true)
              .setIncrementalPushEnabled(true));
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        for (ControllerClient controllerClient: controllerClients) {
          StoreResponse storeResponse = controllerClient.getStore(incPushStoreName);
          Assert.assertFalse(storeResponse.isError());
          StoreInfo storeInfo = storeResponse.getStore();
          Assert.assertNotNull(storeInfo.getHybridStoreConfig());
          assertEquals(storeInfo.getPartitionCount(), 3); // max partition count from the config
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
        assertEquals(storeInfo.getStorageQuotaInByte(), expectedStorageQuotaInDC0);
        assertEquals(storeInfo.isNativeReplicationEnabled(), expectedNativeReplicationConfigInDC0);
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
        assertEquals(storeInfo.getReadQuotaInCU(), expectedReadQuota);

        /**
         * By default, changing read quota in parent should not propagate the other store configs from parent to child;
         * so the below 3 configs in DC0 should remain unchanged.
         */
        assertEquals(storeInfo.getStorageQuotaInByte(), expectedStorageQuotaInDC0);
        assertEquals(storeInfo.isNativeReplicationEnabled(), expectedNativeReplicationConfigInDC0);
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
        assertEquals(storeInfo.getReadQuotaInCU(), newReadQuotaInParent);

        /**
         * Store configs in child datacenter should be identical to the store configs in parent
         */
        assertEquals(storeInfo.getStorageQuotaInByte(), storageQuotaInParent);
        assertEquals(storeInfo.isNativeReplicationEnabled(), nativeReplicationInParent);
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
    String valueRecordSchemaStr2 = TestWriteUtils.SIMPLE_USER_WITH_DEFAULT_SCHEMA.toString();
    String valueRecordSchemaStr3 = TestWriteUtils.USER_WITH_DEFAULT_SCHEMA.toString();

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
          assertEquals(replicationMetadataSchemas.size(), 2);
          Iterator<RmdSchemaEntry> iterator = replicationMetadataSchemas.iterator();
          assertEquals(iterator.next().getSchema(), rmdSchema1);
          assertEquals(iterator.next().getSchema(), rmdSchema2);
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
          assertEquals(replicationMetadataSchemas.size(), 3);
          Iterator<RmdSchemaEntry> iterator = replicationMetadataSchemas.iterator();
          assertEquals(iterator.next().getSchema(), rmdSchema1);
          assertEquals(iterator.next().getSchema(), rmdSchema2);
          assertEquals(iterator.next().getSchema(), rmdSchema3);
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
          assertEquals(versions.size(), 1);
          Assert.assertTrue(versions.get(0).isActiveActiveReplicationEnabled());
          assertEquals(versions.get(0).getRmdVersionId(), 1);
        });

        Collection<RmdSchemaEntry> replicationMetadataSchemas =
            veniceHelixAdmin.getReplicationMetadataSchemas(clusterName, storeName);
        assertEquals(replicationMetadataSchemas.size(), 3);

        Iterator<RmdSchemaEntry> iterator = replicationMetadataSchemas.iterator();
        assertEquals(iterator.next().getSchema(), rmdSchema1);
        assertEquals(iterator.next().getSchema(), rmdSchema2);
        assertEquals(iterator.next().getSchema(), rmdSchema3);
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
      emptyPushToStore(parentControllerClient, childControllerClients, storeName, 2);
      // Should roll back to version 1
      parentControllerClient.rollbackToBackupVersion(storeName);
      for (ControllerClient childControllerClient: childControllerClients) {
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
          StoreResponse storeResponse = childControllerClient.getStore(storeName);
          Assert.assertFalse(storeResponse.isError());
          StoreInfo storeInfo = storeResponse.getStore();
          assertEquals(storeInfo.getCurrentVersion(), 1);
        });
      }

      // roll forward only in dc-0. version should still stay at 1 as v2 is marked as error
      parentControllerClient.rollForwardToFutureVersion(storeName, childDatacenters.get(0).getRegionName());
      for (ControllerClient childControllerClient: childControllerClients) {
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
          StoreResponse storeResponse = childControllerClient.getStore(storeName);
          Assert.assertFalse(storeResponse.isError());
          StoreInfo storeInfo = storeResponse.getStore();
          assertEquals(storeInfo.getCurrentVersion(), 1);
        });
      }
    }
  }

  @Test
  public void testDeleteStoreRTDeletion() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("testDeleteStore");
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
      ControllerResponse response = parentControllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setHybridOffsetLagThreshold(1).setHybridRewindSeconds(60));

      Assert.assertFalse(response.isError(), "Update hybrid store returned an error");

      List<ControllerClient> childControllerClients = new ArrayList<>();
      childControllerClients.add(dc0Client);
      childControllerClients.add(dc1Client);
      emptyPushToStore(parentControllerClient, childControllerClients, storeName, 1);
      List<TopicManager> childDatacenterTopicManagers = new ArrayList<>(2);
      childDatacenterTopicManagers
          .add(childDatacenters.get(0).getControllers().values().iterator().next().getVeniceAdmin().getTopicManager());
      childDatacenterTopicManagers
          .add(childDatacenters.get(1).getControllers().values().iterator().next().getVeniceAdmin().getTopicManager());

      StoreInfo storeInfo = parentControllerClient.getStore(storeName).getStore();
      String storeRT = Utils.getRealTimeTopicName(storeInfo);
      String pushStatusSystemStoreRT =
          Utils.composeRealTimeTopic(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName));
      String metaSystemStoreRT = Utils.composeRealTimeTopic(VeniceSystemStoreUtils.getMetaStoreName(storeName));

      // Ensure all the RT topics are created in all child datacenters
      TestUtils.waitForNonDeterministicAssertion(300, TimeUnit.SECONDS, false, true, () -> {
        for (TopicManager topicManager: childDatacenterTopicManagers) {
          Assert.assertTrue(topicManager.containsTopic(pubSubTopicRepository.getTopic(storeRT)));
          Assert.assertTrue(topicManager.containsTopic(pubSubTopicRepository.getTopic(pushStatusSystemStoreRT)));
          Assert.assertTrue(topicManager.containsTopic(pubSubTopicRepository.getTopic(metaSystemStoreRT)));
        }
      });

      response = parentControllerClient.disableAndDeleteStore(storeName);
      Assert.assertFalse(response.isError(), "Delete store returned an error");

      // Ensure all the RT topics are deleted in all child datacenters
      TestUtils.waitForNonDeterministicAssertion(600, TimeUnit.SECONDS, false, true, () -> {
        for (TopicManager topicManager: childDatacenterTopicManagers) {
          Assert.assertFalse(topicManager.containsTopic(pubSubTopicRepository.getTopic(storeRT)));
          Assert.assertFalse(topicManager.containsTopic(pubSubTopicRepository.getTopic(pushStatusSystemStoreRT)));
          Assert.assertFalse(topicManager.containsTopic(pubSubTopicRepository.getTopic(metaSystemStoreRT)));
        }
      });
    }
  }

  @Test
  public void testDeferredVersionSwap() throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    props.setProperty(DEFER_VERSION_SWAP, "true");
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms =
        new UpdateStoreQueryParams().setPartitionCount(1).setBootstrapToOnlineTimeoutInHours(1);
    createStoreForJob(CLUSTER_NAMES[0], keySchemaStr, valueSchemaStr, props, storeParms).close();

    IntegrationTestPushUtils.runVPJ(props);
    try {
      IntegrationTestPushUtils.runVPJ(props);
      fail("Deferred version swap should fail second push");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Unable to start the push with pushJobId"));
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testUpdateStoreInParentAfterDeletingInChildren() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("storeDeletedInChildren");

    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    try (ControllerClient parentControllerClient =
        ControllerClient.constructClusterControllerClient(clusterName, parentControllerURLs)) {
      // 1. Create a test store through parent controller
      NewStoreResponse newStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
      Assert.assertFalse(
          newStoreResponse.isError(),
          "The NewStoreResponse returned an error: " + newStoreResponse.getError());

      // Wait for store creation to be reflected in all child controllers
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        for (int i = 0; i < childDatacenters.size(); i++) {
          ControllerClient childClient =
              new ControllerClient(clusterName, childDatacenters.get(i).getControllerConnectString());
          StoreResponse storeResponse = childClient.getStore(storeName);
          Assert.assertFalse(storeResponse.isError(), "Store should exist in child datacenter " + i);
          childClient.close();
        }
      });

      // Update read quota as a random store config change
      long newReadQuota = 110L;
      UpdateStoreQueryParams updateParams = new UpdateStoreQueryParams().setReadQuotaInCU(newReadQuota);
      ControllerResponse quotaUpdateResponse = parentControllerClient.updateStore(storeName, updateParams);
      Assert
          .assertFalse(quotaUpdateResponse.isError(), "Failed to update read quota: " + quotaUpdateResponse.getError());

      // Verify the read quota update is reflected in all child controllers
      // The update store command is critical in this test case - it's used as a synchronization point between the
      // PUSH_STATUS_SYSTEM_STORE_AUTO_CREATION_VALIDATION message and the DELETE_STORE message in child controllers.
      // Since the push jobs to the system stores are triggered in parent controller, it's possible that the pushes to
      // system stores already completed, but the PUSH_STATUS_SYSTEM_STORE_AUTO_CREATION_VALIDATION messages haven't
      // been processed yet in child controllers, so checking the push jobs completion of system stores is not enough.
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        for (int i = 0; i < childDatacenters.size(); i++) {
          ControllerClient childClient =
              new ControllerClient(clusterName, childDatacenters.get(i).getControllerConnectString());
          StoreResponse storeResponse = childClient.getStore(storeName);
          Assert.assertFalse(storeResponse.isError(), "Failed to get store from child datacenter " + i);
          StoreInfo storeInfo = storeResponse.getStore();
          Assert.assertEquals(
              storeInfo.getReadQuotaInCU(),
              newReadQuota,
              "Read quota should be updated to " + newReadQuota + " in child datacenter " + i);
          childClient.close();
        }
      });

      String metaSystemStoreTopic =
          Version.composeKafkaTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName), 1);
      String pushStatusSystemStoreTopic =
          Version.composeKafkaTopic(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName), 1);

      // 2. Delete the store in all child regions by sending requests directly to child controllers
      for (int i = 0; i < childDatacenters.size(); i++) {
        try (ControllerClient childClient =
            new ControllerClient(clusterName, childDatacenters.get(i).getControllerConnectString())) {
          // Ensure the pushes to system stores are completed before deleting the parent store
          TestUtils.waitForNonDeterministicPushCompletion(metaSystemStoreTopic, childClient, 30, TimeUnit.SECONDS);
          TestUtils
              .waitForNonDeterministicPushCompletion(pushStatusSystemStoreTopic, childClient, 30, TimeUnit.SECONDS);

          ControllerResponse deleteResponse = childClient.disableAndDeleteStore(storeName);
          Assert.assertFalse(
              deleteResponse.isError(),
              "Failed to delete store in child datacenter " + i + ": " + deleteResponse.getError());
        }
      }

      // Verify store is deleted in all child controllers
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        for (int i = 0; i < childDatacenters.size(); i++) {
          ControllerClient childClient =
              new ControllerClient(clusterName, childDatacenters.get(i).getControllerConnectString());
          StoreResponse storeResponse = childClient.getStore(storeName);
          Assert.assertTrue(
              storeResponse.isError(),
              "Store should not exist in child datacenter " + i + " after deletion");
          Assert.assertTrue(
              storeResponse.getError().contains("does not exist"),
              "Error message should indicate store doesn't exist in child datacenter " + i);
          childClient.close();
        }
      });

      // Record the last successful execution ID for each child datacenter before the update
      Map<Integer, Long> lastSuccessfulExecutionIds = new HashMap<>();
      for (int i = 0; i < childDatacenters.size(); i++) {
        VeniceMultiClusterWrapper childDC = childDatacenters.get(i);
        Admin childAdmin = childDC.getControllers().values().iterator().next().getVeniceAdmin();
        AdminConsumerService adminConsumerService = childAdmin.getAdminConsumerService(clusterName);
        Long executionId = adminConsumerService.getLastSucceededExecutionId(storeName);
        lastSuccessfulExecutionIds.put(i, executionId);
        LOGGER.info(
            "Child datacenter " + i + " has execution ID " + executionId + " for store " + storeName
                + " before update");
      }

      // 3. Send an updateStore command through parent controller to disable write on the store
      UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams().setEnableWrites(false);
      ControllerResponse updateResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));

      // The update should succeed in the parent controller since the store still exists there
      Assert.assertFalse(
          updateResponse.isError(),
          "Update in parent should succeed, but got error: " + updateResponse.getError());

      // Check if the store in parent was updated successfully
      StoreResponse parentStoreResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(parentStoreResponse.isError(), "Store should exist in parent controller");
      StoreInfo storeInfo = parentStoreResponse.getStore();
      Assert.assertFalse(storeInfo.isEnableStoreWrites(), "Store write should be disabled in parent");

      // Verify that initially the admin consumption might fail in child controllers due to store not existing,
      // but after MAX_RETRIES_FOR_NONEXISTENT_STORE retries, the error should be tolerated
      for (int i = 0; i < childDatacenters.size(); i++) {
        int datacenterId = i;
        VeniceMultiClusterWrapper childDC = childDatacenters.get(datacenterId);
        Admin childAdmin = childDC.getControllers().values().iterator().next().getVeniceAdmin();
        AdminConsumerService adminConsumerService = childAdmin.getAdminConsumerService(clusterName);

        // After MAX_RETRIES_FOR_NONEXISTENT_STORE retries, there should be no exception anymore
        // as the message should be skipped
        TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, false, true, () -> {
          Exception lastException = adminConsumerService.getLastExceptionForStore(storeName);
          Assert.assertNull(
              lastException,
              "Exception should be null after max retries in child datacenter " + datacenterId);

          // Verify execution ID hasn't changed
          Long currentExecutionId = adminConsumerService.getLastSucceededExecutionId(storeName);
          Long previousExecutionId = lastSuccessfulExecutionIds.get(datacenterId);

          LOGGER.info(
              "Child datacenter " + datacenterId + " has execution ID " + currentExecutionId + " for store " + storeName
                  + " after update (previous: " + previousExecutionId + ")");

          // The execution ID should be the same as before or both null since we're skipping the message
          if (previousExecutionId != null) {
            Assert.assertEquals(
                currentExecutionId,
                previousExecutionId,
                "Execution ID should not change after skipping the admin message in child datacenter " + datacenterId);
          }
        });
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testSelfManagedTTLRepushEnabledStoreProperty() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("testTTLRepushEnabled");
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs)) {
      NewStoreResponse newStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
      Assert.assertFalse(
          newStoreResponse.isError(),
          "The NewStoreResponse returned an error: " + newStoreResponse.getError());
      // ttlRepushEnabled flag should be false by default
      getAndAssertTTLRepushEnabledFlag(parentControllerClient, storeName, false);
      for (VeniceMultiClusterWrapper veniceMultiClusterWrapper: multiRegionMultiClusterWrapper.getChildRegions()) {
        try (ControllerClient childControllerClient =
            new ControllerClient(clusterName, veniceMultiClusterWrapper.getControllerConnectString())) {
          getAndAssertTTLRepushEnabledFlag(childControllerClient, storeName, false);
        }
      }
      // A TTL re-push should enable the flag
      String ttlRePushId = Version.generateTTLRePushId("test-ttl-re-push");
      VersionCreationResponse ttlRePushVersionCreation =
          mimicVPJPushVersionCreation(parentControllerClient, storeName, ttlRePushId);
      getAndAssertTTLRepushEnabledFlag(parentControllerClient, storeName, true);
      for (VeniceMultiClusterWrapper veniceMultiClusterWrapper: multiRegionMultiClusterWrapper.getChildRegions()) {
        try (ControllerClient childControllerClient =
            new ControllerClient(clusterName, veniceMultiClusterWrapper.getControllerConnectString())) {
          TestUtils.waitForNonDeterministicAssertion(
              10,
              TimeUnit.SECONDS,
              () -> getAndAssertTTLRepushEnabledFlag(childControllerClient, storeName, true));
        }
      }
      parentControllerClient.killOfflinePushJob(ttlRePushVersionCreation.getKafkaTopic());
      // The override batch push should disable the flag
      String overrideRegularPushId = Version.generateRegularPushWithTTLRePushId("regular-test-push-on-ttl-re-push");
      mimicVPJPushVersionCreation(parentControllerClient, storeName, overrideRegularPushId);
      getAndAssertTTLRepushEnabledFlag(parentControllerClient, storeName, false);
      for (VeniceMultiClusterWrapper veniceMultiClusterWrapper: multiRegionMultiClusterWrapper.getChildRegions()) {
        try (ControllerClient childControllerClient =
            new ControllerClient(clusterName, veniceMultiClusterWrapper.getControllerConnectString())) {
          TestUtils.waitForNonDeterministicAssertion(
              10,
              TimeUnit.SECONDS,
              () -> getAndAssertTTLRepushEnabledFlag(childControllerClient, storeName, false));
        }
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testETLStoreConfig() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("test-etl-store-config");
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs)) {
      NewStoreResponse newStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
      Assert.assertFalse(
          newStoreResponse.isError(),
          "The NewStoreResponse returned an error: " + newStoreResponse.getError());
      String etlUserProxyAccount = "etl-user-test";
      Assert.assertFalse(
          parentControllerClient
              .updateStore(
                  storeName,
                  new UpdateStoreQueryParams().setRegularVersionETLEnabled(true)
                      .setEtledProxyUserAccount(etlUserProxyAccount))
              .isError());
      StoreResponse storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      ETLStoreConfig etlStoreConfig = storeResponse.getStore().getEtlStoreConfig();
      verifyETLStoreConfig(etlStoreConfig, true, false, etlUserProxyAccount, VeniceETLStrategy.EXTERNAL_SERVICE);
      for (VeniceMultiClusterWrapper veniceMultiClusterWrapper: multiRegionMultiClusterWrapper.getChildRegions()) {
        try (ControllerClient childControllerClient =
            new ControllerClient(clusterName, veniceMultiClusterWrapper.getControllerConnectString())) {
          TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
            StoreResponse childStoreResponse = childControllerClient.getStore(storeName);
            Assert.assertFalse(childStoreResponse.isError());
            verifyETLStoreConfig(
                childStoreResponse.getStore().getEtlStoreConfig(),
                true,
                false,
                etlUserProxyAccount,
                VeniceETLStrategy.EXTERNAL_SERVICE);
          });
        }
      }
      parentControllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setETLStrategy(VeniceETLStrategy.EXTERNAL_WITH_VENICE_TRIGGER));
      storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      verifyETLStoreConfig(
          storeResponse.getStore().getEtlStoreConfig(),
          true,
          false,
          etlUserProxyAccount,
          VeniceETLStrategy.EXTERNAL_WITH_VENICE_TRIGGER);
      for (VeniceMultiClusterWrapper veniceMultiClusterWrapper: multiRegionMultiClusterWrapper.getChildRegions()) {
        try (ControllerClient childControllerClient =
            new ControllerClient(clusterName, veniceMultiClusterWrapper.getControllerConnectString())) {
          TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
            StoreResponse childStoreResponse = childControllerClient.getStore(storeName);
            Assert.assertFalse(childStoreResponse.isError());
            verifyETLStoreConfig(
                childStoreResponse.getStore().getEtlStoreConfig(),
                true,
                false,
                etlUserProxyAccount,
                VeniceETLStrategy.EXTERNAL_WITH_VENICE_TRIGGER);
          });
        }
      }
    }
  }

  private void verifyETLStoreConfig(
      ETLStoreConfig etlStoreConfig,
      boolean regularVersionETLEnabled,
      boolean futureVersionETLEnabled,
      String etlUserProxyAccount,
      VeniceETLStrategy veniceETLStrategy) {
    Assert.assertEquals(etlStoreConfig.isRegularVersionETLEnabled(), regularVersionETLEnabled);
    Assert.assertEquals(etlStoreConfig.isFutureVersionETLEnabled(), futureVersionETLEnabled);
    Assert.assertEquals(etlStoreConfig.getEtledUserProxyAccount(), etlUserProxyAccount);
    Assert.assertEquals(etlStoreConfig.getETLStrategy(), veniceETLStrategy);
  }

  private void getAndAssertTTLRepushEnabledFlag(
      ControllerClient controllerClient,
      String storeName,
      boolean expectedTTLRepushEnabled) {
    StoreResponse storeResponse = controllerClient.getStore(storeName);
    Assert.assertFalse(storeResponse.isError());
    Assert.assertEquals(storeResponse.getStore().isTTLRepushEnabled(), expectedTTLRepushEnabled);
  }

  private VersionCreationResponse mimicVPJPushVersionCreation(
      ControllerClient controllerClient,
      String storeName,
      String pushId) {
    return controllerClient.retryableRequest(
        5,
        c -> c.requestTopicForWrites(
            storeName,
            1000,
            Version.PushType.BATCH,
            pushId,
            true,
            true,
            false,
            Optional.of(DefaultVenicePartitioner.class.getName()),
            Optional.empty(),
            Optional.ofNullable(multiRegionMultiClusterWrapper.getChildRegions().get(0).getRegionName()),
            false,
            -1,
            false,
            null,
            0,
            false,
            -1));
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
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = childControllerClient.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        StoreInfo storeInfo = storeResponse.getStore();
        assertEquals(storeInfo.getCurrentVersion(), expectedVersion);
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testUserPushKillsCompliancePush() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("testCompliancePushKill");
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs)) {
      // Create a new store
      NewStoreResponse newStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
      Assert.assertFalse(
          newStoreResponse.isError(),
          "The NewStoreResponse returned an error: " + newStoreResponse.getError());

      // Start a compliance push
      String compliancePushId = Version.generateCompliancePushId("test-compliance-push");
      VersionCreationResponse compliancePushResponse =
          mimicVPJPushVersionCreation(parentControllerClient, storeName, compliancePushId);
      Assert.assertFalse(
          compliancePushResponse.isError(),
          "Compliance push creation failed: " + compliancePushResponse.getError());
      int compliancePushVersion = compliancePushResponse.getVersion();

      // Start a user-initiated batch push - this should kill the compliance push
      String userPushId = "user-initiated-push-" + System.currentTimeMillis();
      VersionCreationResponse userPushResponse =
          mimicVPJPushVersionCreation(parentControllerClient, storeName, userPushId);
      Assert.assertFalse(userPushResponse.isError(), "User push creation failed: " + userPushResponse.getError());
      int userPushVersion = userPushResponse.getVersion();

      // User push should have created a new version (compliance push was killed)
      Assert.assertEquals(
          userPushVersion,
          compliancePushVersion + 1,
          "User push should create version " + (compliancePushVersion + 1) + " after killing compliance push");
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testCompliancePushCannotKillAnotherCompliancePush() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("testCompliancePushNoKill");
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs)) {
      // Create a new store
      NewStoreResponse newStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
      Assert.assertFalse(
          newStoreResponse.isError(),
          "The NewStoreResponse returned an error: " + newStoreResponse.getError());

      // Start a compliance push
      String compliancePushId1 = Version.generateCompliancePushId("test-compliance-push-1");
      VersionCreationResponse compliancePushResponse1 =
          mimicVPJPushVersionCreation(parentControllerClient, storeName, compliancePushId1);
      Assert.assertFalse(
          compliancePushResponse1.isError(),
          "First compliance push creation failed: " + compliancePushResponse1.getError());

      // Start another compliance push - this should NOT kill the first one (should fail with concurrent push error)
      String compliancePushId2 = Version.generateCompliancePushId("test-compliance-push-2");
      VersionCreationResponse compliancePushResponse2 =
          mimicVPJPushVersionCreation(parentControllerClient, storeName, compliancePushId2);
      Assert.assertTrue(
          compliancePushResponse2.isError(),
          "Second compliance push should fail because system pushes cannot kill other system pushes");
      Assert.assertTrue(
          compliancePushResponse2.getError().contains("An ongoing push") && compliancePushResponse2.getError()
              .contains("is found and it must be terminated before another push can be started"),
          "Error should indicate an ongoing push must be terminated: " + compliancePushResponse2.getError());
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testCompliancePushCannotKillUserPush() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("testCompliancePushNoKillUser");
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs)) {
      // Create a new store
      NewStoreResponse newStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
      Assert.assertFalse(
          newStoreResponse.isError(),
          "The NewStoreResponse returned an error: " + newStoreResponse.getError());

      // Start a user push (regular push ID without any system prefix)
      String userPushId = System.currentTimeMillis() + "_https://example.com/user-push-job";
      VersionCreationResponse userPushResponse =
          mimicVPJPushVersionCreation(parentControllerClient, storeName, userPushId);
      Assert.assertFalse(userPushResponse.isError(), "User push creation failed: " + userPushResponse.getError());

      // Start a compliance push - this should NOT kill the user push (should fail with concurrent push error)
      String compliancePushId = Version.generateCompliancePushId("test-compliance-push");
      VersionCreationResponse compliancePushResponse =
          mimicVPJPushVersionCreation(parentControllerClient, storeName, compliancePushId);
      Assert.assertTrue(
          compliancePushResponse.isError(),
          "Compliance push should fail because system pushes cannot kill user pushes");
      Assert.assertTrue(
          compliancePushResponse.getError().contains("An ongoing push") && compliancePushResponse.getError()
              .contains("is found and it must be terminated before another push can be started"),
          "Error should indicate an ongoing push must be terminated: " + compliancePushResponse.getError());
    }
  }

  static PubSubTopic getVersionPubsubTopic(String storeName, ControllerResponse response) {
    assertFalse(response.isError(), "Failed to perform empty push on test store");
    String versionTopic = null;
    if (response instanceof VersionCreationResponse) {
      versionTopic = ((VersionCreationResponse) response).getKafkaTopic();
    } else if (response instanceof JobStatusQueryResponse) {
      versionTopic = Version.composeKafkaTopic(storeName, ((JobStatusQueryResponse) response).getVersion());
    }
    return new PubSubTopicRepository().getTopic(versionTopic);
  }
}
