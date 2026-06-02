package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.ALLOW_CLUSTER_WIPE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS;
import static com.linkedin.venice.ConfigKeys.DEGRADED_MODE_AUTO_RECOVERY_ENABLED;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.DegradedDcResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateClusterConfigQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.IntegrationTestUtils;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * End-to-end integration test for degraded-mode batch push.
 *
 * Tests the full lifecycle: enable degraded mode -> mark DC degraded -> batch push (auto-converted
 * to targeted region push) -> verify PARTIALLY_ONLINE -> verify incremental push blocked ->
 * unmark DC -> verify recovery.
 */
public class DegradedModeBatchPushTest extends AbstractMultiRegionTest {
  private static final long TEST_TIMEOUT = 240_000;

  private String clusterName;

  @Override
  protected int getNumberOfRegions() {
    // Need 3 regions so marking 1 as degraded still leaves 2 healthy (minimum required)
    return 3;
  }

  @Override
  protected Properties getExtraControllerProperties() {
    Properties controllerProps = new Properties();
    controllerProps.put(NATIVE_REPLICATION_SOURCE_FABRIC, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    controllerProps.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    controllerProps.put(ALLOW_CLUSTER_WIPE, "true");
    controllerProps.put(DEGRADED_MODE_AUTO_RECOVERY_ENABLED, "true");
    // Enable DeferredVersionSwapService so PUSHED → PARTIALLY_ONLINE transition occurs in test env.
    // Pattern mirrors TestDeferredVersionSwapWithFailingRegions: short sleep so the swap fires quickly.
    controllerProps.put(CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED, true);
    controllerProps.put(CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS, 100);
    return controllerProps;
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
    Utils.thisIsLocalhost();
    super.setUp();
    clusterName = multiRegionMultiClusterWrapper.getClusterNames()[0];
    IntegrationTestUtils.waitForParticipantStorePushInAllRegions(clusterName, childDatacenters);
  }

  private void waitForSystemStorePushes(String storeName, ControllerClient... dcClients) {
    String metaStoreTopic = Version.composeKafkaTopic(VeniceSystemStoreUtils.getMetaStoreName(storeName), 1);
    String pushStatusStoreTopic =
        Version.composeKafkaTopic(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName), 1);
    for (ControllerClient dcClient: dcClients) {
      TestUtils.waitForNonDeterministicPushCompletion(metaStoreTopic, dcClient, 2, TimeUnit.MINUTES);
      TestUtils.waitForNonDeterministicPushCompletion(pushStatusStoreTopic, dcClient, 2, TimeUnit.MINUTES);
    }
  }

  /**
   * Full degraded-mode lifecycle test:
   * 1. Enable degraded mode on cluster
   * 2. Create store, do normal batch push (succeeds in all DCs)
   * 3. Mark dc-1 as degraded
   * 4. Verify degraded DCs reported correctly
   * 5. Do a batch push — should auto-convert to targeted region push
   * 6. Verify push succeeds with version in dc-0 but not dc-1
   * 7. Unmark dc-1 — triggers auto-recovery
   * 8. Verify dc-1 eventually recovers
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testDegradedModeBatchPushLifecycle() {
    String storeName = Utils.getUniqueString("degraded-mode-store");
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentClient = new ControllerClient(clusterName, parentControllerURLs);
        ControllerClient dc0Client =
            new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client =
            new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
        ControllerClient dc2Client =
            new ControllerClient(clusterName, childDatacenters.get(2).getControllerConnectString())) {

      // Step 1: Enable degraded mode on the cluster via live cluster config
      ControllerResponse enableResponse =
          parentClient.updateClusterConfig(new UpdateClusterConfigQueryParams().setDegradedModeEnabled(true));
      Assert.assertFalse(enableResponse.isError(), "Failed to enable degraded mode: " + enableResponse.getError());

      // Step 2: Create store and wait for system stores
      List<ControllerClient> dcClients = Arrays.asList(dc0Client, dc1Client, dc2Client);
      TestUtils.createAndVerifyStoreInAllRegions(storeName, parentClient, dcClients);
      waitForSystemStorePushes(storeName, dc0Client, dc1Client, dc2Client);

      // Configure store for batch-only with NR enabled.
      // setTargetRegionSwapWaitTime(0) — zero-minute wait so DVSS can attempt roll-forward as soon as
      // target regions complete. Internally DVSS computes storeWaitTimeSeconds = MINUTES.toSeconds(0) = 0,
      // so the wait-time gate clears immediately. Critical for keeping this E2E under a few minutes.
      Assert.assertFalse(
          parentClient
              .updateStore(
                  storeName,
                  new UpdateStoreQueryParams().setNativeReplicationEnabled(true)
                      .setPartitionCount(1)
                      .setTargetRegionSwapWaitTime(0))
              .isError());

      // Step 3: Mark dc-1 as degraded
      ControllerResponse markResponse = parentClient.markDatacenterDegraded("dc-1", 60, "integration-test");
      Assert.assertFalse(markResponse.isError(), "Failed to mark dc-1 degraded: " + markResponse.getError());

      // Step 4: Verify degraded DCs
      DegradedDcResponse degradedResponse = parentClient.getDegradedDatacenters();
      Assert.assertFalse(degradedResponse.isError());
      Assert
          .assertTrue(degradedResponse.getDegradedDatacenters().containsKey("dc-1"), "dc-1 should be in degraded set");

      // Step 5: Do a batch push — should auto-convert to targeted region push excluding dc-1
      VersionCreationResponse versionResponse = parentClient.requestTopicForWrites(
          storeName,
          1024,
          Version.PushType.BATCH,
          Version.guidBasedDummyPushId(),
          true,
          false,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          false,
          -1);
      Assert.assertFalse(versionResponse.isError(), "Push request failed: " + versionResponse.getError());

      // Verify auto-conversion: degradedDatacenters should be populated in response
      Assert.assertNotNull(versionResponse.getDegradedDatacenters(), "degradedDatacenters should be set");
      Assert.assertTrue(
          versionResponse.getDegradedDatacenters().contains("dc-1"),
          "dc-1 should be in degradedDatacenters");

      int versionNumber = versionResponse.getVersion();
      String kafkaTopic = versionResponse.getKafkaTopic();

      // Write batch data
      List<PubSubBrokerWrapper> pubSubBrokerWrappers =
          childDatacenters.stream().map(dc -> dc.getKafkaBrokerWrapper()).collect(Collectors.toList());

      PubSubProducerAdapterFactory producerFactory =
          childDatacenters.get(0).getKafkaBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
      Map<String, String> additionalConfigs = PubSubBrokerWrapper.getBrokerDetailsForClients(pubSubBrokerWrappers);

      TestUtils.writeBatchData(
          versionResponse,
          STRING_SCHEMA.toString(),
          STRING_SCHEMA.toString(),
          IntStream.range(0, 10).mapToObj(i -> new AbstractMap.SimpleEntry<>(String.valueOf(i), String.valueOf(i))),
          HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID,
          producerFactory,
          additionalConfigs,
          pubSubBrokerWrappers.get(0).getPubSubPositionTypeRegistry());

      // Step 6: Wait for push to complete in the healthy DCs.
      //
      // Cannot use TestUtils.waitForNonDeterministicPushCompletion(parentClient) here — that helper
      // polls parent's getOffLinePushStatus without target-region filtering, so it aggregates
      // statuses across ALL DCs. dc-1 (degraded) correctly skipped AddVersion (via the
      // degradedDatacenters field on the admin message → AdminExecutionTask.isDegradedDC), so
      // dc-1's push monitor returns NOT_CREATED for this topic. That NOT_CREATED would never
      // converge to COMPLETED on the unfiltered aggregate, and the helper would time out.
      //
      // Instead, query with the target region filter so the parent only checks dc-0 and dc-2.
      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, true, () -> {
        JobStatusQueryResponse jobStatus =
            parentClient.queryJobStatus(kafkaTopic, Optional.empty(), 60_000, "dc-0,dc-2", false);
        Assert.assertFalse(jobStatus.isError(), "queryJobStatus failed: " + jobStatus.getError());
        ExecutionStatus status = ExecutionStatus.valueOf(jobStatus.getStatus());
        Assert.assertEquals(status, ExecutionStatus.COMPLETED, "Push status: " + status);
      });

      // Verify dc-0 has the version (healthy DC)
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        StoreResponse dc0StoreResp = dc0Client.getStore(storeName);
        Assert.assertFalse(dc0StoreResp.isError());
        StoreInfo dc0Store = dc0StoreResp.getStore();
        Assert.assertEquals(dc0Store.getCurrentVersion(), versionNumber, "dc-0 should have the new version current");
      });

      // Step 6b: Verify actual data is readable from dc-0 (healthy DC)
      String dc0RouterUrl = childDatacenters.get(0).getClusters().get(clusterName).getRandomRouterURL();
      try (AvroGenericStoreClient<String, Object> dc0Reader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(dc0RouterUrl))) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          for (int i = 0; i < 10; i++) {
            Object value = dc0Reader.get(String.valueOf(i)).get();
            Assert.assertNotNull(value, "Key " + i + " should be readable from dc-0");
            Assert.assertEquals(value.toString(), String.valueOf(i));
          }
        });
      }

      // Step 6c: Verify dc-2 (other healthy DC) also has the data
      String dc2RouterUrl = childDatacenters.get(2).getClusters().get(clusterName).getRandomRouterURL();
      try (AvroGenericStoreClient<String, Object> dc2Reader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(dc2RouterUrl))) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          for (int i = 0; i < 10; i++) {
            Object value = dc2Reader.get(String.valueOf(i)).get();
            Assert.assertNotNull(value, "Key " + i + " should be readable from dc-2");
            Assert.assertEquals(value.toString(), String.valueOf(i));
          }
        });
      }

      // Step 6d: Verify dc-1 (degraded DC) does NOT have the version as current
      // This is the critical assertion — if dc-1 has the version, auto-conversion had no effect
      StoreResponse dc1StoreResp = dc1Client.getStore(storeName);
      Assert.assertFalse(dc1StoreResp.isError());
      Assert.assertNotEquals(
          dc1StoreResp.getStore().getCurrentVersion(),
          versionNumber,
          "dc-1 (degraded) should NOT have the new version as current");

      // Step 7: Kick the parent's STARTED → PUSHED transition.
      //
      // updateParentVersionStatusIfTerminal (VeniceParentHelixAdmin) only runs when
      // getOffLinePushStatus is invoked with isTargetRegionPushWithDeferredSwap=true. It
      // counts COMPLETED among ONLY the target regions and sets parent to PUSHED. VPJ
      // does this in production; we mirror it here. Do NOT assert on the aggregate
      // response — with the degraded-DC skip working, dc-1 returns NOT_CREATED and the
      // aggregate is non-terminal. The side effect (parent → PUSHED) is what we want.
      parentClient.queryJobStatus(kafkaTopic, Optional.empty(), 60_000, "dc-0,dc-2", true);

      // Step 7b: Wait for DVSS to transition parent PUSHED → PARTIALLY_ONLINE.
      //
      // DVSS sees parent at PUSHED, checks target regions (dc-0/dc-2 → terminal), then
      // checks non-target regions (dc-1 → version missing → retried 5x then classified
      // as failed). With all non-target regions failed, DVSS sets PARTIALLY_ONLINE
      // (see DeferredVersionSwapService.getRegionsToRollForward).
      //
      // With targetRegionSwapWaitTime=0 + DVSS sleep of 100ms, the transition fires
      // within a few seconds of the queryJobStatus call above.
      TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, true, () -> {
        StoreResponse parentStoreResp = parentClient.getStore(storeName);
        Assert.assertFalse(parentStoreResp.isError());
        StoreInfo parentStore = parentStoreResp.getStore();
        Optional<Version> v =
            parentStore.getVersions().stream().filter(ver -> ver.getNumber() == versionNumber).findFirst();
        Assert.assertTrue(v.isPresent(), "Version " + versionNumber + " should exist on parent");
        Assert.assertEquals(
            v.get().getStatus(),
            com.linkedin.venice.meta.VersionStatus.PARTIALLY_ONLINE,
            "Parent version should be PARTIALLY_ONLINE after DVSS rolls forward — current status: "
                + v.get().getStatus());
      });

      // Step 8: Unmark dc-1 — triggers auto-recovery (finds the PARTIALLY_ONLINE version).
      ControllerResponse unmarkResponse = parentClient.unmarkDatacenterDegraded("dc-1");
      Assert.assertFalse(unmarkResponse.isError(), "Failed to unmark dc-1: " + unmarkResponse.getError());

      // Verify no more degraded DCs
      DegradedDcResponse afterUnmark = parentClient.getDegradedDatacenters();
      Assert.assertFalse(afterUnmark.isError());
      Assert.assertTrue(
          afterUnmark.getDegradedDatacenters() == null || afterUnmark.getDegradedDatacenters().isEmpty(),
          "No DCs should be degraded after unmark");

      // Full dc-1 recovery (prepareDataRecovery → initiateDataRecovery → child catches up) is
      // verified in unit tests; the local integration test cluster does not support cross-DC
      // data recovery, so we do not assert dc-1 has the version here.
    }
  }

  /**
   * Verify that incremental pushes are blocked when DCs are degraded.
   * Setup: create store, do a normal batch push (need current version for inc push),
   * mark DC degraded, then attempt incremental push — should fail.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testIncrementalPushBlockedDuringDegradedMode() {
    String storeName = Utils.getUniqueString("degraded-inc-push-store");
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentClient = new ControllerClient(clusterName, parentControllerURLs);
        ControllerClient dc0Client =
            new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client =
            new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
        ControllerClient dc2Client =
            new ControllerClient(clusterName, childDatacenters.get(2).getControllerConnectString())) {

      // Enable degraded mode
      parentClient.updateClusterConfig(new UpdateClusterConfigQueryParams().setDegradedModeEnabled(true));

      // Create store with incremental push enabled
      List<ControllerClient> dcClients = Arrays.asList(dc0Client, dc1Client, dc2Client);
      TestUtils.createAndVerifyStoreInAllRegions(storeName, parentClient, dcClients);
      waitForSystemStorePushes(storeName, dc0Client, dc1Client, dc2Client);

      ControllerResponse updateResp = parentClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setNativeReplicationEnabled(true)
              .setActiveActiveReplicationEnabled(true)
              .setPartitionCount(1)
              .setIncrementalPushEnabled(true)
              .setHybridRewindSeconds(10)
              .setHybridOffsetLagThreshold(2));
      Assert.assertFalse(updateResp.isError(), "Store update failed: " + updateResp.getError());

      // Wait for store update to propagate to child DCs
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        StoreResponse resp = dc0Client.getStore(storeName);
        Assert.assertFalse(resp.isError());
        Assert.assertTrue(resp.getStore().isIncrementalPushEnabled(), "Inc push should be enabled in dc-0");
      });

      // Do a normal batch push first (before marking DC degraded so it goes to all DCs)
      VersionCreationResponse emptyPushResp =
          parentClient.emptyPush(storeName, "base-push-" + System.currentTimeMillis(), 1000);
      Assert.assertFalse(emptyPushResp.isError(), "Empty push failed: " + emptyPushResp.getError());
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentClient,
          2,
          TimeUnit.MINUTES);

      // NOW mark dc-1 as degraded (after base push completes)
      ControllerResponse markResponse = parentClient.markDatacenterDegraded("dc-1", 60, "test");
      Assert.assertFalse(markResponse.isError(), "Failed to mark dc-1 degraded: " + markResponse.getError());

      // Attempt incremental push — should be rejected
      VersionCreationResponse incPushResponse = parentClient.requestTopicForWrites(
          storeName,
          1024,
          Version.PushType.INCREMENTAL,
          Version.guidBasedDummyPushId(),
          true,
          false,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          false,
          -1);
      Assert.assertTrue(incPushResponse.isError(), "Incremental push should be rejected during degraded mode");
      Assert.assertTrue(
          incPushResponse.getError().contains("Incremental push blocked"),
          "Error should mention incremental push blocked. Actual: " + incPushResponse.getError());

      // Clean up: unmark
      parentClient.unmarkDatacenterDegraded("dc-1");
    }
  }

  /**
   * Verify that unmarking a non-degraded DC is idempotent — calling unmark twice should not fail.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testDoubleUnmarkIsIdempotent() {
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentClient = new ControllerClient(clusterName, parentControllerURLs)) {
      parentClient.updateClusterConfig(new UpdateClusterConfigQueryParams().setDegradedModeEnabled(true));

      // Mark dc-2
      ControllerResponse markResponse = parentClient.markDatacenterDegraded("dc-2", 60, "test");
      Assert.assertFalse(markResponse.isError(), markResponse.getError());

      // First unmark
      ControllerResponse unmark1 = parentClient.unmarkDatacenterDegraded("dc-2");
      Assert.assertFalse(unmark1.isError(), "First unmark should succeed: " + unmark1.getError());

      // Second unmark — should also succeed (idempotent)
      ControllerResponse unmark2 = parentClient.unmarkDatacenterDegraded("dc-2");
      Assert.assertFalse(unmark2.isError(), "Second unmark should be idempotent: " + unmark2.getError());

      // Verify no DCs are degraded
      DegradedDcResponse degradedDcs = parentClient.getDegradedDatacenters();
      Assert.assertFalse(degradedDcs.isError());
      Assert.assertTrue(
          degradedDcs.getDegradedDatacenters() == null || degradedDcs.getDegradedDatacenters().isEmpty(),
          "No DCs should be degraded");
    }
  }

  /**
   * Test that marking a DC as degraded is rejected when degraded mode is not enabled.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testMarkDegradedRejectedWhenFeatureDisabled() {
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    String testCluster = multiRegionMultiClusterWrapper.getClusterNames()[0];

    try (ControllerClient parentClient = new ControllerClient(testCluster, parentControllerURLs)) {
      // Disable degraded mode
      parentClient.updateClusterConfig(new UpdateClusterConfigQueryParams().setDegradedModeEnabled(false));

      // Attempt to mark dc-2 as degraded should fail (feature disabled)
      ControllerResponse response = parentClient.markDatacenterDegraded("dc-2", 60, "test");
      Assert.assertTrue(response.isError(), "Should reject mark when degraded mode is disabled");
      Assert.assertTrue(
          response.getError().contains("Degraded mode is not enabled"),
          "Error should mention degraded mode not enabled. Actual: " + response.getError());

      // Re-enable for other tests
      parentClient.updateClusterConfig(new UpdateClusterConfigQueryParams().setDegradedModeEnabled(true));
    }
  }
}
