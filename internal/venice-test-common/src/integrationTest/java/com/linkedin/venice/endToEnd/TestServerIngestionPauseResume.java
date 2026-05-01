package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducerForStream;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.IngestionPauseMode;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class TestServerIngestionPauseResume extends AbstractMultiRegionTest {
  private static final long HYBRID_REWIND_SECONDS = 10L;
  private static final long HYBRID_OFFSET_LAG_THRESHOLD = 2L;

  @Test(timeOut = 300 * Time.MS_PER_SECOND)
  public void testRealTimeConsumptionPausesAndResumesOnServer() throws Exception {
    String storeName = Utils.getUniqueString("test_server_ingestion_pause");

    try (ControllerClient parentClient = new ControllerClient(CLUSTER_NAME, parentController.getControllerUrl())) {
      assertCommand(parentClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
      assertCommand(
          parentClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
                  .setHybridRewindSeconds(HYBRID_REWIND_SECONDS)
                  .setHybridOffsetLagThreshold(HYBRID_OFFSET_LAG_THRESHOLD)));

      // Initial batch push (version 1) and wait for it to complete.
      assertCommand(parentClient.emptyPush(storeName, "push-1", 1000));
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentClient,
          60,
          TimeUnit.SECONDS);
    }

    VeniceClusterWrapper dc0 = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    VeniceSystemProducer producer = getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName);
    try (AvroGenericStoreClient<String, Object> dc0Client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.<String, Object>defaultGenericClientConfig(storeName).setVeniceURL(dc0.getRandomRouterURL()))) {

      // Send key1 and confirm dc0 reads it back — baseline that hybrid ingestion is working.
      sendStreamingRecord(producer, storeName, "key1", "value1");
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Object v = dc0Client.get("key1").get();
        assertNotNull(v, "key1 should be readable before pause");
        assertEquals(v.toString(), "value1");
      });

      // Pause ingestion globally (all regions, all versions).
      try (ControllerClient parentClient = new ControllerClient(CLUSTER_NAME, parentController.getControllerUrl())) {
        assertCommand(
            parentClient.updateStore(
                storeName,
                new UpdateStoreQueryParams().setIngestionPauseMode(IngestionPauseMode.ALL_VERSIONS)));
      }

      // Wait for the pause mode to propagate parent -> child controller -> server's
      // storeRepository before producing — otherwise a fast write could land before the SIT
      // observes the new mode and would be ingested under the old (NOT_PAUSED) state.
      try (ControllerClient dc0Controller =
          new ControllerClient(CLUSTER_NAME, childDatacenters.get(0).getControllerConnectString())) {
        TestUtils.waitForNonDeterministicAssertion(
            30,
            TimeUnit.SECONDS,
            () -> assertEquals(
                assertCommand(dc0Controller.getStore(storeName)).getStore().getIngestionPauseMode(),
                IngestionPauseMode.ALL_VERSIONS));
      }

      // Send key2 while paused. It will land in the RT topic but dc0's SIT should not consume it.
      sendStreamingRecord(producer, storeName, "key2", "value2");

      // Assert key2 stays invisible in dc0 for a meaningful window. We wait longer than the hybrid
      // offset lag threshold / rewind window so a passing consumer would have made it visible.
      Thread.sleep(15_000);
      assertNull(dc0Client.get("key2").get(), "key2 should NOT be readable while ingestion is paused in dc0");

      // Resume ingestion.
      try (ControllerClient parentClient = new ControllerClient(CLUSTER_NAME, parentController.getControllerUrl())) {
        assertCommand(
            parentClient.updateStore(
                storeName,
                new UpdateStoreQueryParams().setIngestionPauseMode(IngestionPauseMode.NOT_PAUSED)));
      }

      // After resume, dc0 should eventually catch up and expose key2.
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        Object v = dc0Client.get("key2").get();
        assertNotNull(v, "key2 should be readable after resume");
        assertEquals(v.toString(), "value2");
      });

      // key1 should still be readable (sanity: resume didn't blow away prior state).
      assertEquals(dc0Client.get("key1").get().toString(), "value1");
    } finally {
      producer.stop();
    }
  }

  /**
   * Verifies that the {@code ingestionPausedRegions} filter isolates the pause effect to the
   * named region(s): when only dc1 is listed, dc0 continues to ingest as normal.
   *
   * <p>Uses a native-replication + active-active hybrid store so dc1 leaders pull dc0's RT —
   * which is what makes the post-resume catch-up assertion meaningful (a paused dc1 would
   * otherwise have nothing to catch up on).
   */
  @Test(timeOut = 300 * Time.MS_PER_SECOND)
  public void testRegionScopedPauseOnlyAffectsTargetedRegion() throws Exception {
    String storeName = Utils.getUniqueString("test_region_scoped_pause");
    String dc1Name = multiRegionMultiClusterWrapper.getChildRegionNames().get(1);

    try (ControllerClient parentClient = new ControllerClient(CLUSTER_NAME, parentController.getControllerUrl())) {
      assertCommand(parentClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
      assertCommand(
          parentClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
                  .setHybridRewindSeconds(HYBRID_REWIND_SECONDS)
                  .setHybridOffsetLagThreshold(HYBRID_OFFSET_LAG_THRESHOLD)
                  .setNativeReplicationEnabled(true)
                  .setActiveActiveReplicationEnabled(true)));
      assertCommand(parentClient.emptyPush(storeName, "push-1", 1000));
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentClient,
          60,
          TimeUnit.SECONDS);
    }

    VeniceClusterWrapper dc0 = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    VeniceClusterWrapper dc1 = childDatacenters.get(1).getClusters().get(CLUSTER_NAME);
    VeniceSystemProducer producer = getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName);
    try (
        AvroGenericStoreClient<String, Object> dc0Client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.<String, Object>defaultGenericClientConfig(storeName).setVeniceURL(dc0.getRandomRouterURL()));
        AvroGenericStoreClient<String, Object> dc1Client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.<String, Object>defaultGenericClientConfig(storeName)
                .setVeniceURL(dc1.getRandomRouterURL()))) {

      // Pause ONLY dc1 via the region filter. dc0 should remain unaffected.
      try (ControllerClient parentClient = new ControllerClient(CLUSTER_NAME, parentController.getControllerUrl())) {
        assertCommand(
            parentClient.updateStore(
                storeName,
                new UpdateStoreQueryParams().setIngestionPauseMode(IngestionPauseMode.ALL_VERSIONS)
                    .setIngestionPausedRegions(java.util.Arrays.asList(dc1Name))));
      }

      // Wait for child controllers to apply the filtered mode: dc0 stays NOT_PAUSED, dc1 sees
      // ALL_VERSIONS. This confirms the region filter in AdminExecutionTask runs end-to-end.
      try (
          ControllerClient dc0Controller =
              new ControllerClient(CLUSTER_NAME, childDatacenters.get(0).getControllerConnectString());
          ControllerClient dc1Controller =
              new ControllerClient(CLUSTER_NAME, childDatacenters.get(1).getControllerConnectString())) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          assertEquals(
              assertCommand(dc0Controller.getStore(storeName)).getStore().getIngestionPauseMode(),
              IngestionPauseMode.NOT_PAUSED,
              "dc0 should stay NOT_PAUSED because it was excluded from the region filter");
          assertEquals(
              assertCommand(dc1Controller.getStore(storeName)).getStore().getIngestionPauseMode(),
              IngestionPauseMode.ALL_VERSIONS,
              "dc1 should see ALL_VERSIONS because it was named in the region filter");
        });
      }

      // dc0 should continue ingesting normally even while dc1 is paused.
      sendStreamingRecord(producer, storeName, "key1", "value1");
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Object v = dc0Client.get("key1").get();
        assertNotNull(v, "key1 should be readable in dc0 while only dc1 is paused");
        assertEquals(v.toString(), "value1");
      });

      // dc1 should NOT see key1 while its ingestion is paused. Wait longer than the hybrid
      // offset-lag threshold so a passing consumer would have already made it visible.
      Thread.sleep(15_000);
      assertNull(dc1Client.get("key1").get(), "key1 should NOT be readable in dc1 while its ingestion is paused");

      // Resume.
      try (ControllerClient parentClient = new ControllerClient(CLUSTER_NAME, parentController.getControllerUrl())) {
        assertCommand(
            parentClient.updateStore(
                storeName,
                new UpdateStoreQueryParams().setIngestionPauseMode(IngestionPauseMode.NOT_PAUSED)));
      }

      // After resume, dc1 should eventually catch up and expose key1.
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        Object v = dc1Client.get("key1").get();
        assertNotNull(v, "key1 should be readable in dc1 after resume");
        assertEquals(v.toString(), "value1");
      });
    } finally {
      producer.stop();
    }
  }
}
