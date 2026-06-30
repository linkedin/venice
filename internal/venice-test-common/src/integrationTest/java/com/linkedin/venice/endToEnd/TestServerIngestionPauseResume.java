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
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.IngestionPauseMode;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
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
      // Round-13 bump: 120s also hit the wall in CI run 25767164291 / shard 49 with
      // dc-1=NOT_CREATED at 134s -- cross-fabric version-topic creation on a cold
      // multi-region cluster needs longer headroom. Going to 180s; outer @Test cap is 300s.
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentClient,
          180,
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
      // Wait for the server-side SIT to actually apply the pause (set the gauge). This signals
      // that maybeTransitionPauseState has unsubscribed the consumer, so any record produced
      // after this point cannot leak through. More robust than a fixed sleep.
      waitForStoreLevelPausedGauge(dc0, storeName, 1.0);

      // Send key2 while paused. It will land in the RT topic but dc0's SIT should not consume it.
      sendStreamingRecord(producer, storeName, "key2", "value2");

      // Assert key2 stays invisible in dc0 for the entire 15s window. Polls and fails fast if the
      // key ever appears (rather than sleeping and checking once, which can miss a brief
      // ingestion window or pass on slow propagation).
      assertKeyRemainsAbsent(dc0Client, "key2", 15_000, "key2 should NOT be readable while ingestion is paused in dc0");

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
      // Round-13 bump: 120s also hit the wall in CI run 25767164291 / shard 49 with
      // dc-1=NOT_CREATED at 134s -- cross-fabric version-topic creation on a cold
      // multi-region cluster needs longer headroom. Going to 180s; outer @Test cap is 300s.
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentClient,
          180,
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
      // Wait for dc1's SIT to actually apply the pause (gauge=1) before producing — direct
      // signal that the consumer has been unsubscribed.
      waitForStoreLevelPausedGauge(dc1, storeName, 1.0);

      // dc0 should continue ingesting normally even while dc1 is paused.
      sendStreamingRecord(producer, storeName, "key1", "value1");
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Object v = dc0Client.get("key1").get();
        assertNotNull(v, "key1 should be readable in dc0 while only dc1 is paused");
        assertEquals(v.toString(), "value1");
      });

      // dc1 should NOT see key1 for the entire 15s window. Polls and fails fast if the key ever
      // appears (rather than sleeping and checking once, which can pass on slow propagation).
      assertKeyRemainsAbsent(
          dc1Client,
          "key1",
          15_000,
          "key1 should NOT be readable in dc1 while its ingestion is paused");

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

  /**
   * Polls every server in the cluster until ALL servers hosting the store report
   * {@code store_level_paused_gauge == expected} for the current version. Direct signal that
   * {@code maybeTransitionPauseState} has run and applied the requested transition on every
   * replica — replaces fixed-duration buffer waits between updateStore and the next test
   * action.
   *
   * <p>Previously waited for at-least-one server, which let the assertion proceed while a
   * sibling replica was still ingesting. That replica could then serve the post-pause read of
   * key2 via Helix routing — observed as "key2 should NOT be readable ... (observed: value2)".
   * Requiring all-replicas-paused closes that window.
   */
  private void waitForStoreLevelPausedGauge(VeniceClusterWrapper cluster, String storeName, double expected) {
    String metricName = "." + storeName + "_current--store_level_paused_gauge.IngestionStatsGauge";
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      int hostingServers = 0;
      int convergedServers = 0;
      double minObserved = Double.POSITIVE_INFINITY;
      for (VeniceServerWrapper server: cluster.getVeniceServers()) {
        MetricsRepository repo = server.getMetricsRepository();
        Metric metric = repo.getMetric(metricName);
        if (metric == null) {
          continue; // server does not host this store's current version
        }
        hostingServers++;
        minObserved = Math.min(minObserved, metric.value());
        if (metric.value() == expected) {
          convergedServers++;
        }
      }
      if (hostingServers == 0 || convergedServers != hostingServers) {
        throw new AssertionError(
            "Expected " + metricName + " == " + expected + " on ALL hosting servers; converged " + convergedServers
                + "/" + hostingServers + " servers; min observed: " + minObserved);
      }
    });
  }

  /**
   * Polls the store client over a window, failing fast if the key ever becomes visible. Replaces
   * the {@code Thread.sleep + assertNull} pattern, which can pass on slow propagation (the key
   * arrives just after the single check) or fail on a brief ingestion window. Here we check
   * every 250ms for the entire window, so a single positive read fails the assertion.
   */
  private void assertKeyRemainsAbsent(
      AvroGenericStoreClient<String, Object> client,
      String key,
      long windowMs,
      String message) throws Exception {
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(windowMs);
    while (System.nanoTime() < deadlineNanos) {
      Object value = client.get(key).get();
      assertNull(value, message + " (observed: " + value + ")");
      Thread.sleep(250);
    }
  }
}
