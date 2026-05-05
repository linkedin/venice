package com.linkedin.venice.endToEnd;

import static org.testng.Assert.*;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.tehuti.MetricsUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration tests for server-side batch-push record count verification.
 *
 * <p>Producer (VPJ / Spark) and consumer (server) count records the same way by construction, so
 * a "natural" mismatch never occurs in normal pushes. To exercise the server's deficit-detection
 * path, these tests bypass VPJ and use {@link VeniceWriter} directly to:</p>
 * <ol>
 *   <li>Push a known number of records.</li>
 *   <li>Broadcast EOP with an inflated {@code prc} (per-partition record count) header — i.e. claim
 *       the producer wrote more records than were actually sent.</li>
 * </ol>
 *
 * <p>Two scenarios are covered:</p>
 * <ul>
 *   <li>Per-store {@code batchPushRecordCountVerificationEnabled} flag <b>disabled</b> (default):
 *       server detects the deficit, emits the {@code batch_push_record_count_mismatch} sensor and a
 *       tagged log line, but does <b>not</b> throw — push completes normally.</li>
 *   <li>Per-store flag <b>enabled</b>: server throws {@code VeniceException} on the EOP control
 *       message, the replica goes to ERROR, push status transitions to {@link ExecutionStatus#ERROR},
 *       and the version never becomes current.</li>
 * </ul>
 *
 * <p>Together, these are the integration-test analogs of the unit tests in
 * {@code StoreIngestionTaskRecordCountTest}. They exercise both the metric-only and throw paths
 * end-to-end across the real producer/Kafka/server boundary — which is the regression that would
 * have caught the March 2026 incident-10468 shape (VPJ wrote N, server got 0).</p>
 */
public class TestBatchPushRecordCountVerification {
  private static final int RECORDS_PRODUCED = 50;
  private static final long INFLATED_PRC = 200L; // > RECORDS_PRODUCED — guaranteed deficit on every partition
  private static final int PARTITION_COUNT = 2;
  private static final int VALUE_SCHEMA_ID = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

  private VeniceClusterWrapper veniceCluster;
  private ControllerClient controllerClient;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws VeniceClientException {
    Utils.thisIsLocalhost();
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(1)
        .numberOfRouters(1)
        .replicationFactor(1)
        .partitionSize(100)
        .sslToStorageNodes(false)
        .sslToKafka(false)
        .build();
    veniceCluster = ServiceFactory.getVeniceCluster(options);
    controllerClient = new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs());
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    IOUtils.closeQuietly(veniceCluster);
  }

  /**
   * Per-store flag <b>disabled</b>: metric-only mode. Server detects the deficit, increments
   * {@code batch_push_record_count_mismatch}, but does NOT throw. Push completes — version becomes
   * current.
   */
  @Test(timeOut = 90_000)
  public void testDeficitEmitsMismatchSensorButPushSucceedsWhenFlagDisabled() throws Exception {
    String storeName = Utils.getUniqueString("test_prc_mismatch_metric_only");
    veniceCluster.getNewStore(storeName);
    veniceCluster.useControllerClient(client -> {
      TestUtils.assertCommand(
          client.updateStore(
              storeName,
              new UpdateStoreQueryParams().setPartitionCount(PARTITION_COUNT)
                  .setBatchPushRecordCountVerificationEnabled(false)));
    });

    String storeVersionName;
    try (VeniceWriter<String, String, byte[]> writer = createWriterAndPushDeficit(storeName)) {
      storeVersionName = writer.getTopicName();
    }
    final int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);

    // Push should still complete — the per-store verification flag is disabled.
    TestUtils.waitForNonDeterministicCompletion(60, TimeUnit.SECONDS, () -> {
      int currentVersion = controllerClient.getStore(storeName).getStore().getCurrentVersion();
      return currentVersion == pushVersion;
    });

    // Mismatch sensor fired; match sensor stayed at 0.
    String mismatchMetric = "." + storeName + "--batch_push_record_count_mismatch.Count";
    String matchMetric = "." + storeName + "--batch_push_record_count_match.Count";
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      double mismatchSum = MetricsUtils.getSum(mismatchMetric, veniceCluster.getVeniceServers());
      double matchSum = MetricsUtils.getSum(matchMetric, veniceCluster.getVeniceServers());
      assertTrue(
          mismatchSum > 0,
          "Expected " + mismatchMetric + " > 0 (server should detect the inflated prc deficit) but got " + mismatchSum);
      assertEquals(
          matchSum,
          0.0,
          matchMetric + " should be 0 — every partition's EOP claimed an inflated count, so no match");
    });

    // No ingestion failure when flag is disabled.
    double ingestionFailureSum =
        MetricsUtils.getSum("." + storeName + "--ingestion_failure.Count", veniceCluster.getVeniceServers());
    assertEquals(
        ingestionFailureSum,
        0.0,
        "ingestion_failure should be 0 when the per-store verification flag is disabled but was "
            + ingestionFailureSum);
  }

  /**
   * Per-store flag <b>enabled</b>: throw mode. The server throws {@code VeniceException} when the
   * locally-counted record count is less than the producer-side {@code prc} count. The exception
   * propagates {@code processEndOfPush} → SIT consumer loop → {@code reportError}. The replica
   * is marked ERROR, push status transitions to {@link ExecutionStatus#ERROR}, and the version
   * never becomes current. This is the path that would fail a real VPJ push job in incident-10468
   * shape.
   */
  @Test(timeOut = 120_000)
  public void testDeficitFailsPushWhenFlagEnabled() throws Exception {
    String storeName = Utils.getUniqueString("test_prc_mismatch_throw");
    veniceCluster.getNewStore(storeName);
    veniceCluster.useControllerClient(client -> {
      TestUtils.assertCommand(
          client.updateStore(
              storeName,
              new UpdateStoreQueryParams().setPartitionCount(PARTITION_COUNT)
                  .setBatchPushRecordCountVerificationEnabled(true)));
    });

    String storeVersionName;
    try (VeniceWriter<String, String, byte[]> writer = createWriterAndPushDeficit(storeName)) {
      storeVersionName = writer.getTopicName();
    }
    final int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);

    // Push status must transition to ERROR; the version must never become current.
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
      ExecutionStatus status = ExecutionStatus.valueOf(controllerClient.queryJobStatus(storeVersionName).getStatus());
      assertEquals(
          status,
          ExecutionStatus.ERROR,
          "Push status should be ERROR — server should throw on prc deficit when per-store flag is enabled, "
              + "but observed: " + status);
      int currentVersion = controllerClient.getStore(storeName).getStore().getCurrentVersion();
      assertNotEquals(
          currentVersion,
          pushVersion,
          "Version " + pushVersion + " must NOT become current after a record-count deficit failure");
    });

    // Mismatch sensor fired; match sensor stayed at 0; failure sensor fired.
    // Wrap in waitForNonDeterministicAssertion: Tehuti recordings are observed via the metrics
    // repository which can lag behind the actual record() call (the throw path runs on the SIT
    // consumer thread, but the metric value is sampled here on the test thread), so reading
    // immediately after the push transitions to ERROR can briefly observe stale 0 values.
    String mismatchMetric = "." + storeName + "--batch_push_record_count_mismatch.Count";
    String matchMetric = "." + storeName + "--batch_push_record_count_match.Count";
    String failureMetric = "." + storeName + "--record_count_mismatch_failure.Count";
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      double mismatchSum = MetricsUtils.getSum(mismatchMetric, veniceCluster.getVeniceServers());
      double matchSum = MetricsUtils.getSum(matchMetric, veniceCluster.getVeniceServers());
      double failureSum = MetricsUtils.getSum(failureMetric, veniceCluster.getVeniceServers());
      assertTrue(
          mismatchSum > 0,
          "Expected " + mismatchMetric + " > 0 (server should detect the inflated prc deficit) but got " + mismatchSum);
      assertEquals(
          matchSum,
          0.0,
          matchMetric + " should be 0 — every partition's EOP claimed an inflated count, so no match");
      assertTrue(
          failureSum > 0,
          "Expected " + failureMetric + " > 0 — flag is enabled so the throw path must fire the failure sensor "
              + "distinctly from the informational mismatch sensor; got " + failureSum);
    });
  }

  /**
   * Creates a new version, opens a {@link VeniceWriter}, pushes {@link #RECORDS_PRODUCED} records,
   * then broadcasts EOP with an inflated prc header on every partition (guaranteed deficit). The
   * caller is responsible for closing the returned writer.
   */
  private VeniceWriter<String, String, byte[]> createWriterAndPushDeficit(String storeName) throws Exception {
    VersionCreationResponse creationResponse = veniceCluster.getNewVersion(storeName);
    VeniceWriter<String, String, byte[]> writer = veniceCluster.getVeniceWriter(creationResponse.getKafkaTopic());

    writer.broadcastStartOfPush(new HashMap<>());
    for (int i = 0; i < RECORDS_PRODUCED; i++) {
      writer.put("key_" + i, "value_" + i, VALUE_SCHEMA_ID).get();
    }
    Map<Integer, Long> badPrc = new HashMap<>();
    for (int p = 0; p < PARTITION_COUNT; p++) {
      badPrc.put(p, INFLATED_PRC);
    }
    writer.broadcastEndOfPush(new HashMap<>(), badPrc);
    return writer;
  }
}
