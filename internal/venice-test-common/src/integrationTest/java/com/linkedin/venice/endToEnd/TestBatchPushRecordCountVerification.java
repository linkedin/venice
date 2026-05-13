package com.linkedin.venice.endToEnd;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

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
import com.linkedin.venice.utils.IntegrationTestPushUtils;
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
 * Integration test for server-side batch-push record count verification end-to-end.
 *
 * <p>Producer (VPJ / Spark) and consumer (server) count records the same way by construction, so
 * a "natural" mismatch never occurs in normal pushes. To exercise the server's deficit-detection
 * path, this test bypasses VPJ and uses {@link VeniceWriter} directly to:</p>
 * <ol>
 *   <li>Push a known number of records.</li>
 *   <li>Broadcast EOP with an inflated {@code prc} (per-partition record count) header — i.e. claim
 *       the producer wrote more records than were actually sent.</li>
 * </ol>
 *
 * <p>The server runs with the cluster default {@code
 * server.batch.push.record.count.verification.fail.on.mismatch.enabled = true}, so it must throw
 * {@link com.linkedin.venice.exceptions.VeniceException} on the EOP control message, the replica
 * goes to ERROR, push status transitions to {@link ExecutionStatus#ERROR}, and the version never
 * becomes current. This is the regression that would have caught the March 2026 incident-10468
 * shape (VPJ wrote N, server got 0).</p>
 *
 * <p>Metric-only mode (server flag {@code false}) and per-leg / DaVinci-skip / not-future-version
 * behavior are covered by unit tests in {@code StoreIngestionTaskRecordCountTest} and the OTel
 * stats tests, not here.</p>
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
   * Strict mode (server default): the server throws {@code VeniceException} when the locally-
   * counted record count is less than the producer-side {@code prc} count. The exception
   * propagates {@code processEndOfPush} → SIT consumer loop → {@code reportError}. The replica
   * is marked ERROR, push status transitions to {@link ExecutionStatus#ERROR}, and the version
   * never becomes current. This is the path that would fail a real VPJ push job in incident-10468
   * shape. Metric-only mode (server flag {@code false}) and per-leg / DaVinci-skip behavior are
   * covered by {@code StoreIngestionTaskRecordCountTest} at the unit level.
   */
  @Test(timeOut = 120_000)
  public void testDeficitFailsPushWhenServerStrictModeDefault() throws Exception {
    String storeName = Utils.getUniqueString("test_prc_mismatch_throw");
    veniceCluster.getNewStore(storeName);
    veniceCluster.useControllerClient(client -> {
      TestUtils.assertCommand(
          client.updateStore(storeName, new UpdateStoreQueryParams().setPartitionCount(PARTITION_COUNT)));
    });

    String storeVersionName;
    try (VeniceWriter<String, String, byte[]> writer = createWriterAndPushDeficit(storeName)) {
      storeVersionName = writer.getTopicName();
    }
    final int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);

    // Behavioral assertion: push status must transition to ERROR and version must never become current.
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
      ExecutionStatus status = ExecutionStatus.valueOf(controllerClient.queryJobStatus(storeVersionName).getStatus());
      assertEquals(
          status,
          ExecutionStatus.ERROR,
          "Push status should be ERROR — server should throw on prc deficit in default strict mode, " + "but observed: "
              + status);
      int currentVersion = controllerClient.getStore(storeName).getStore().getCurrentVersion();
      assertNotEquals(
          currentVersion,
          pushVersion,
          "Version " + pushVersion + " must NOT become current after a record-count deficit failure");
    });

    // Metric assertion: mismatch OTel counter fires; match counter stays at 0 (every partition's
    // EOP claimed an inflated count, so no match path executed).
    IntegrationTestPushUtils.assertBatchPushRecordCountSensors(
        veniceCluster.getVeniceServers(),
        storeName,
        /* expectMatch */ false,
        /* expectMismatch */ true);
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
