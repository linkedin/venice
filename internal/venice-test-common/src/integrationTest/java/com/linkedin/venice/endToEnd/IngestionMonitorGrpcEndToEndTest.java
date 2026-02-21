package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.GRPC_SERVER_WORKER_THREAD_COUNT;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_INBOUND_ENABLED;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;

import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.grpc.GrpcUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.protocols.IngestionMonitorRequest;
import com.linkedin.venice.protocols.IngestionMonitorResponse;
import com.linkedin.venice.protocols.VeniceIngestionMonitorServiceGrpc;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * End-to-end integration test for the gRPC ingestion monitor service.
 *
 * <p>Sets up a hybrid store, runs an empty push to create a version, then hooks up
 * gRPC monitoring and streams 100 real-time records. Validates that the monitor reports a positive
 * record consumption rate.
 *
 * <p>Note: Active-Active replication requires a multi-region setup to function correctly (leader
 * transition depends on NR source fabric). This test uses a single-region hybrid store which still
 * exercises the core ingestion monitoring path (consumed records, E2E latency, storage put latency,
 * leader produce latency, etc.).
 */
public class IngestionMonitorGrpcEndToEndTest {
  private static final Logger LOGGER = LogManager.getLogger(IngestionMonitorGrpcEndToEndTest.class);
  private static final int STREAMING_RECORD_COUNT = 100;
  private static final long PUSH_TIMEOUT_MS = 60_000;

  private VeniceClusterWrapper cluster;
  private SSLFactory sslFactory;
  private String storeName;
  private int storeVersion;

  @BeforeClass
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();
    sslFactory = SslUtils.getVeniceLocalSslFactory();

    Properties props = new Properties();
    props.put(SERVER_HTTP2_INBOUND_ENABLED, "true");
    props.put(GRPC_SERVER_WORKER_THREAD_COUNT, "2");

    cluster = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
            .numberOfPartitions(1)
            .numberOfRouters(1)
            .numberOfServers(1)
            .replicationFactor(1)
            .sslToStorageNodes(true)
            .enableGrpc(true)
            .extraProperties(props)
            .build());

    storeName = Utils.getUniqueString("ingestionMonitorHybridTest");

    // Create the store
    cluster.getNewStore(storeName);

    // Configure as hybrid
    ControllerResponse updateResponse = cluster.updateStore(
        storeName,
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setHybridRewindSeconds(25L)
            .setHybridOffsetLagThreshold(1L));
    Assert.assertFalse(updateResponse.isError(), "Failed to update store: " + updateResponse.getError());

    // Empty push to create the first version
    cluster.useControllerClient(controllerClient -> {
      ControllerResponse pushResponse =
          controllerClient.sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, PUSH_TIMEOUT_MS);
      Assert.assertFalse(pushResponse.isError(), "Empty push failed: " + pushResponse.getError());
    });

    // Wait for version to become current
    cluster.useControllerClient(controllerClient -> {
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = controllerClient.getStore(storeName);
        Assert.assertNotNull(storeResponse.getStore(), "Store not found");
        Assert.assertTrue(storeResponse.getStore().getCurrentVersion() > 0, "No current version yet");
        storeVersion = storeResponse.getStore().getCurrentVersion();
      });
    });

    LOGGER.info("Setup complete. Store: {}, version: {}", storeName, storeVersion);
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  /**
   * Core test: hook up the gRPC ingestion monitor, then stream 100 records and verify
   * the monitor reports a positive consumed-records rate.
   */
  @Test(timeOut = 120_000)
  public void testMonitorSeesPositiveRateDuringRealtimeIngestion() throws Exception {
    VeniceServerWrapper serverWrapper = cluster.getVeniceServers().get(0);
    int grpcPort = serverWrapper.getGrpcPort();
    String host = serverWrapper.getHost();
    String versionTopic = Version.composeKafkaTopic(storeName, storeVersion);

    LOGGER.info("Connecting to gRPC ingestion monitor at {}:{} for {}", host, grpcPort, versionTopic);

    ManagedChannel channel = buildGrpcChannel(host, grpcPort);
    try {
      VeniceIngestionMonitorServiceGrpc.VeniceIngestionMonitorServiceBlockingStub stub =
          VeniceIngestionMonitorServiceGrpc.newBlockingStub(channel).withDeadlineAfter(60, TimeUnit.SECONDS);

      // Start monitoring before sending records
      IngestionMonitorRequest request = IngestionMonitorRequest.newBuilder()
          .setVersionTopic(versionTopic)
          .setPartition(0)
          .setIntervalMs(2000)
          .build();

      Iterator<IngestionMonitorResponse> responses = stub.monitorIngestion(request);

      // Consume the first snapshot (should be a baseline with 0 rates since no RT data yet)
      Assert.assertTrue(responses.hasNext(), "Expected at least one streaming response");
      IngestionMonitorResponse baseline = responses.next();
      LOGGER.info(
          "Baseline: state={}, hybrid={}, records/s={}, bytes/s={}",
          baseline.getLeaderFollowerState(),
          baseline.getIsHybrid(),
          baseline.getRecordsPolledPerSec(),
          baseline.getBytesPolledPerSec());

      Assert.assertTrue(baseline.getTimestampMs() > 0, "Timestamp should be positive");
      Assert.assertTrue(baseline.getIsHybrid(), "Store should be hybrid");

      // Now stream 100 records via Samza producer (writes to the RT topic)
      SystemProducer veniceProducer =
          IntegrationTestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.STREAM);
      try {
        for (int i = 0; i < STREAMING_RECORD_COUNT; i++) {
          sendStreamingRecord(veniceProducer, storeName, "key_" + i, "value_" + i);
        }
        veniceProducer.flush(storeName);
        LOGGER.info("Sent {} streaming records to RT topic", STREAMING_RECORD_COUNT);
      } finally {
        veniceProducer.stop();
      }

      // Collect subsequent snapshots until we see a positive consumed-records rate.
      // The monitor emits every 2 seconds; give it several intervals.
      boolean sawPositiveRate = false;
      for (int tick = 0; tick < 15 && responses.hasNext(); tick++) {
        IngestionMonitorResponse snapshot = responses.next();
        LOGGER.info(
            "Tick {}: records/s={}, bytes/s={}, e2e_ms={}, put_ms={}, idle_ms={}",
            tick,
            snapshot.getRecordsPolledPerSec(),
            snapshot.getBytesPolledPerSec(),
            snapshot.getConsumedRecordE2EProcessingLatencyAvgMs(),
            snapshot.getStorageEnginePutLatencyAvgMs(),
            snapshot.getElapsedTimeSinceLastRecordMs());

        if (snapshot.getRecordsPolledPerSec() > 0) {
          sawPositiveRate = true;
          LOGGER.info(
              "Observed positive consumed-records rate: {}/s at tick {}",
              snapshot.getRecordsPolledPerSec(),
              tick);
          // Also validate bytes rate is positive when record rate is
          Assert.assertTrue(
              snapshot.getBytesPolledPerSec() > 0,
              "Bytes rate should be positive when record rate is positive");
          break;
        }
      }

      Assert.assertTrue(sawPositiveRate, "Monitor never reported a positive consumed-records rate within the window");
    } finally {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test(timeOut = 30_000)
  public void testMonitorIngestionNonExistentTopic() throws Exception {
    VeniceServerWrapper serverWrapper = cluster.getVeniceServers().get(0);
    int grpcPort = serverWrapper.getGrpcPort();
    String host = serverWrapper.getHost();

    ManagedChannel channel = buildGrpcChannel(host, grpcPort);
    try {
      VeniceIngestionMonitorServiceGrpc.VeniceIngestionMonitorServiceBlockingStub stub =
          VeniceIngestionMonitorServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);

      IngestionMonitorRequest request = IngestionMonitorRequest.newBuilder()
          .setVersionTopic("nonExistentStore_v999")
          .setPartition(0)
          .setIntervalMs(1000)
          .build();

      Iterator<IngestionMonitorResponse> responses = stub.monitorIngestion(request);
      try {
        responses.next();
        Assert.fail("Expected StatusRuntimeException for non-existent topic");
      } catch (StatusRuntimeException e) {
        Assert.assertEquals(
            e.getStatus().getCode(),
            io.grpc.Status.Code.NOT_FOUND,
            "Expected NOT_FOUND status for non-existent topic");
        LOGGER.info("Correctly received NOT_FOUND for non-existent topic: {}", e.getStatus().getDescription());
      }
    } finally {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test(timeOut = 30_000)
  public void testMonitorIngestionNonExistentPartition() throws Exception {
    VeniceServerWrapper serverWrapper = cluster.getVeniceServers().get(0);
    int grpcPort = serverWrapper.getGrpcPort();
    String host = serverWrapper.getHost();
    String versionTopic = Version.composeKafkaTopic(storeName, storeVersion);

    ManagedChannel channel = buildGrpcChannel(host, grpcPort);
    try {
      VeniceIngestionMonitorServiceGrpc.VeniceIngestionMonitorServiceBlockingStub stub =
          VeniceIngestionMonitorServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);

      IngestionMonitorRequest request = IngestionMonitorRequest.newBuilder()
          .setVersionTopic(versionTopic)
          .setPartition(9999)
          .setIntervalMs(1000)
          .build();

      Iterator<IngestionMonitorResponse> responses = stub.monitorIngestion(request);
      try {
        responses.next();
        Assert.fail("Expected StatusRuntimeException for non-existent partition");
      } catch (StatusRuntimeException e) {
        Assert.assertEquals(
            e.getStatus().getCode(),
            io.grpc.Status.Code.NOT_FOUND,
            "Expected NOT_FOUND status for non-existent partition");
        LOGGER.info("Correctly received NOT_FOUND for non-existent partition: {}", e.getStatus().getDescription());
      }
    } finally {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private ManagedChannel buildGrpcChannel(String host, int grpcPort) {
    ChannelCredentials credentials = GrpcUtils.buildChannelCredentials(sslFactory);
    return Grpc.newChannelBuilder(host + ":" + grpcPort, credentials).build();
  }
}
