package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.vpj.VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_FABRIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.davinci.replication.merge.RmdSerDe;
import com.linkedin.davinci.storage.chunking.SingleGetChunkingAdapter;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.jobs.StageMetricsSnapshot;
import com.linkedin.venice.jobs.StageMetricsSnapshot.StageSummary;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeClass;


/**
 * Abstract base class for KIF repush integration tests. Provides shared infrastructure:
 * cluster setup, helper methods for building repush properties, waiting for versions,
 * verifying data, and asserting stage metrics.
 *
 * <p>Concrete subclasses are split across CI shards for parallelism.
 */
public abstract class AbstractTestRepush extends AbstractMultiRegionTest {
  protected static final Logger LOGGER = LogManager.getLogger(AbstractTestRepush.class);
  protected static final int TEST_TIMEOUT_MS = 180_000;
  protected static final int ASSERTION_TIMEOUT_MS = 60_000;

  protected String[] dcNames;

  @Override
  protected boolean shouldCreateD2Client() {
    return true;
  }

  @Override
  @BeforeClass(alwaysRun = true)
  public void setUp() {
    super.setUp();
    dcNames = multiRegionMultiClusterWrapper.getChildRegionNames().toArray(new String[0]);
  }

  // ==================================================================================
  // Shared helpers
  // ==================================================================================

  /**
   * Wait until every child region's controller observes Active/Active replication enabled on
   * the given store. createStoreForJob issues updateStore against the parent controller; the
   * admin-channel propagation to each child controller is async. The first @Test method in
   * each shard hits this race because the parent's setActiveActiveReplicationEnabled(true)
   * has not yet reached dc-1 when the immediately-following push fires — controller throws
   * HTTP 500 "Store doesn't have Active/Active enabled in region dc-1, but A/A is enabled in
   * parent which indicates A/A is fully ramped." Other tests in the class race past it
   * because by the time they run, the admin channel has flushed.
   *
   * <p>Subclasses that build A/A-enabled stores should call this immediately after
   * {@code createStoreForJob}.
   */
  protected void waitForAAReplicationPropagation(String storeName) {
    for (VeniceMultiClusterWrapper childRegion: childDatacenters) {
      try (
          ControllerClient childClient = new ControllerClient(CLUSTER_NAME, childRegion.getControllerConnectString())) {
        // Use the 5-arg overload with retryOnThrowable=true so transient NPE/IOException from
        // getStore() returning a partially-propagated store are retried instead of failing the
        // wait. The 4th-arg form takes exponentialBackOff (not retryOnThrowable) -- explicit
        // about which boolean does what here.
        TestUtils.waitForNonDeterministicAssertion(
            60,
            TimeUnit.SECONDS,
            false /* exponentialBackOff */,
            true /* retryOnThrowable */,
            () -> {
              StoreResponse storeResponse = childClient.getStore(storeName);
              assertNotNull(storeResponse, "null storeResponse from region " + childRegion.getRegionName());
              assertFalse(
                  storeResponse.isError(),
                  "getStore returned error in region " + childRegion.getRegionName() + ": " + storeResponse.getError());
              assertNotNull(
                  storeResponse.getStore(),
                  "store not yet propagated to region " + childRegion.getRegionName());
              assertTrue(
                  storeResponse.getStore().isActiveActiveReplicationEnabled(),
                  "A/A not propagated to region " + childRegion.getRegionName());
            });
      }
    }
  }

  /**
   * Build standard repush properties targeting dc-0 with Spark engine.
   */
  protected Properties buildRepushProps(String storeName, String inputDirPath) {
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    props.put(SOURCE_KAFKA, "true");
    props.put(KAFKA_INPUT_FABRIC, dcNames[0]);
    props.put(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5000");
    props.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    return props;
  }

  /**
   * Wait for a version to become current in all DCs.
   */
  protected void waitForVersion(String storeName, int expectedVersion) {
    String parentControllerUrl = multiRegionMultiClusterWrapper.getControllerConnectString();
    try (ControllerClient controllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
        for (int version: controllerClient.getStore(storeName).getStore().getColoToCurrentVersions().values()) {
          assertEquals(version, expectedVersion, "All DCs should be on v" + expectedVersion);
        }
      });
    }
  }

  /**
   * Assert that a stage exists with exact record counts and positive byte/time values.
   */
  protected void assertStage(
      StageMetricsSnapshot snapshot,
      String stageName,
      long expectedRecordsIn,
      long expectedRecordsOut) {
    StageSummary stage = snapshot.getStage(stageName);
    assertNotNull(stage, stageName + " stage should be registered");
    assertEquals(stage.getRecordsIn(), expectedRecordsIn, stageName + " recordsIn");
    assertEquals(stage.getRecordsOut(), expectedRecordsOut, stageName + " recordsOut");
    assertTrue(stage.getBytesIn() > 0, stageName + " bytesIn should be > 0");
    assertTrue(stage.getBytesOut() > 0, stageName + " bytesOut should be > 0");
    assertTrue(stage.getTimeNs() > 0, stageName + " timeNs should be > 0");
  }

  /**
   * Assert that a sink stage (kafka_write) exists with at least the expected input record count.
   * Uses >= because kafka_write also counts spray-all-partitions synthetic records.
   * Sink stages have recordsOut=0 and bytesOut=0 because the partition writer consumes all rows.
   */
  protected void assertSinkStage(StageMetricsSnapshot snapshot, String stageName, long minExpectedRecordsIn) {
    StageSummary stage = snapshot.getStage(stageName);
    assertNotNull(stage, stageName + " stage should be registered");
    // kafka_write counts data records + spray-all-partitions synthetic records, so use >=
    assertTrue(
        stage.getRecordsIn() >= minExpectedRecordsIn,
        stageName + " recordsIn (" + stage.getRecordsIn() + ") should be >= " + minExpectedRecordsIn);
    assertTrue(stage.getBytesIn() > 0, stageName + " bytesIn should be > 0");
    assertTrue(stage.getTimeNs() > 0, stageName + " timeNs should be > 0");
  }

  /**
   * Verify that batch data records [1..recordCount] are present and correct in the specified DC.
   */
  protected void verifyBatchData(String storeName, int recordCount, int dcIndex) {
    VeniceClusterWrapper cluster = childDatacenters.get(dcIndex).getClusters().get(CLUSTER_NAME);
    String routerUrl = cluster.getRandomRouterURL();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      try (AvroGenericStoreClient<String, Object> client = ClientFactory
          .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        for (int i = 1; i <= recordCount; i++) {
          Object value = client.get(Integer.toString(i)).get();
          assertNotNull(value, "Key " + i + " is null in dc-" + dcIndex);
          assertEquals(value.toString(), "test_name_" + i);
        }
      }
    });
  }

  /**
   * Asserts the {@code ingestion.batch_push_record_count_match.count} /
   * {@code ingestion.batch_push_record_count_mismatch.count} OTel counters fired (or stayed at 0)
   * across servers in <b>all</b> child DCs. Validates that the EOP prc header propagates leader →
   * remote local VT and that the server-side counter at EOP agrees with the producer's count.
   *
   * <p>Thin wrapper around {@link IntegrationTestPushUtils#assertBatchPushRecordCountSensors} that
   * collects servers across all child DCs in this multi-region fixture.</p>
   */
  protected void assertBatchPushRecordCountSensorsAllDcs(
      String storeName,
      boolean expectMatch,
      boolean expectMismatch) {
    List<VeniceServerWrapper> allServers = new ArrayList<>();
    for (VeniceMultiClusterWrapper dc: childDatacenters) {
      allServers.addAll(dc.getClusters().get(CLUSTER_NAME).getVeniceServers());
    }
    IntegrationTestPushUtils.assertBatchPushRecordCountSensors(allServers, storeName, expectMatch, expectMismatch);
  }

  protected GenericRecord readValue(AvroGenericStoreClient<Object, Object> storeReader, String key)
      throws ExecutionException, InterruptedException {
    return (GenericRecord) storeReader.get(key).get();
  }

  protected void validateRmdData(
      RmdSerDe rmdSerDe,
      String kafkaTopic,
      String key,
      Consumer<RmdWithValueSchemaId> rmdDataValidationFlow) {
    for (VeniceServerWrapper serverWrapper: multiRegionMultiClusterWrapper.getChildRegions()
        .get(0)
        .getClusters()
        .get("venice-cluster0")
        .getVeniceServers()) {
      StorageEngine storageEngine = serverWrapper.getVeniceServer().getStorageService().getStorageEngine(kafkaTopic);
      assertNotNull(storageEngine);
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, true, () -> {
        ValueRecord result = SingleGetChunkingAdapter
            .getReplicationMetadata(storageEngine, 0, serializeStringKeyToByteArray(key), true, null);
        assertNotNull(result, "RMD should exist for key: " + key);
        byte[] rmdBytes = result.serialize();
        RmdWithValueSchemaId rmdWithValueSchemaId = new RmdWithValueSchemaId();
        rmdSerDe.deserializeValueSchemaIdPrependedRmdBytes(rmdBytes, rmdWithValueSchemaId);
        rmdDataValidationFlow.accept(rmdWithValueSchemaId);
      });
    }
  }

  /**
   * Verifies that EOP prc headers match the expected per-partition record distribution
   * for the given keys. Reads from dc-0's broker.
   *
   * <p>Also verifies that the {@code prc} header reaches the local VT in <b>every</b> remote
   * region — exercising the leader's cross-region EOP re-emit header-propagation path. Without
   * propagation, remote-region followers would consume an EOP with no prc header and be unable
   * to run record-count verification.</p>
   */
  protected void verifyEopPartitionRecordCounts(
      String storeName,
      int version,
      int partitionCount,
      Collection<String> keys) {
    // Source region (dc-0): assert prc matches the expected partitioner-derived distribution.
    Map<Integer, Long> sourceCounts = IntegrationTestPushUtils.getEopPartitionRecordCounts(
        childDatacenters.get(0).getClusters().get(CLUSTER_NAME).getPubSubBrokerWrapper(),
        storeName,
        version,
        partitionCount);
    IntegrationTestPushUtils.verifyPerPartitionCounts(sourceCounts, keys, "\"string\"", partitionCount);

    // Remote regions: assert prc was preserved through the leader's pass-through put on local VT.
    for (int dcIndex = 1; dcIndex < childDatacenters.size(); dcIndex++) {
      Map<Integer, Long> remoteLocalVtCounts = IntegrationTestPushUtils.getEopPartitionRecordCounts(
          childDatacenters.get(dcIndex).getClusters().get(CLUSTER_NAME).getPubSubBrokerWrapper(),
          storeName,
          version,
          partitionCount);
      assertEquals(
          remoteLocalVtCounts,
          sourceCounts,
          "prc header on EOP must be preserved by the leader's re-emit to local VT in remote region dc-" + dcIndex
              + " — without propagation, remote-region followers cannot run record-count verification");
    }
  }

  protected byte[] serializeStringKeyToByteArray(String key) {
    Utf8 utf8Key = new Utf8(key);
    DatumWriter<Utf8> writer = new GenericDatumWriter<>(Schema.create(Schema.Type.STRING));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = AvroCompatibilityHelper.newBinaryEncoder(out);
    try {
      writer.write(utf8Key, encoder);
      encoder.flush();
    } catch (IOException e) {
      throw new RuntimeException("Failed to write input: " + utf8Key + " to binary encoder", e);
    }
    return out.toByteArray();
  }
}
