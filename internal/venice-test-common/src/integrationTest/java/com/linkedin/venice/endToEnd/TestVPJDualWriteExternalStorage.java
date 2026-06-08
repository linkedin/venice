package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_VALUE_PREFIX;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_EXTERNAL_STORAGE_BATCH_SIZE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_EXTERNAL_STORAGE_WRITER_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SPARK_NATIVE_INPUT_FORMAT_ENABLED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.task.datawriter.ExternalStorageRecord;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.StorageMode;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * End-to-end integration test for the VPJ external-storage dual-write path. The gating predicate fires only
 * when both halves are set: the VPJ-side writer-class config AND the store-version-side {@code storageMode
 * == DUAL_WRITE}. This test runs three parameterizations to exercise the cross-product:
 *
 * <ul>
 *   <li>{@code batchSize=1, storageMode=DUAL_WRITE} — dual-write fires; the impl observes one batchPut of
 *       size {@code 1} per record.</li>
 *   <li>{@code batchSize=25, storageMode=DUAL_WRITE} — dual-write fires; the impl observes at least one
 *       batchPut of size {@code 25}.</li>
 *   <li>{@code batchSize=1, storageMode=INTERNAL} — gate is closed even though the writer-class is set;
 *       the impl is never loaded and the in-memory sink stays empty for the push topic. Venice still
 *       receives every record.</li>
 * </ul>
 */
public class TestVPJDualWriteExternalStorage {
  private static final int TEST_TIMEOUT_MS = 120_000;
  private static final int STORE_VERSION_AVAILABILITY_TIMEOUT_SEC = 60;
  private static final int READ_VERIFICATION_TIMEOUT_SEC = 60;

  private VeniceClusterWrapper veniceCluster;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
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
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    IOUtils.closeQuietly(veniceCluster);
  }

  @AfterMethod(alwaysRun = true)
  public void resetSink() {
    InMemoryExternalStorageWriter.clearAll();
  }

  @DataProvider(name = "gateAndBatchSize")
  public Object[][] gateAndBatchSize() {
    return new Object[][] {
        // batchSize, storageMode, expectDualWrite
        { 1, StorageMode.DUAL_WRITE, true }, { 25, StorageMode.DUAL_WRITE, true }, { 1, StorageMode.INTERNAL, false } };
  }

  @Test(timeOut = TEST_TIMEOUT_MS, dataProvider = "gateAndBatchSize")
  public void dualWritePathHonorsBothHalvesOfTheGate(int batchSize, StorageMode storageMode, boolean expectDualWrite)
      throws Exception {
    File inputDir = Utils.getTempDataDirectory();
    writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String storeName = Utils.getUniqueString("dual_write_store_" + storageMode.name().toLowerCase() + "_b" + batchSize);

    Schema stringSchema = Schema.parse("\"string\"");
    Properties props = defaultVPJProps(veniceCluster, "file://" + inputDir.getAbsolutePath(), storeName);
    props.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    props.setProperty(SPARK_NATIVE_INPUT_FORMAT_ENABLED, "true");
    props.setProperty(PUSH_JOB_EXTERNAL_STORAGE_WRITER_CLASS, InMemoryExternalStorageWriter.class.getName());
    props.setProperty(PUSH_JOB_EXTERNAL_STORAGE_BATCH_SIZE, String.valueOf(batchSize));
    // Probe value to verify that arbitrary keys under the push.job.external.storage.* prefix are
    // forwarded from the VPJ driver to the executor's task properties (the OSS prefix-forwarding rule
    // in AbstractDataWriterSparkJob). Read back by InMemoryExternalStorageWriter.configure().
    String expectedProbeValue = "probe-value-for-b" + batchSize + "-" + storageMode.name();
    props.setProperty(InMemoryExternalStorageWriter.FORWARDED_CONFIG_PROBE_KEY, expectedProbeValue);

    // Set the store-level storageMode up-front; the controller copies it onto every new version (#2823) and
    // VPJ reads it back during validateStoreSettingAndPopulate as the per-push targetStorageMode.
    UpdateStoreQueryParams storeParams = new UpdateStoreQueryParams().setStorageMode(storageMode);
    createStoreForJob(
        veniceCluster.getClusterName(),
        stringSchema.toString(),
        stringSchema.toString(),
        props,
        storeParams).close();

    IntegrationTestPushUtils.runVPJ(props);

    int currentVersion = waitForStoreVersionToBeServing(storeName);
    String currentTopic = Version.composeKafkaTopic(storeName, currentVersion);

    AvroGenericDeserializer<Object> sinkValueDeserializer = new AvroGenericDeserializer<>(stringSchema, stringSchema);

    try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      // Wait-for-non-deterministic absorbs router routing-data readiness and ZSTD-dictionary-download
      // latency for both the Venice-only and dual-write paths.
      TestUtils.waitForNonDeterministicAssertion(READ_VERIFICATION_TIMEOUT_SEC, TimeUnit.SECONDS, true, true, () -> {
        // Venice side: every input key returns the expected value via the thin client. Holds regardless of
        // whether dual-write is engaged.
        for (int i = 1; i <= DEFAULT_USER_DATA_RECORD_COUNT; i++) {
          Object veniceValue = client.get(Integer.toString(i)).get();
          assertNotNull(veniceValue, "Thin client returned null for key " + i);
          assertEquals(
              veniceValue.toString(),
              DEFAULT_USER_DATA_VALUE_PREFIX + i,
              "Venice value mismatch for key " + i);
        }

        Map<ByteBuffer, byte[]> sink = InMemoryExternalStorageWriter.snapshotForTopic(currentTopic);
        int batchPutInvocations = InMemoryExternalStorageWriter.batchPutInvocationsFor(currentTopic);

        if (!expectDualWrite) {
          // Gate is closed: the wrapper must not load or invoke the external impl for this topic.
          assertEquals(
              batchPutInvocations,
              0,
              "Gate is closed (storageMode=" + storageMode
                  + "); InMemoryExternalStorageWriter must never receive a batchPut for " + currentTopic);
          assertTrue(sink.isEmpty(), "Gate is closed; sink for " + currentTopic + " must be empty");
          // The impl is never loaded when the gate is closed, so the forwarded-config probe never
          // executed — observed value should be null. (No assertion on the prefix forwarding itself in
          // this branch; the positive branches below cover it.)
          return;
        }

        // Verify the OSS prefix-forwarding rule: the probe value set on the VPJ driver under the
        // push.job.external.storage.* prefix should have reached configure() on the executor.
        assertEquals(
            InMemoryExternalStorageWriter.forwardedConfigObservedFor(currentTopic),
            expectedProbeValue,
            "Custom push.job.external.storage.* property did not propagate from driver to executor's configure()");

        // Dual-write fires: external sink received every record. The on-disk format matches Venice's
        // RocksDB layout — first 4 bytes are the BE-encoded value schema id, the rest is the Avro payload
        // (uncompressed here since the store uses NO_OP compression by default). A reader of the external
        // sink decodes without any chunk reassembly.
        assertEquals(
            sink.size(),
            DEFAULT_USER_DATA_RECORD_COUNT,
            "External sink for topic " + currentTopic + " did not receive every pushed record");

        // Build the bit-for-bit expected blob set by reproducing the same encoding the partition writer
        // emits: AvroSerializer over the value schema, then prepend the BE schema-id prefix. The set is
        // unordered because Spark task scheduling does not preserve input order in the sink map.
        AvroSerializer<CharSequence> valueSerializer = new AvroSerializer<>(stringSchema);
        Set<ByteBuffer> expectedFormattedValues = new HashSet<>();
        for (int i = 1; i <= DEFAULT_USER_DATA_RECORD_COUNT; i++) {
          byte[] avroPayload = valueSerializer.serialize(DEFAULT_USER_DATA_VALUE_PREFIX + i);
          byte[] formatted = new byte[ExternalStorageRecord.SCHEMA_ID_PREFIX_LENGTH + avroPayload.length];
          ByteUtils.writeInt(formatted, HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID, 0);
          System
              .arraycopy(avroPayload, 0, formatted, ExternalStorageRecord.SCHEMA_ID_PREFIX_LENGTH, avroPayload.length);
          expectedFormattedValues.add(ByteBuffer.wrap(formatted));
        }
        Set<ByteBuffer> actualFormattedValues = new HashSet<>();
        for (byte[] formattedValue: sink.values()) {
          actualFormattedValues.add(ByteBuffer.wrap(formattedValue));
        }
        assertEquals(
            actualFormattedValues,
            expectedFormattedValues,
            "Sink blobs must be bit-for-bit [4-byte BE schemaId][Avro-encoded value] for every input record");

        // Cross-check via the deserializer path that an external reader who knows the format can decode
        // back to the original value strings. Belt-and-suspenders relative to the byte-level set assert.
        Set<String> decodedSinkValues = new HashSet<>();
        for (byte[] formattedValue: sink.values()) {
          ByteBuffer view = ByteBuffer.wrap(formattedValue);
          int schemaId = view.getInt();
          assertEquals(
              schemaId,
              HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID,
              "Schema-id prefix should match the only registered value schema for this store");
          decodedSinkValues.add(sinkValueDeserializer.deserialize(view).toString());
        }
        for (int i = 1; i <= DEFAULT_USER_DATA_RECORD_COUNT; i++) {
          String expected = DEFAULT_USER_DATA_VALUE_PREFIX + i;
          assertTrue(decodedSinkValues.contains(expected), "External sink missing decoded value " + expected);
        }

        // Batching contract: when batchSize > 1 the wrapper must flush at least one full-size batch; when
        // batchSize == 1 the wrapper must never batch.
        int observedMaxBatch = InMemoryExternalStorageWriter.maxBatchSizeFor(currentTopic);
        assertTrue(batchPutInvocations > 0, "Expected at least one batchPut invocation for " + currentTopic);
        if (batchSize == 1) {
          assertEquals(
              observedMaxBatch,
              1,
              "With batchSize=1 the wrapper must never batch; expected per-record batchPut of size 1");
        } else {
          assertEquals(
              observedMaxBatch,
              batchSize,
              "With batchSize=" + batchSize + " the wrapper should have flushed at least one full batch");
        }
      });
    }
  }

  private int waitForStoreVersionToBeServing(String storeName) {
    AtomicInteger currentVersionRef = new AtomicInteger(0);
    veniceCluster.useControllerClient(controllerClient -> {
      TestUtils.waitForNonDeterministicAssertion(
          STORE_VERSION_AVAILABILITY_TIMEOUT_SEC,
          TimeUnit.SECONDS,
          true,
          true,
          () -> {
            int currentVersion = controllerClient.getStore(storeName).getStore().getCurrentVersion();
            assertTrue(currentVersion > 0, "Store " + storeName + " has no current version yet");
            veniceCluster.refreshAllRouterMetaData();
            currentVersionRef.set(currentVersion);
          });
    });
    return currentVersionRef.get();
  }
}
