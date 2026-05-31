package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.TestUtils.assertCommand;
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
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.task.datawriter.ExternalStorageRecord;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.StorageMode;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * End-to-end test for the VPJ external-storage dual-write path, exercised across two regions. One region
 * (dc0) has its store-level {@code storageMode} set to {@code DUAL_WRITE} via a region-scoped
 * {@code UpdateStore}, while the other (dc1) stays {@code INTERNAL}. A single VPJ batch push resolves each
 * region's storage mode through the parent controller and fans out external writes only to the
 * {@code DUAL_WRITE} regions.
 *
 * <p>This single multi-region test covers the full cross-product of the dual-write contract (consolidating
 * what used to be a separate single-region test):
 * <ul>
 *   <li><b>Per-region routing / gating:</b> only dc0's region-keyed sink is populated; dc1's stays empty —
 *       i.e. the {@code DUAL_WRITE} region writes and the {@code INTERNAL} region does not.</li>
 *   <li><b>On-disk format:</b> each sink value is bit-for-bit {@code [4-byte BE schemaId][Avro value]}.</li>
 *   <li><b>Config forwarding:</b> an arbitrary {@code push.job.external.storage.*} key set on the driver
 *       reaches {@code configure()} on the executor.</li>
 *   <li><b>Batching:</b> {@code batchSize=1} flushes one record per {@code batchPut}; {@code batchSize=25}
 *       flushes a full 25-record batch (the store is pinned to a single partition for determinism).</li>
 *   <li><b>Ingestion unaffected:</b> Venice still serves every record in the {@code DUAL_WRITE} region.</li>
 * </ul>
 */
public class TestVPJDualWriteExternalStorageMultiRegion extends AbstractMultiRegionTest {
  private static final int READ_VERIFICATION_TIMEOUT_SEC = 60;

  @AfterMethod(alwaysRun = true)
  public void resetSink() {
    InMemoryExternalStorageWriter.clearAll();
  }

  @DataProvider(name = "batchSize")
  public Object[][] batchSize() {
    return new Object[][] { { 1 }, { 25 } };
  }

  @Test(timeOut = 240 * Time.MS_PER_SECOND, dataProvider = "batchSize")
  public void dualWriteFansOutOnlyToDualWriteRegions(int batchSize) throws Exception {
    String storeName = Utils.getUniqueString("dual_write_multi_region_b" + batchSize);
    String dc0Region = multiRegionMultiClusterWrapper.getChildRegionNames().get(0);
    String dc1Region = multiRegionMultiClusterWrapper.getChildRegionNames().get(1);

    File inputDir = Utils.getTempDataDirectory();
    writeSimpleAvroFileWithStringToStringSchema(inputDir);
    Schema stringSchema = Schema.parse("\"string\"");

    Properties props = IntegrationTestPushUtils
        .defaultVPJProps(multiRegionMultiClusterWrapper, "file://" + inputDir.getAbsolutePath(), storeName);
    props.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    props.setProperty(SPARK_NATIVE_INPUT_FORMAT_ENABLED, "true");
    props.setProperty(PUSH_JOB_EXTERNAL_STORAGE_WRITER_CLASS, InMemoryExternalStorageWriter.class.getName());
    props.setProperty(PUSH_JOB_EXTERNAL_STORAGE_BATCH_SIZE, String.valueOf(batchSize));
    // Probe an arbitrary push.job.external.storage.* key to verify driver->executor prefix forwarding reaches
    // configure(). Read back via forwardedConfigObservedFor().
    String expectedProbeValue = "probe-value-for-b" + batchSize;
    props.setProperty(InMemoryExternalStorageWriter.FORWARDED_CONFIG_PROBE_KEY, expectedProbeValue);

    try (ControllerClient parentClient = new ControllerClient(CLUSTER_NAME, parentController.getControllerUrl());
        ControllerClient dc0Client =
            new ControllerClient(CLUSTER_NAME, childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client =
            new ControllerClient(CLUSTER_NAME, childDatacenters.get(1).getControllerConnectString())) {
      assertCommand(parentClient.createNewStore(storeName, "owner", stringSchema.toString(), stringSchema.toString()));
      // Pin to a single partition so all records land in one partition writer, making the batching assertions
      // below deterministic.
      assertCommand(
          parentClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA).setPartitionCount(1)));

      // Enable dual-write in dc0 only; dc1's region-filter early-return leaves it at INTERNAL. The VPJ driver
      // reads each region's store-level value through the parent, so this must settle before the push runs.
      assertCommand(
          parentClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setStorageMode(StorageMode.DUAL_WRITE).setRegionsFilter(dc0Region)));
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        StoreInfo dc0Store = assertCommand(dc0Client.getStore(storeName)).getStore();
        StoreInfo dc1Store = assertCommand(dc1Client.getStore(storeName)).getStore();
        assertEquals(
            dc0Store.getStorageMode(),
            StorageMode.DUAL_WRITE,
            "dc0 store-level storageMode should be DUAL_WRITE");
        assertEquals(
            dc1Store.getStorageMode(),
            StorageMode.INTERNAL,
            "dc1 store-level storageMode should stay INTERNAL");
      });

      IntegrationTestPushUtils.runVPJ(props);

      String topic = Version.composeKafkaTopic(storeName, 1);

      // Build the bit-for-bit expected blob set: AvroSerializer over the value schema, then prepend the
      // BE schema-id prefix — identical to what the partition writer emits (NO_OP compression by default).
      AvroSerializer<CharSequence> valueSerializer = new AvroSerializer<>(stringSchema);
      Set<ByteBuffer> expectedFormattedValues = new HashSet<>();
      for (int i = 1; i <= DEFAULT_USER_DATA_RECORD_COUNT; i++) {
        byte[] avroPayload = valueSerializer.serialize(DEFAULT_USER_DATA_VALUE_PREFIX + i);
        byte[] formatted = new byte[ExternalStorageRecord.SCHEMA_ID_PREFIX_LENGTH + avroPayload.length];
        ByteUtils.writeInt(formatted, HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID, 0);
        System.arraycopy(avroPayload, 0, formatted, ExternalStorageRecord.SCHEMA_ID_PREFIX_LENGTH, avroPayload.length);
        expectedFormattedValues.add(ByteBuffer.wrap(formatted));
      }

      // Per-region dual-write: dc0's sink received every record; dc1's sink was never written.
      assertEquals(
          InMemoryExternalStorageWriter.regionsWithDataForTopic(topic),
          Collections.singleton(dc0Region),
          "Only the DUAL_WRITE region (dc0) should have an external sink populated");

      Map<ByteBuffer, byte[]> dc0Sink = InMemoryExternalStorageWriter.snapshotForRegionAndTopic(dc0Region, topic);
      assertEquals(
          dc0Sink.size(),
          DEFAULT_USER_DATA_RECORD_COUNT,
          "dc0 external sink did not receive every pushed record");
      Set<ByteBuffer> actualFormattedValues = new HashSet<>();
      for (byte[] formattedValue: dc0Sink.values()) {
        actualFormattedValues.add(ByteBuffer.wrap(formattedValue));
      }
      assertEquals(
          actualFormattedValues,
          expectedFormattedValues,
          "dc0 sink blobs must be bit-for-bit [4-byte BE schemaId][Avro-encoded value] for every input record");

      // Per-key cross-check: for each input key, look up its blob in the external sink (keyed by the serialized
      // key, exactly as the partition writer emitted it), strip the 4-byte BE schema-id prefix, and Avro-decode
      // the value. Confirm the decoded value matches the expected input value for that specific key. This ties
      // each external-storage entry to a key (the set assertion above is key-agnostic) and the decoded map is
      // cross-checked against what Venice serves in the read-verification block below.
      AvroSerializer<CharSequence> keySerializer = new AvroSerializer<>(stringSchema);
      AvroGenericDeserializer<Object> valueDeserializer = new AvroGenericDeserializer<>(stringSchema, stringSchema);
      Map<String, String> externalDecodedByKey = new HashMap<>();
      for (int i = 1; i <= DEFAULT_USER_DATA_RECORD_COUNT; i++) {
        String key = Integer.toString(i);
        byte[] blob = dc0Sink.get(ByteBuffer.wrap(keySerializer.serialize(key)));
        assertNotNull(blob, "dc0 external sink has no entry for key " + key);
        ByteBuffer view = ByteBuffer.wrap(blob);
        assertEquals(
            view.getInt(),
            HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID,
            "External value for key " + key + " must be prefixed with the value schema id");
        byte[] valuePayload = new byte[view.remaining()];
        view.get(valuePayload);
        String decoded = valueDeserializer.deserialize(valuePayload).toString();
        assertEquals(
            decoded,
            DEFAULT_USER_DATA_VALUE_PREFIX + i,
            "External-stored value for key " + key + " does not match the expected input value");
        externalDecodedByKey.put(key, decoded);
      }

      assertTrue(
          InMemoryExternalStorageWriter.snapshotForRegionAndTopic(dc1Region, topic).isEmpty(),
          "dc1 is INTERNAL; its external sink must stay empty");

      // Config forwarding: the arbitrary push.job.external.storage.* probe set on the driver reached
      // configure() on the executor.
      assertEquals(
          InMemoryExternalStorageWriter.forwardedConfigObservedFor(topic),
          expectedProbeValue,
          "Custom push.job.external.storage.* property did not propagate from driver to executor's configure()");

      // Batching contract: batchSize=1 never batches; batchSize>1 flushes at least one full-size batch (the
      // store is pinned to a single partition above so this is deterministic).
      int observedMaxBatch = InMemoryExternalStorageWriter.maxBatchSizeFor(topic);
      assertTrue(InMemoryExternalStorageWriter.batchPutInvocationsFor(topic) > 0, "Expected at least one batchPut");
      if (batchSize == 1) {
        assertEquals(observedMaxBatch, 1, "With batchSize=1 the wrapper must never batch");
      } else {
        assertEquals(observedMaxBatch, batchSize, "With batchSize=" + batchSize + " a full batch should flush");
      }

      // Venice still serves every record in the DUAL_WRITE region — dual-write must not break ingestion.
      VeniceClusterWrapper dc0Cluster = getCluster(0);
      try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(dc0Cluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(READ_VERIFICATION_TIMEOUT_SEC, TimeUnit.SECONDS, true, true, () -> {
          dc0Cluster.refreshAllRouterMetaData();
          for (int i = 1; i <= DEFAULT_USER_DATA_RECORD_COUNT; i++) {
            Object veniceValue = client.get(Integer.toString(i)).get();
            assertNotNull(veniceValue, "Thin client returned null for key " + i);
            assertEquals(
                veniceValue.toString(),
                DEFAULT_USER_DATA_VALUE_PREFIX + i,
                "Venice value mismatch for key " + i);
            // Direct external == Venice per key: the value an external reader decodes from dc0's sink equals
            // what Venice serves for the same key (non-chunked case).
            assertEquals(
                externalDecodedByKey.get(Integer.toString(i)),
                veniceValue.toString(),
                "External-storage value and Venice value diverge for key " + i);
          }
        });
      }
    }
  }
}
