package com.linkedin.venice.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.ImmutableChangeCapturePubSubMessage;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.davinci.utils.ClientRmdSerDe;
import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PushInputSchemaBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestVersionSpecificChangelogConsumerReplicationMetadata {
  private static final Logger LOGGER =
      LogManager.getLogger(TestVersionSpecificChangelogConsumerReplicationMetadata.class);
  static final int TEST_TIMEOUT = 3 * 60_000; // 3 minutes

  private ChangelogConsumerTestFixture fixture;

  protected boolean isAAWCParallelProcessingEnabled() {
    return false;
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    fixture = new ChangelogConsumerTestFixture(isAAWCParallelProcessingEnabled());
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(fixture);
  }

  @AfterMethod(alwaysRun = true)
  public void cleanupAfterTest() {
    fixture.cleanupAfterTest();
  }

  /**
   * Verifies that the RMD payload is present and correctly deserializable on the version-specific changelog consumer
   * for chunked records with no compression.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testVersionSpecificWithChunkedRecordRmdPayload()
      throws IOException, ExecutionException, InterruptedException {
    runVersionSpecificWithChunkedRecordRmdPayload(CompressionStrategy.NO_OP);
  }

  /**
   * Same as {@link #testVersionSpecificWithChunkedRecordRmdPayload()} but with GZIP compression.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testVersionSpecificWithChunkedRecordRmdPayloadGzip()
      throws IOException, ExecutionException, InterruptedException {
    runVersionSpecificWithChunkedRecordRmdPayload(CompressionStrategy.GZIP);
  }

  /**
   * Same as {@link #testVersionSpecificWithChunkedRecordRmdPayload()} but with ZSTD_WITH_DICT compression.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testVersionSpecificWithChunkedRecordRmdPayloadZstd()
      throws IOException, ExecutionException, InterruptedException {
    runVersionSpecificWithChunkedRecordRmdPayload(CompressionStrategy.ZSTD_WITH_DICT);
  }

  /**
   * Verifies that the RMD payload is present and correctly deserializable on the version-specific changelog consumer
   * for chunked records. The test:
   * (1) Batch-pushes one large record (floatArray of 265K floats ≈ 1.06 MB) that exceeds the ~950 KB chunking
   *     threshold. This creates a chunked batch record in the version topic with no RMD.
   * (2) Sends a write-compute partial update that only touches the "name" field. The AA leader merges this into the
   *     existing large record, producing a chunked VT record with a small RMD (floatArray stays in put-only state,
   *     so activeElemTS remains empty and the RMD stays small).
   * (3) Reads from the version-specific changelog consumer and verifies that the near-line message carries a
   *     non-null replicationMetadataPayload that ClientRmdSerDe can deserialize into a valid RMD record with a
   *     positive timestamp.
   */
  private void runVersionSpecificWithChunkedRecordRmdPayload(CompressionStrategy compressionStrategy)
      throws IOException, ExecutionException, InterruptedException {
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("CollectionRecordV1.avsc"));
    Schema partialUpdateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);
    Schema pushInputSchema =
        new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA).setValueSchema(valueSchema).build();

    // 265K floats × 4 bytes ≈ 1.06 MB, which exceeds the ~950 KB chunking threshold.
    List<Float> largeFloatArray = new ArrayList<>(265_000);
    for (int i = 0; i < 265_000; i++) {
      largeFloatArray.add((float) i);
    }

    File inputDir = getTempDataDirectory();
    File avroFile = new File(inputDir, "batch_data.avro");
    try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(pushInputSchema))) {
      writer.create(pushInputSchema, avroFile);
      GenericRecord valueRecord = new GenericData.Record(valueSchema);
      valueRecord.put("name", "initial_name");
      valueRecord.put("floatArray", largeFloatArray);
      valueRecord.put("stringMap", Collections.emptyMap());
      GenericRecord inputRecord = new GenericData.Record(pushInputSchema);
      inputRecord.put(DEFAULT_KEY_FIELD_PROP, "key1");
      inputRecord.put(DEFAULT_VALUE_FIELD_PROP, valueRecord);
      writer.append(inputRecord);
    }

    String storeName = Utils.getUniqueString("testRmdChunking");
    fixture.addStoreToDelete(storeName);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProps = TestWriteUtils.defaultVPJProps(
        fixture.getParentControllers().get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        fixture.getClusterWrapper().getPubSubClientProperties());

    String keySchemaStr = pushInputSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = pushInputSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    UpdateStoreQueryParams storeParams = ChangelogConsumerTestUtils.buildDefaultStoreParams()
        .setWriteComputationEnabled(true)
        .setCompressionStrategy(compressionStrategy);
    createStoreForJob(fixture.getClusterName(), keySchemaStr, valueSchemaStr, vpjProps, storeParams);
    fixture.getParentControllerClient()
        .updateStore(storeName, new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true));
    ChangelogConsumerTestUtils.waitForMetaSystemStoreToBeReady(
        storeName,
        fixture.getChildControllerClientRegion0(),
        fixture.getClusterWrapper());
    IntegrationTestPushUtils.runVPJ(vpjProps, 1, fixture.getChildControllerClientRegion0());

    // Send several write-compute partial update that only touches "name", leaving floatArray in put-only state.
    // The AA leader merges this into the existing large record, producing a chunked VT record with a small RMD.
    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducerForStream(fixture.getMultiRegionMultiClusterWrapper(), 0, storeName)) {
      for (int i = 0; i < 10; i++) {
        UpdateBuilder updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
        updateBuilder.setNewFieldValue("name", "updated_name_" + i);
        sendStreamingRecord(veniceProducer, storeName, "key1", updateBuilder.build(), null);
      }
    }

    Properties consumerProperties = ChangelogConsumerTestUtils.buildConsumerProperties(
        fixture.getMultiRegionMultiClusterWrapper(),
        fixture.getLocalKafka(),
        fixture.getClusterName(),
        fixture.getLocalZkServer());
    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
    ChangelogClientConfig globalChangelogClientConfig = ChangelogConsumerTestUtils
        .buildBaseChangelogClientConfig(consumerProperties, fixture.getLocalZkServer().getAddress(), 3)
        .setD2Client(fixture.getD2Client())
        .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);

    VeniceChangelogConsumer<Utf8, GenericRecord> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getVersionSpecificChangelogConsumer(storeName, 1, false);
    fixture.addCloseable(changeLogConsumer);
    changeLogConsumer.subscribeAll().get();

    try (StoreSchemaFetcher schemaFetcher = ClientFactory.createStoreSchemaFetcher(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setD2Client(fixture.getD2Client())
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME))) {
      ClientRmdSerDe clientRmdSerDe = new ClientRmdSerDe(schemaFetcher);
      List<String> recordsWithValidatedRmd = new ArrayList<>();
      final AtomicInteger index = new AtomicInteger(0);
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        Collection<PubSubMessage<Utf8, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> pubSubMessages =
            changeLogConsumer.poll(1000);
        for (PubSubMessage<Utf8, ChangeEvent<GenericRecord>, VeniceChangeCoordinate> msg: pubSubMessages) {
          if (msg.getKey() == null) {
            continue; // skip control messages
          }
          ImmutableChangeCapturePubSubMessage<Utf8, ChangeEvent<GenericRecord>> message =
              (ImmutableChangeCapturePubSubMessage<Utf8, ChangeEvent<GenericRecord>>) msg;
          ByteBuffer rmdPayload = message.getReplicationMetadataPayload();
          if (rmdPayload == null || !rmdPayload.hasRemaining()) {
            continue; // skip empty rmd records
          }
          String key = msg.getKey().toString();
          String nameValue = msg.getValue().getCurrentValue().get("name").toString();
          GenericRecord rmdRecord =
              clientRmdSerDe.deserializeRmdBytes(message.getWriterSchemaId(), message.getWriterSchemaId(), rmdPayload);
          assertNotNull(rmdRecord, "Deserialized RMD record should not be null for key: " + key);
          // The timestamp field is a union: either a Long (put-only) or a per-field GenericRecord.
          // After a partial update that only touches "name", it is a per-field record.
          Object timestampObj = rmdRecord.get("timestamp");
          long timestamp;
          if (timestampObj instanceof Long) {
            timestamp = (Long) timestampObj;
          } else {
            // Per-field record: use the "name" field's timestamp as a representative positive value.
            timestamp = (long) ((GenericRecord) timestampObj).get("name");
          }
          assertTrue(timestamp > 0, "RMD timestamp should be positive for key: " + key);
          assertEquals(nameValue, "updated_name_" + index.get());
          index.incrementAndGet();
          recordsWithValidatedRmd.add(nameValue);
        }
        assertEquals(recordsWithValidatedRmd.size(), 10);
      });
    }
  }
}
