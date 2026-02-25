package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.ChunkingTestUtils.getChunkValueManifest;
import static com.linkedin.venice.utils.ChunkingTestUtils.validateChunksFromManifests;
import static com.linkedin.venice.utils.ChunkingTestUtils.validateValueChunks;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.IntegrationTestReadUtils.readValue;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V2_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.tehuti.MetricsUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * This class includes tests on partial update for Non-A/A scenarios:
 * chunk deletion, streaming rewind, superset schema, and batch write-compute.
 */
public class PartialUpdateNonAATest extends AbstractMultiRegionTest {
  private static final int TEST_TIMEOUT_MS = 180_000;

  @Test(timeOut = TEST_TIMEOUT_MS * 3)
  public void testNonAAPartialUpdateChunkDeletion() {
    final String storeName = Utils.getUniqueString("partialUpdateChunking");
    String parentControllerUrl = getParentControllerUrl();
    String keySchemaStr = "{\"type\" : \"string\"}";
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("CollectionRecordV1.avsc"));
    Schema partialUpdateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);
    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient.createNewStore(storeName, "test_owner", keySchemaStr, valueSchema.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setWriteComputationEnabled(true)
              .setActiveActiveReplicationEnabled(false)
              .setChunkingEnabled(true)
              .setRmdChunkingEnabled(false)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
    }

    VeniceClusterWrapper veniceCluster = getClusterDC0();

    String key = "key1";
    String primitiveFieldName = "name";
    String mapFieldName = "stringMap";

    // Insert large amount of Map entries to trigger value chunking.
    int oldUpdateCount = 29;
    int singleUpdateEntryCount = 10000;
    try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);
        AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      for (int i = 0; i < oldUpdateCount; i++) {
        producePartialUpdate(
            storeName,
            veniceProducer,
            partialUpdateSchema,
            key,
            primitiveFieldName,
            mapFieldName,
            singleUpdateEntryCount,
            i);
      }
      // Verify the value record has been partially updated.
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS, true, () -> {
        try {
          GenericRecord valueRecord = readValue(storeReader, key);
          boolean nullRecord = (valueRecord == null);
          assertFalse(nullRecord);
          assertEquals(valueRecord.get(primitiveFieldName).toString(), "Tottenham"); // Updated field
          Map<String, String> mapFieldResult = new HashMap<>();
          ((Map<Utf8, Utf8>) valueRecord.get(mapFieldName))
              .forEach((x, y) -> mapFieldResult.put(x.toString(), y.toString()));
          assertEquals(mapFieldResult.size(), oldUpdateCount * singleUpdateEntryCount);
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

      String kafkaTopic_v1 = Version.composeKafkaTopic(storeName, 1);
      validateValueChunks(multiRegionMultiClusterWrapper, CLUSTER_NAME, kafkaTopic_v1, key, Assert::assertNotNull);
      VeniceServerWrapper serverWrapper = multiRegionMultiClusterWrapper.getChildRegions()
          .get(0)
          .getClusters()
          .get(CLUSTER_NAME)
          .getVeniceServers()
          .get(0);
      StorageEngine storageEngine = serverWrapper.getVeniceServer().getStorageService().getStorageEngine(kafkaTopic_v1);
      ChunkedValueManifest valueManifest = getChunkValueManifest(storageEngine, 0, key, false);

      int updateCount = 30;
      producePartialUpdate(
          storeName,
          veniceProducer,
          partialUpdateSchema,
          key,
          primitiveFieldName,
          mapFieldName,
          singleUpdateEntryCount,
          updateCount - 1);

      // Verify the value record has been partially updated.
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS, true, () -> {
        try {
          GenericRecord valueRecord = readValue(storeReader, key);
          boolean nullRecord = (valueRecord == null);
          assertFalse(nullRecord);
          assertEquals(valueRecord.get(primitiveFieldName).toString(), "Tottenham"); // Updated field
          Map<String, String> mapFieldResult = new HashMap<>();
          ((Map<Utf8, Utf8>) valueRecord.get(mapFieldName))
              .forEach((x, y) -> mapFieldResult.put(x.toString(), y.toString()));
          assertEquals(mapFieldResult.size(), updateCount * singleUpdateEntryCount);
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        Assert.assertNotNull(valueManifest);
        validateChunksFromManifests(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            kafkaTopic_v1,
            0,
            valueManifest,
            null,
            (valueChunkBytes, rmdChunkBytes) -> {
              Assert.assertNull(valueChunkBytes);
            },
            false);
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT_MS * 3)
  public void testNonAARewind() {
    final String storeName = Utils.getUniqueString("test");
    String parentControllerUrl = getParentControllerUrl();
    String keySchemaStr = "{\"type\" : \"string\"}";
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("CollectionRecordV1.avsc"));

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient.createNewStore(storeName, "test_owner", keySchemaStr, valueSchema.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setChunkingEnabled(true)
              .setPartitionCount(1)
              .setHybridRewindSeconds(100000L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
    }

    VeniceClusterWrapper veniceCluster = getClusterDC0();

    String key = "key";
    String primitiveFieldName = "name";
    String listFieldName = "floatArray";
    String mapFieldName = "stringMap";

    int totalUpdateCount = 1000;
    try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);
        AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      for (int i = 0; i < totalUpdateCount; i++) {
        GenericRecord value = new GenericData.Record(valueSchema);
        value.put(primitiveFieldName, "London");
        value.put(listFieldName, new ArrayList<>());
        value.put(mapFieldName, new HashMap<>());
        sendStreamingRecord(veniceProducer, storeName, key + i, value);
      }
      // Verify the value record has been partially updated.
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS, true, true, () -> {
        try {
          for (int i = 0; i < totalUpdateCount; i++) {
            GenericRecord valueRecord = readValue(storeReader, key + i);
            boolean nullRecord = (valueRecord == null);
            assertFalse(nullRecord);
            assertEquals(valueRecord.get(primitiveFieldName).toString(), "London");
          }
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      VersionCreationResponse response =
          TestUtils.assertCommand(parentControllerClient.emptyPush(storeName, "test_push_id_v2", 1000));
      TestUtils.waitForNonDeterministicPushCompletion(
          response.getKafkaTopic(),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
    }

    String baseMetricName = "." + storeName + "_current--duplicate_msg.DIVStatsGauge";
    double totalCountMetric = MetricsUtils.getSum(baseMetricName, veniceCluster.getVeniceServers());
    baseMetricName = "." + storeName + "_future--duplicate_msg.DIVStatsGauge";
    totalCountMetric += MetricsUtils.getSum(baseMetricName, veniceCluster.getVeniceServers());
    // ToDo: Duplicate message is caused by consuming the end of segment for VSM during resubscription.
    // Figure out how to deal with it.
    Assert.assertTrue(totalCountMetric <= 2.0d);
  }

  /**
   * This test simulates a situation where the stored value schema mismatches with the value schema used by a partial update
   * request. In other words, the partial update request tries to update a field that does not exist in the stored value
   * record due to schema mismatch.
   *
   * In this case, we expect a superset schema that contains fields from all value schema to be used to store the partially
   * updated value record. The partially updated value record should contain original fields as well as the partially updated
   * field.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testUpdateWithSupersetSchema() {
    final String storeName = Utils.getUniqueString("store");
    String parentControllerUrl = getParentControllerUrl();
    Schema valueSchemaV1 = AvroCompatibilityHelper.parse(loadFileAsString("writecompute/test/PersonV1.avsc"));
    Schema valueSchemaV2 = AvroCompatibilityHelper.parse(loadFileAsString("writecompute/test/PersonV2.avsc"));
    String valueFieldName = "name";

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", STRING_SCHEMA.toString(), valueSchemaV1.toString()));

      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setWriteComputationEnabled(true)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      assertCommand(parentControllerClient.addValueSchema(storeName, valueSchemaV2.toString()));
    }

    SystemProducer veniceProducer = null;
    VeniceClusterWrapper veniceCluster = getClusterDC0();

    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {

      // Step 1. Put a value record.
      veniceProducer = IntegrationTestPushUtils.getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);
      String key = "key1";
      GenericRecord value = new GenericData.Record(valueSchemaV1);
      value.put(valueFieldName, "Lebron");
      value.put("age", 37);
      sendStreamingRecord(veniceProducer, storeName, key, value);

      // Verify the Put has been persisted
      TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, () -> {
        try {
          GenericRecord retrievedValue = readValue(storeReader, key);
          assertNotNull(retrievedValue);
          assertEquals(retrievedValue.get(valueFieldName).toString(), "Lebron");
          assertEquals(retrievedValue.get("age").toString(), "37");

        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

      // Step 2: Partially update a field that exists in V2 schema (and it does not exist in V1 schema).
      Schema writeComputeSchemaV2 =
          WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchemaV2);
      UpdateBuilder updateBuilder = new UpdateBuilderImpl(writeComputeSchemaV2);
      updateBuilder.setNewFieldValue(valueFieldName, "Lebron James");
      updateBuilder.setNewFieldValue("hometown", "Akron");
      GenericRecord partialUpdateRecord = updateBuilder.build();
      sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord);

      // Verify the value record has been partially updated and it uses V3 superset value schema now.
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        try {
          GenericRecord retrievedValue = readValue(storeReader, key);
          assertNotNull(retrievedValue);
          assertEquals(retrievedValue.get(valueFieldName).toString(), "Lebron James"); // Updated field
          assertEquals(retrievedValue.get("age").toString(), "37");
          assertEquals(retrievedValue.get("hometown").toString(), "Akron"); // Updated field

        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

    } finally {
      if (veniceProducer != null) {
        veniceProducer.stop();
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testWriteComputeWithSamzaBatchJob() throws Exception {
    long streamingRewindSeconds = 10L;
    long streamingMessageLag = 2L;

    String storeName = Utils.getUniqueString("write-compute-store");
    File inputDir = getTempDataDirectory();
    String parentControllerURL = getParentControllerUrl();
    // Records 1-100, id string to name record
    Schema recordSchema = writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    VeniceClusterWrapper veniceClusterWrapper = getClusterDC0();
    try (ControllerClient controllerClient = new ControllerClient(CLUSTER_NAME, parentControllerURL)) {

      String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
      String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
      assertCommand(controllerClient.createNewStore(storeName, "test_owner", keySchemaStr, valueSchemaStr));

      ControllerResponse response = controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setHybridRewindSeconds(streamingRewindSeconds)
              .setHybridOffsetLagThreshold(streamingMessageLag)
              .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setWriteComputationEnabled(true)
              .setChunkingEnabled(true));

      assertFalse(response.isError());

      // Add a new value schema v2 to store
      SchemaResponse schemaResponse = controllerClient.addValueSchema(storeName, NAME_RECORD_V2_SCHEMA.toString());
      assertFalse(schemaResponse.isError());

      // Add WC (Write Compute) schema associated to v2.
      // (this is a test environment only needed step since theres no parent)
      Schema writeComputeSchema =
          WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(NAME_RECORD_V2_SCHEMA);
      schemaResponse =
          controllerClient.addDerivedSchema(storeName, schemaResponse.getId(), writeComputeSchema.toString());
      assertFalse(schemaResponse.isError());

      // Run empty push to create a version and get everything created
      controllerClient.sendEmptyPushAndWait(storeName, "foopush", 10000, 60 * Time.MS_PER_SECOND);

      // build partial update
      char[] chars = new char[5];
      Arrays.fill(chars, 'f');
      String firstName = new String(chars);
      Arrays.fill(chars, 'l');
      String lastName = new String(chars);

      UpdateBuilder updateBuilder = new UpdateBuilderImpl(writeComputeSchema);
      updateBuilder.setNewFieldValue("firstName", firstName);
      updateBuilder.setNewFieldValue("lastName", lastName);
      GenericRecord partialUpdateRecord = updateBuilder.build();

      try (VeniceSystemProducer veniceProducer =
          IntegrationTestPushUtils.getSamzaProducerForBatch(multiRegionMultiClusterWrapper, storeName)) {
        for (int i = 0; i < 10; i++) {
          String key = String.valueOf(i);
          sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord);
        }
        // send end of push
        controllerClient.writeEndOfPush(storeName, 2);
      }

      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {
        // Verify everything made it
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          try {
            for (int i = 0; i < 10; i++) {
              GenericRecord retrievedValue = readValue(storeReader, Integer.toString(i));
              assertNotNull(retrievedValue, "Key " + i + " should not be missing!");
              assertEquals(retrievedValue.get("firstName").toString(), firstName);
              assertEquals(retrievedValue.get("lastName").toString(), lastName);
              assertEquals(retrievedValue.get("age").toString(), "-1");
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }
  }

  private void producePartialUpdate(
      String storeName,
      SystemProducer veniceProducer,
      Schema partialUpdateSchema,
      String key,
      String primitiveFieldName,
      String mapFieldName,
      int singleUpdateEntryCount,
      int updateCount) {
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
    updateBuilder.setNewFieldValue(primitiveFieldName, "Tottenham");
    Map<String, String> newEntries = new HashMap<>();
    for (int j = 0; j < singleUpdateEntryCount; j++) {
      String idx = String.valueOf(updateCount * singleUpdateEntryCount + j);
      newEntries.put("key_" + idx, "value_" + idx);
    }
    updateBuilder.setEntriesToAddToMapField(mapFieldName, newEntries);
    GenericRecord partialUpdateRecord = updateBuilder.build();
    sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord, updateCount * 10L + 1);
  }
}
