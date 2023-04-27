package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.hadoop.VenicePushJob.REWIND_TIME_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.hadoop.VenicePushJob.SOURCE_KAFKA;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.PARENT_D2_SERVICE_NAME;
import static com.linkedin.venice.samza.VeniceSystemFactory.DEPLOYMENT_ID;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_AGGREGATE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PARENT_CONTROLLER_D2_SERVICE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PARENT_D2_ZK_HOSTS;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducerConfig;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingDeleteRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.NESTED_SCHEMA_STRING;
import static com.linkedin.venice.utils.TestWriteUtils.NESTED_SCHEMA_STRING_V2;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToRecordSchema;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTaskBackdoor;
import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.davinci.replication.merge.RmdSerDe;
import com.linkedin.davinci.replication.merge.StringAnnotatedStoreSchemaCache;
import com.linkedin.davinci.storage.chunking.SingleGetChunkingAdapter;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.ConfigKeys;
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
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This class includes tests on partial update (Write Compute) with a setup that has both the parent and child controllers.
 */
public class PartialUpdateTest {
  private static final int NUMBER_OF_CHILD_DATACENTERS = 1;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final int TEST_TIMEOUT_MS = 120_000;
  private static final String CLUSTER_NAME = "venice-cluster0";

  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private VeniceControllerWrapper parentController;
  private List<VeniceMultiClusterWrapper> childDatacenters;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    Properties controllerProps = new Properties();
    controllerProps.put(ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, false);
    this.multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        2,
        1,
        2,
        Optional.of(new VeniceProperties(controllerProps)),
        Optional.of(new Properties(controllerProps)),
        Optional.of(new VeniceProperties(serverProperties)),
        false);
    this.childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    List<VeniceControllerWrapper> parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    if (parentControllers.size() != 1) {
      throw new IllegalStateException("Expect only one parent controller. Got: " + parentControllers.size());
    }
    this.parentController = parentControllers.get(0);
  }

  @Test
  public void testRepushWithChunkingFlagChanged() throws IOException {
    final String storeName = Utils.getUniqueString("reproduce");
    String parentControllerUrl = parentController.getControllerUrl();
    Schema keySchema = AvroCompatibilityHelper.parse(loadFileAsString("UserKey.avsc"));
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("UserValue.avsc"));
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient.createNewStore(storeName, "test_owner", keySchema.toString(), valueSchema.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setWriteComputationEnabled(true)
              .setHybridRewindSeconds(86400L)
              .setHybridOffsetLagThreshold(10L);
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

      VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
      AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()));
      SystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);
      GenericRecord keyRecord = new GenericData.Record(keySchema);
      keyRecord.put("learnerUrn", "urn:li:member:682787898");
      keyRecord.put("query", "python");
      GenericRecord checkpointKeyRecord = new GenericData.Record(keySchema);
      checkpointKeyRecord.put("learnerUrn", "urn:li:member:123");
      checkpointKeyRecord.put("query", "python");

      GenericRecord partialUpdateRecord = new UpdateBuilderImpl(writeComputeSchema)
          .setElementsToAddToListField("blockedContentsUrns", Collections.singletonList("urn:li:lyndaCourse:751323"))
          .build();
      sendStreamingRecord(veniceProducer, storeName, keyRecord, partialUpdateRecord);

      // Perform one time repush to make sure repush can handle RMD chunks data correctly.
      Properties props =
          IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, "dummyInputPath", storeName);
      props.setProperty(SOURCE_KAFKA, "true");
      props.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getKafka().getAddress());
      props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
      // intentionally stop re-consuming from RT so stale records don't affect the testing results
      // props.put(REWIND_TIME_IN_SECONDS_OVERRIDE, 0);
      TestWriteUtils.runPushJob("Run repush job 1", props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        try {
          GenericRecord value = (GenericRecord) storeReader.get(keyRecord).get();
          assertNotNull(value, "key " + keyRecord + " should not be missing!");
          LogManager.getLogger().info("DEBUGGING: " + value);
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

      // Enable chunking
      UpdateStoreQueryParams newUpdateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA).setChunkingEnabled(true);
      updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, newUpdateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      // Perform one time repush to make sure repush can handle chunks data correctly.
      // intentionally stop re-consuming from RT so stale records don't affect the testing results
      props.put(REWIND_TIME_IN_SECONDS_OVERRIDE, 0);
      TestWriteUtils.runPushJob("Run repush job 2", props);

      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 3),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        try {
          GenericRecord value = (GenericRecord) storeReader.get(keyRecord).get();
          assertNotNull(value, "key " + keyRecord + " should not be missing!");
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
      partialUpdateRecord = new UpdateBuilderImpl(writeComputeSchema)
          .setElementsToAddToListField("blockedContentsUrns", Collections.singletonList("urn:li:lyndaCourse:1"))
          .build();
      sendStreamingRecord(veniceProducer, storeName, keyRecord, partialUpdateRecord);
      partialUpdateRecord = new UpdateBuilderImpl(writeComputeSchema)
          .setElementsToAddToListField("blockedContentsUrns", Collections.singletonList("urn:li:lyndaCourse:2"))
          .build();
      sendStreamingRecord(veniceProducer, storeName, keyRecord, partialUpdateRecord);
      sendStreamingRecord(veniceProducer, storeName, checkpointKeyRecord, partialUpdateRecord);
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        try {
          GenericRecord value = (GenericRecord) storeReader.get(checkpointKeyRecord).get();
          assertNotNull(value, "key " + checkpointKeyRecord + " should not be missing!");
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        try {
          GenericRecord value = (GenericRecord) storeReader.get(keyRecord).get();
          assertNotNull(value, "key " + keyRecord + " should not be missing!");
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }
  }

  /**
   * This integration test verifies that in A/A + partial update enabled store, UPDATE on a key that was written in the
   * batch push should not throw exception, as the update logic should initialize a new RMD record for the original value
   * and apply updates on top of them.
   */
  @Test
  public void testPartialUpdateOnBatchPushedKeys() throws IOException {
    final String storeName = Utils.getUniqueString("rmdChunking");
    String parentControllerUrl = parentController.getControllerUrl();
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToRecordSchema(inputDir, true);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProperties =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);

    Schema valueSchema = AvroCompatibilityHelper.parse(valueSchemaStr);
    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema);
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);
    ReadOnlySchemaRepository schemaRepo = mock(ReadOnlySchemaRepository.class);
    when(schemaRepo.getReplicationMetadataSchema(storeName, 1, 1)).thenReturn(new RmdSchemaEntry(1, 1, rmdSchema));
    when(schemaRepo.getDerivedSchema(storeName, 1, 1)).thenReturn(new DerivedSchemaEntry(1, 1, writeComputeSchema));
    when(schemaRepo.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));

    VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl);
        AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName)
                .setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {
      assertCommand(
          parentControllerClient.createNewStore(storeName, "test_owner", keySchemaStr, valueSchema.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setWriteComputationEnabled(true)
              .setActiveActiveReplicationEnabled(true)
              .setChunkingEnabled(true)
              .setRmdChunkingEnabled(true)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      // VPJ push
      String childControllerUrl = childDatacenters.get(0).getRandomController().getControllerUrl();
      try (ControllerClient childControllerClient = new ControllerClient(CLUSTER_NAME, childControllerUrl)) {
        runVPJ(vpjProperties, 1, childControllerClient);
      }
      // Produce partial updates on batch pushed keys
      SystemProducer veniceProducer = getSamzaProducer(veniceClusterWrapper, storeName, Version.PushType.STREAM);
      for (int i = 1; i < 100; i++) {
        GenericRecord partialUpdateRecord =
            new UpdateBuilderImpl(writeComputeSchema).setNewFieldValue("firstName", "new_name_" + i).build();
        sendStreamingRecord(veniceProducer, storeName, String.valueOf(i), partialUpdateRecord);
      }

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        try {
          for (int i = 1; i < 100; i++) {
            String key = String.valueOf(i);
            GenericRecord value = readValue(storeReader, key);
            assertNotNull(value, "Key " + key + " should not be missing!");
            assertEquals(value.get("firstName").toString(), "new_name_" + key);
            assertEquals(value.get("lastName").toString(), "last_name_" + key);
          }
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }
  }

  /**
   * This integration test performs a few actions to test RMD chunking logic:
   * (1) Send a bunch of large UPDATE messages to make sure eventually the key's value + RMD size greater than 1MB and
   * thus trigger chunking / RMD chunking.
   * (2) Run a KIF repush to make sure it handles RMD chunks correctly.
   * (3) Send a DELETE message to partially delete some of the items in the map field.
   * (4) Send a DELETE message to fully delete the record.
   */
  @Test(timeOut = TEST_TIMEOUT_MS * 4)
  public void testReplicationMetadataChunkingE2E() throws IOException {
    final String storeName = Utils.getUniqueString("rmdChunking");
    String parentControllerUrl = parentController.getControllerUrl();
    String keySchemaStr = "{\"type\" : \"string\"}";
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("CollectionRecordV1.avsc"));
    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema);
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);
    ReadOnlySchemaRepository schemaRepo = mock(ReadOnlySchemaRepository.class);
    when(schemaRepo.getReplicationMetadataSchema(storeName, 1, 1)).thenReturn(new RmdSchemaEntry(1, 1, rmdSchema));
    when(schemaRepo.getDerivedSchema(storeName, 1, 1)).thenReturn(new DerivedSchemaEntry(1, 1, writeComputeSchema));
    when(schemaRepo.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));
    StringAnnotatedStoreSchemaCache stringAnnotatedStoreSchemaCache =
        new StringAnnotatedStoreSchemaCache(storeName, schemaRepo);
    RmdSerDe rmdSerDe = new RmdSerDe(stringAnnotatedStoreSchemaCache, 1);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient.createNewStore(storeName, "test_owner", keySchemaStr, valueSchema.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setWriteComputationEnabled(true)
              .setActiveActiveReplicationEnabled(true)
              .setChunkingEnabled(true)
              .setRmdChunkingEnabled(true)
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
      assertTrue(parentControllerClient.getStore(storeName).getStore().isRmdChunkingEnabled());
      assertTrue(parentControllerClient.getStore(storeName).getStore().getVersion(1).get().isRmdChunkingEnabled());
    }

    VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    SystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);

    String key = "key1";
    String primitiveFieldName = "name";
    String listFieldName = "intArray";
    String mapFieldName = "stringMap";

    // Insert large amount of Map entries to trigger RMD chunking.
    int updateCount = 30;
    int singleUpdateEntryCount = 10000;
    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      Map<String, String> newEntries = new HashMap<>();
      for (int i = 0; i < updateCount; i++) {
        UpdateBuilder updateBuilder = new UpdateBuilderImpl(writeComputeSchema);
        updateBuilder.setNewFieldValue(primitiveFieldName, "Tottenham");
        newEntries.clear();
        for (int j = 0; j < singleUpdateEntryCount; j++) {
          String idx = String.valueOf(i * singleUpdateEntryCount + j);
          newEntries.put("key_" + idx, "value_" + idx);
        }
        updateBuilder.setEntriesToAddToMapField(mapFieldName, newEntries);
        GenericRecord partialUpdateRecord = updateBuilder.build();
        sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord, i * 10L + 1);
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
          assertEquals(mapFieldResult.size(), updateCount * singleUpdateEntryCount);
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
      // Validate RMD bytes after PUT requests.
      String kafkaTopic = Version.composeKafkaTopic(storeName, 1);
      validateRmdData(rmdSerDe, kafkaTopic, key, rmdWithValueSchemaId -> {
        GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get("timestamp");
        GenericRecord stringMapTimestampRecord = (GenericRecord) timestampRecord.get("stringMap");
        List<Long> activeElementsTimestamps = (List<Long>) stringMapTimestampRecord.get("activeElementsTimestamps");
        assertEquals(activeElementsTimestamps.size(), updateCount * singleUpdateEntryCount);
      });

      // Perform one time repush to make sure repush can handle RMD chunks data correctly.
      Properties props =
          IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, "dummyInputPath", storeName);
      props.setProperty(SOURCE_KAFKA, "true");
      props.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getKafka().getAddress());
      props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
      // intentionally stop re-consuming from RT so stale records don't affect the testing results
      props.put(REWIND_TIME_IN_SECONDS_OVERRIDE, 0);
      TestWriteUtils.runPushJob("Run repush job", props);

      ControllerClient controllerClient =
          new ControllerClient("venice-cluster0", childDatacenters.get(0).getControllerConnectString());
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          () -> Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 2));
      veniceCluster.refreshAllRouterMetaData();

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

      // Validate RMD bytes after PUT requests.
      kafkaTopic = Version.composeKafkaTopic(storeName, 2);
      validateRmdData(rmdSerDe, kafkaTopic, key, rmdWithValueSchemaId -> {
        GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get("timestamp");
        GenericRecord stringMapTimestampRecord = (GenericRecord) timestampRecord.get("stringMap");
        List<Long> activeElementsTimestamps = (List<Long>) stringMapTimestampRecord.get("activeElementsTimestamps");
        assertEquals(activeElementsTimestamps.size(), updateCount * singleUpdateEntryCount);
      });

      // Send DELETE record that partially removes data.
      sendStreamingDeleteRecord(veniceProducer, storeName, key, (updateCount - 1) * 10L);

      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        GenericRecord valueRecord = readValue(storeReader, key);
        boolean nullRecord = (valueRecord == null);
        assertFalse(nullRecord);

        Map<String, String> mapFieldResult = new HashMap<>();
        ((Map<Utf8, Utf8>) valueRecord.get(mapFieldName))
            .forEach((x, y) -> mapFieldResult.put(x.toString(), y.toString()));
        assertEquals(mapFieldResult.size(), singleUpdateEntryCount);
      });

      validateRmdData(rmdSerDe, kafkaTopic, key, rmdWithValueSchemaId -> {
        GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get("timestamp");
        GenericRecord stringMapTimestampRecord = (GenericRecord) timestampRecord.get("stringMap");
        List<Long> activeElementsTimestamps = (List<Long>) stringMapTimestampRecord.get("activeElementsTimestamps");
        assertEquals(activeElementsTimestamps.size(), singleUpdateEntryCount);
        List<Long> deletedElementsTimestamps = (List<Long>) stringMapTimestampRecord.get("deletedElementsTimestamps");
        assertEquals(deletedElementsTimestamps.size(), 0);
      });

      // Send DELETE record that fully removes data.
      sendStreamingDeleteRecord(veniceProducer, storeName, key, updateCount * 10L);
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        GenericRecord valueRecord = readValue(storeReader, key);
        boolean nullRecord = (valueRecord == null);
        assertTrue(nullRecord);
      });
      validateRmdData(rmdSerDe, kafkaTopic, key, rmdWithValueSchemaId -> {
        Assert.assertTrue(
            rmdWithValueSchemaId.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME) instanceof GenericRecord);
        GenericRecord timestampRecord =
            (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME);
        GenericRecord stringMapTimestampRecord = (GenericRecord) timestampRecord.get("stringMap");
        assertEquals(stringMapTimestampRecord.get(TOP_LEVEL_TS_FIELD_NAME), (long) (updateCount) * 10);
      });
    } finally {
      veniceProducer.stop();
    }
  }

  private void validateRmdData(
      RmdSerDe rmdSerDe,
      String kafkaTopic,
      String key,
      Consumer<RmdWithValueSchemaId> rmdDataValidationFlow) {
    for (VeniceServerWrapper serverWrapper: multiRegionMultiClusterWrapper.getChildRegions()
        .get(0)
        .getClusters()
        .get("venice-cluster0")
        .getVeniceServers()) {
      AbstractStorageEngine storageEngine =
          serverWrapper.getVeniceServer().getStorageService().getStorageEngine(kafkaTopic);
      assertNotNull(storageEngine);
      ValueRecord result = SingleGetChunkingAdapter
          .getReplicationMetadata(storageEngine, 0, serializeStringKeyToByteArray(key), true, null);
      // Avoid assertion failure logging massive RMD record.
      boolean nullRmd = (result == null);
      assertFalse(nullRmd);
      byte[] value = result.serialize();
      RmdWithValueSchemaId rmdWithValueSchemaId = rmdSerDe.deserializeValueSchemaIdPrependedRmdBytes(value);
      rmdDataValidationFlow.accept(rmdWithValueSchemaId);
    }
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
  public void testUpdateWithSupersetSchema() throws IOException {
    final String storeName = Utils.getUniqueString("store");
    String parentControllerUrl = parentController.getControllerUrl();
    String keySchemaStr = "{\"type\" : \"string\"}";
    Schema valueSchemaV1 = AvroCompatibilityHelper.parse(loadFileAsString("writecompute/test/PersonV1.avsc"));
    Schema valueSchemaV2 = AvroCompatibilityHelper.parse(loadFileAsString("writecompute/test/PersonV2.avsc"));
    String valueFieldName = "name";

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient.createNewStore(storeName, "test_owner", keySchemaStr, valueSchemaV1.toString()));

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
    VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);

    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {

      // Step 1. Put a value record.
      veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);
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

  private GenericRecord readValue(AvroGenericStoreClient<Object, Object> storeReader, String key)
      throws ExecutionException, InterruptedException {
    return (GenericRecord) storeReader.get(key).get();
  }

  @Test(timeOut = 120
      * Time.MS_PER_SECOND, dataProvider = "Boolean-Compression", dataProviderClass = DataProviderUtils.class)
  public void testWriteComputeWithHybridLeaderFollowerLargeRecord(
      boolean writeComputeFromCache,
      CompressionStrategy compressionStrategy) throws Exception {

    SystemProducer veniceProducer = null;

    try {
      long streamingRewindSeconds = 10L;
      long streamingMessageLag = 2L;

      String storeName = Utils.getUniqueString("write-compute-store");
      File inputDir = getTempDataDirectory();
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      String parentControllerURL = parentController.getControllerUrl();
      // Records 1-100, id string to name record
      Schema recordSchema = writeSimpleAvroFileWithStringToRecordSchema(inputDir, true);
      VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
      Properties vpjProperties =
          IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
      try (ControllerClient controllerClient = new ControllerClient(CLUSTER_NAME, parentControllerURL);
          AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
              ClientConfig.defaultGenericClientConfig(storeName)
                  .setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {

        String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
        String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
        assertCommand(controllerClient.createNewStore(storeName, "test_owner", keySchemaStr, valueSchemaStr));

        ControllerResponse response = controllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(streamingRewindSeconds)
                .setHybridOffsetLagThreshold(streamingMessageLag)
                .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
                .setChunkingEnabled(true)
                .setCompressionStrategy(compressionStrategy)
                .setWriteComputationEnabled(true)
                .setHybridRewindSeconds(10L)
                .setHybridOffsetLagThreshold(2L));

        assertFalse(response.isError());

        // Add a new value schema v2 to store
        SchemaResponse schemaResponse = controllerClient.addValueSchema(storeName, NESTED_SCHEMA_STRING_V2);
        assertFalse(schemaResponse.isError());

        // Add WC (Write Compute) schema associated to v2.
        // Note that Write Compute schema needs to be registered manually here because the integration test harness
        // does not create any parent controller. In production, when a value schema is added to a WC-enabled store via
        // a parent controller, it will automatically generate and register its WC schema.
        Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance()
            .convertFromValueRecordSchema(AvroCompatibilityHelper.parse(NESTED_SCHEMA_STRING_V2));
        schemaResponse =
            controllerClient.addDerivedSchema(storeName, schemaResponse.getId(), writeComputeSchema.toString());
        assertFalse(schemaResponse.isError());

        // VPJ push
        String childControllerUrl = childDatacenters.get(0).getRandomController().getControllerUrl();
        try (ControllerClient childControllerClient = new ControllerClient(CLUSTER_NAME, childControllerUrl)) {
          runVPJ(vpjProperties, 1, childControllerClient);
        }

        // Verify records (note, records 1-100 have been pushed)
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          try {
            for (int i = 1; i < 100; i++) {
              String key = String.valueOf(i);
              GenericRecord value = readValue(storeReader, key);
              assertNotNull(value, "Key " + key + " should not be missing!");
              assertEquals(value.get("firstName").toString(), "first_name_" + key);
              assertEquals(value.get("lastName").toString(), "last_name_" + key);
              assertEquals(value.get("age"), -1);
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // disable the purging of transientRecord buffer using reflection.
        if (writeComputeFromCache) {
          String topicName = Version.composeKafkaTopic(storeName, 1);
          for (VeniceServerWrapper veniceServerWrapper: veniceClusterWrapper.getVeniceServers()) {
            StoreIngestionTaskBackdoor.setPurgeTransientRecordBuffer(veniceServerWrapper, topicName, false);
          }
        }

        // Do not send large record to RT; RT doesn't support chunking
        veniceProducer = getSamzaProducer(veniceClusterWrapper, storeName, Version.PushType.STREAM);
        String key = String.valueOf(101);
        Schema valueSchema = AvroCompatibilityHelper.parse(NESTED_SCHEMA_STRING);
        GenericRecord value = new GenericData.Record(valueSchema);
        char[] chars = new char[100];
        Arrays.fill(chars, 'f');
        String firstName = new String(chars);
        Arrays.fill(chars, 'l');
        String lastName = new String(chars);
        value.put("firstName", firstName);
        value.put("lastName", lastName);
        sendStreamingRecord(veniceProducer, storeName, key, value);

        // Verify the streaming record
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          try {
            GenericRecord retrievedValue = readValue(storeReader, key);
            assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
            assertEquals(retrievedValue.get("firstName").toString(), firstName);
            assertEquals(retrievedValue.get("lastName").toString(), lastName);
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // Update the record
        Arrays.fill(chars, 'u');
        String updatedFirstName = new String(chars);
        final int updatedAge = 1;
        UpdateBuilder updateBuilder = new UpdateBuilderImpl(writeComputeSchema);
        updateBuilder.setNewFieldValue("firstName", updatedFirstName);
        updateBuilder.setNewFieldValue("age", updatedAge);
        GenericRecord partialUpdateRecord = updateBuilder.build();

        sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord);
        // Verify the update
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          try {
            GenericRecord retrievedValue = readValue(storeReader, key);
            assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
            assertEquals(retrievedValue.get("firstName").toString(), updatedFirstName);
            assertEquals(retrievedValue.get("lastName").toString(), lastName);
            assertEquals(retrievedValue.get("age"), updatedAge);
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // Update the record again
        Arrays.fill(chars, 'v');
        String updatedFirstName1 = new String(chars);

        updateBuilder = new UpdateBuilderImpl(writeComputeSchema);
        updateBuilder.setNewFieldValue("firstName", updatedFirstName1);
        GenericRecord partialUpdateRecord1 = updateBuilder.build();
        sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord1);
        // Verify the update
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
          try {
            GenericRecord retrievedValue = readValue(storeReader, key);
            assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
            assertEquals(retrievedValue.get("firstName").toString(), updatedFirstName1);
            assertEquals(retrievedValue.get("lastName").toString(), lastName);
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // Delete the record
        sendStreamingRecord(veniceProducer, storeName, key, null);
        // Verify the delete
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          try {
            GenericRecord retrievedValue = readValue(storeReader, key);
            assertNull(retrievedValue, "Key " + key + " should be missing!");
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // Update the record again
        Arrays.fill(chars, 'w');
        String updatedFirstName2 = new String(chars);
        Arrays.fill(chars, 'g');
        String updatedLastName = new String(chars);

        updateBuilder = new UpdateBuilderImpl(writeComputeSchema);
        updateBuilder.setNewFieldValue("firstName", updatedFirstName2);
        updateBuilder.setNewFieldValue("lastName", updatedLastName);
        updateBuilder.setNewFieldValue("age", 2);
        GenericRecord partialUpdateRecord2 = updateBuilder.build();

        sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord2);
        // Verify the update
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          try {
            GenericRecord retrievedValue = readValue(storeReader, key);
            assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
            assertEquals(retrievedValue.get("firstName").toString(), updatedFirstName2);
            assertEquals(retrievedValue.get("lastName").toString(), updatedLastName);
            assertEquals(retrievedValue.get("age"), 2);
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // Update the record again
        Arrays.fill(chars, 'x');
        String updatedFirstName3 = new String(chars);

        updateBuilder = new UpdateBuilderImpl(writeComputeSchema);
        updateBuilder.setNewFieldValue("firstName", updatedFirstName3);
        GenericRecord partialUpdateRecord3 = updateBuilder.build();
        sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord3);
        // Verify the update
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          try {
            GenericRecord retrievedValue = readValue(storeReader, key);
            assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
            assertEquals(retrievedValue.get("firstName").toString(), updatedFirstName3);
            assertEquals(retrievedValue.get("lastName").toString(), updatedLastName);
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

      }
    } finally {
      if (veniceProducer != null) {
        veniceProducer.stop();
      }
    }
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testWriteComputeWithSamzaBatchJob() throws Exception {

    SystemProducer veniceProducer = null;
    long streamingRewindSeconds = 10L;
    long streamingMessageLag = 2L;

    String storeName = Utils.getUniqueString("write-compute-store");
    File inputDir = getTempDataDirectory();
    String parentControllerURL = parentController.getControllerUrl();
    // Records 1-100, id string to name record
    Schema recordSchema = writeSimpleAvroFileWithStringToRecordSchema(inputDir, true);
    VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    try (ControllerClient controllerClient = new ControllerClient(CLUSTER_NAME, parentControllerURL);
        AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName)
                .setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {

      String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
      String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
      assertCommand(controllerClient.createNewStore(storeName, "test_owner", keySchemaStr, valueSchemaStr));

      ControllerResponse response = controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setHybridRewindSeconds(streamingRewindSeconds)
              .setHybridOffsetLagThreshold(streamingMessageLag)
              .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setWriteComputationEnabled(true)
              .setChunkingEnabled(true)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L));

      assertFalse(response.isError());

      // Add a new value schema v2 to store
      SchemaResponse schemaResponse = controllerClient.addValueSchema(storeName, NESTED_SCHEMA_STRING_V2);
      assertFalse(schemaResponse.isError());

      // Add WC (Write Compute) schema associated to v2.
      // (this is a test environment only needed step since theres no parent)
      Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance()
          .convertFromValueRecordSchema(AvroCompatibilityHelper.parse(NESTED_SCHEMA_STRING_V2));
      schemaResponse =
          controllerClient.addDerivedSchema(storeName, schemaResponse.getId(), writeComputeSchema.toString());
      assertFalse(schemaResponse.isError());

      // Run empty push to create a version and get everything created
      controllerClient.sendEmptyPushAndWait(storeName, "foopush", 10000, 60 * Time.MS_PER_SECOND);

      VeniceSystemFactory factory = new VeniceSystemFactory();
      Version.PushType pushType = Version.PushType.BATCH;
      Map<String, String> samzaConfig = getSamzaProducerConfig(veniceClusterWrapper, storeName, pushType);
      // final boolean veniceAggregate = config.getBoolean(prefix + VENICE_AGGREGATE, false);
      samzaConfig.put("systems.venice." + VENICE_AGGREGATE, "true");
      samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, multiRegionMultiClusterWrapper.getZkServerWrapper().getAddress());
      samzaConfig.put(VENICE_PARENT_CONTROLLER_D2_SERVICE, PARENT_D2_SERVICE_NAME);
      samzaConfig.put(DEPLOYMENT_ID, Utils.getUniqueString("venice-push-id"));
      veniceProducer = factory.getProducer("venice", new MapConfig(samzaConfig), null);
      veniceProducer.start();

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

      for (int i = 0; i < 10; i++) {
        String key = String.valueOf(i);
        sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord);
      }

      // send end of push
      controllerClient.writeEndOfPush(storeName, 2);

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
    } finally {
      if (veniceProducer != null) {
        veniceProducer.stop();
      }
    }
  }

  /**
   * Blocking, waits for new version to go online
   */
  private void runVPJ(Properties vpjProperties, int expectedVersionNumber, ControllerClient controllerClient) {
    String jobName = Utils.getUniqueString("write-compute-job-" + expectedVersionNumber);
    try (VenicePushJob job = new VenicePushJob(jobName, vpjProperties)) {
      job.run();
      TestUtils.waitForNonDeterministicCompletion(
          60,
          TimeUnit.SECONDS,
          () -> controllerClient.getStore((String) vpjProperties.get(VenicePushJob.VENICE_STORE_NAME_PROP))
              .getStore()
              .getCurrentVersion() == expectedVersionNumber);
    }
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  private byte[] serializeStringKeyToByteArray(String key) {
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
