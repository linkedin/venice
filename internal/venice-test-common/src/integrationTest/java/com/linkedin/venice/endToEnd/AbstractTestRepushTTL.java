package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.ACTIVE_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingDeleteRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_START_TIMESTAMP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REWIND_TIME_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_REPUSH_SOURCE_PUBSUB_BROKER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.replication.merge.RmdSerDe;
import com.linkedin.davinci.replication.merge.StringAnnotatedStoreSchemaCache;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Abstract base class for TTL repush integration tests.
 * Concrete subclasses select the engine (MR vs Spark) via {@link #useSpark()}.
 */
public abstract class AbstractTestRepushTTL extends AbstractTestRepush {
  /**
   * Subclasses return {@code true} to run with Spark, {@code false} for MR.
   */
  protected abstract boolean useSpark();

  @Test(timeOut = TEST_TIMEOUT_MS, dataProvider = "Compression-Strategies", dataProviderClass = DataProviderUtils.class)
  public void testRepushWithTTLWithActiveActivePartialUpdateStore(CompressionStrategy compressionStrategy) {
    final String storeName = Utils.getUniqueString("ttlRepushAAWC");
    String parentControllerUrl = getParentControllerUrl();
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("CollectionRecordV1.avsc"));
    Schema partialUpdateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema);
    ReadOnlySchemaRepository schemaRepo = mock(ReadOnlySchemaRepository.class);
    when(schemaRepo.getReplicationMetadataSchema(storeName, 1, 1)).thenReturn(new RmdSchemaEntry(1, 1, rmdSchema));
    when(schemaRepo.getDerivedSchema(storeName, 1, 1)).thenReturn(new DerivedSchemaEntry(1, 1, partialUpdateSchema));
    when(schemaRepo.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));
    StringAnnotatedStoreSchemaCache stringAnnotatedStoreSchemaCache =
        new StringAnnotatedStoreSchemaCache(storeName, schemaRepo);
    RmdSerDe rmdSerDe = new RmdSerDe(stringAnnotatedStoreSchemaCache, 1);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", STRING_SCHEMA.toString(), valueSchema.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setPartitionCount(1)
              .setCompressionStrategy(compressionStrategy)
              .setWriteComputationEnabled(true)
              .setActiveActiveReplicationEnabled(true)
              .setChunkingEnabled(true)
              .setRmdChunkingEnabled(true)
              .setHybridRewindSeconds(1L)
              .setHybridOffsetLagThreshold(1L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          60,
          TimeUnit.SECONDS);
    }

    VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);

    long STALE_TS = 99999L;
    long FRESH_TS = 100000L;
    String STRING_MAP_FILED = "stringMap";
    String REGULAR_FIELD = "name";
    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";
    String key4 = "key4";
    String key5 = "key5";

    try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM)) {
      /**
       * Case 1: The record is partially stale, TTL repush should only keep the part that's fresh.
       */

      // This update is expected to be carried into TTL repush.
      UpdateBuilder updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
      updateBuilder.setEntriesToAddToMapField(STRING_MAP_FILED, Collections.singletonMap("k1", "v1"));
      sendStreamingRecord(veniceProducer, storeName, key1, updateBuilder.build(), FRESH_TS);
      // This update is expected to be WIPED OUT after TTL repush.
      updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
      updateBuilder.setNewFieldValue(REGULAR_FIELD, "new_name");
      updateBuilder.setEntriesToAddToMapField(STRING_MAP_FILED, Collections.singletonMap("k2", "v2"));
      sendStreamingRecord(veniceProducer, storeName, key1, updateBuilder.build(), STALE_TS);

      /**
       * Case 2: The record is fully stale, TTL repush should drop the record.
       */

      // This update is expected to be WIPED OUT after TTL repush.
      updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
      updateBuilder.setNewFieldValue(REGULAR_FIELD, "new_name_2");
      sendStreamingRecord(veniceProducer, storeName, key2, updateBuilder.build(), STALE_TS);

      /**
       * Case 3: The record is fully fresh, TTL repush should keep the record.
       */

      // This update is expected to be carried into TTL repush.
      updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
      updateBuilder.setNewFieldValue(REGULAR_FIELD, "new_name_3");
      sendStreamingRecord(veniceProducer, storeName, key3, updateBuilder.build(), FRESH_TS);

      /**
       * Case 4: The delete record is fully fresh, TTL repush should NOT FAIL due to this record.
       */

      sendStreamingRecord(veniceProducer, storeName, key4, updateBuilder.build(), FRESH_TS);
      sendStreamingDeleteRecord(veniceProducer, storeName, key4, FRESH_TS + 1);

      /**
       * Case 5: The delete record is stale, TTL repush should keep new update.
       */

      sendStreamingDeleteRecord(veniceProducer, storeName, key5, STALE_TS);
      updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
      updateBuilder.setNewFieldValue(REGULAR_FIELD, "new_name_5");
      sendStreamingRecord(veniceProducer, storeName, key5, updateBuilder.build(), FRESH_TS);

    }

    /**
     * Validate the data is ready in storage before TTL repush.
     */
    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        try {
          GenericRecord valueRecord = readValue(storeReader, key1);
          assertNotNull(valueRecord);
          assertEquals(valueRecord.get(REGULAR_FIELD), new Utf8("new_name"));
          assertNotNull(valueRecord.get(STRING_MAP_FILED));
          Map<Utf8, Utf8> stringMapValue = (Map<Utf8, Utf8>) valueRecord.get(STRING_MAP_FILED);
          assertEquals(stringMapValue.get(new Utf8("k1")), new Utf8("v1"));
          assertEquals(stringMapValue.get(new Utf8("k2")), new Utf8("v2"));

          valueRecord = readValue(storeReader, key2);
          assertNotNull(valueRecord);
          assertEquals(valueRecord.get(REGULAR_FIELD), new Utf8("new_name_2"));

          valueRecord = readValue(storeReader, key3);
          assertNotNull(valueRecord);
          assertEquals(valueRecord.get(REGULAR_FIELD), new Utf8("new_name_3"));

          valueRecord = readValue(storeReader, key4);
          assertNull(valueRecord);

          valueRecord = readValue(storeReader, key5);
          assertNotNull(valueRecord);
          assertEquals(valueRecord.get(REGULAR_FIELD), new Utf8("new_name_5"));
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }

    String kafkaTopic_v1 = Version.composeKafkaTopic(storeName, 1);
    // Validate RMD bytes after initial update requests.
    validateRmdData(rmdSerDe, kafkaTopic_v1, key1, rmdWithValueSchemaId -> {
      GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
      GenericRecord stringMapTsRecord = (GenericRecord) timestampRecord.get(STRING_MAP_FILED);
      Assert.assertEquals(stringMapTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 0L);
      Assert.assertEquals(stringMapTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Arrays.asList(STALE_TS, FRESH_TS));
      Assert.assertEquals(timestampRecord.get(REGULAR_FIELD), STALE_TS);
    });

    validateRmdData(rmdSerDe, kafkaTopic_v1, key2, rmdWithValueSchemaId -> {
      GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
      Assert.assertEquals(timestampRecord.get(REGULAR_FIELD), STALE_TS);
    });

    validateRmdData(rmdSerDe, kafkaTopic_v1, key3, rmdWithValueSchemaId -> {
      GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
      Assert.assertEquals(timestampRecord.get(REGULAR_FIELD), FRESH_TS);
    });

    // Perform one time repush to make sure repush can handle RMD chunks data correctly.
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, "dummyInputPath", storeName);
    props.setProperty(SOURCE_KAFKA, "true");
    props.setProperty(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, veniceCluster.getPubSubBrokerWrapper().getAddress());
    props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    props.setProperty(REPUSH_TTL_ENABLE, "true");
    // Override the TTL repush start TS to work with logical TS setup.
    props.setProperty(REPUSH_TTL_START_TIMESTAMP, String.valueOf(FRESH_TS));
    // Override the rewind time to make sure not to consume 24hrs data from RT topic.
    props.put(REWIND_TIME_IN_SECONDS_OVERRIDE, 0);
    if (useSpark()) {
      props.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    }
    IntegrationTestPushUtils.runVPJ(props);
    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
    }

    /**
     * Validate the data is ready in storage after TTL repush.
     */
    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        try {
          // Key 1 is partially preserved.
          GenericRecord valueRecord = readValue(storeReader, key1);
          assertNotNull(valueRecord);
          assertEquals(valueRecord.get(REGULAR_FIELD), new Utf8("default_name"));
          assertNotNull(valueRecord.get(STRING_MAP_FILED));
          Map<Utf8, Utf8> stringMapValue = (Map<Utf8, Utf8>) valueRecord.get(STRING_MAP_FILED);
          assertEquals(stringMapValue.get(new Utf8("k1")), new Utf8("v1"));
          assertNull(stringMapValue.get(new Utf8("k2")));

          // Key 2 is fully removed.
          valueRecord = readValue(storeReader, key2);
          assertNull(valueRecord);

          // Key 3 is fully preserved.
          valueRecord = readValue(storeReader, key3);
          assertNotNull(valueRecord);
          assertEquals(valueRecord.get(REGULAR_FIELD), new Utf8("new_name_3"));

          valueRecord = readValue(storeReader, key4);
          assertNull(valueRecord);

          valueRecord = readValue(storeReader, key5);
          assertNotNull(valueRecord);
          assertEquals(valueRecord.get(REGULAR_FIELD), new Utf8("new_name_5"));
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }

    String kafkaTopic_v2 = Version.composeKafkaTopic(storeName, 2);
    // Validate RMD bytes after TTL repush.
    validateRmdData(rmdSerDe, kafkaTopic_v2, key1, rmdWithValueSchemaId -> {
      GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
      GenericRecord stringMapTsRecord = (GenericRecord) timestampRecord.get(STRING_MAP_FILED);
      Assert.assertEquals(stringMapTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), STALE_TS);
      Assert.assertEquals(stringMapTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.singletonList(FRESH_TS));
      Assert.assertEquals(timestampRecord.get(REGULAR_FIELD), STALE_TS);
    });
    validateRmdData(rmdSerDe, kafkaTopic_v1, key2, rmdWithValueSchemaId -> {
      GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
      Assert.assertEquals(timestampRecord.get(REGULAR_FIELD), STALE_TS);
    });

    validateRmdData(rmdSerDe, kafkaTopic_v1, key3, rmdWithValueSchemaId -> {
      GenericRecord timestampRecord = (GenericRecord) rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_NAME);
      Assert.assertEquals(timestampRecord.get(REGULAR_FIELD), FRESH_TS);
    });
  }
}
