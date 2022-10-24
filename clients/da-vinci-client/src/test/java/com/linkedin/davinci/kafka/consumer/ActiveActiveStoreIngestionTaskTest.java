package com.linkedin.davinci.kafka.consumer;

import static org.mockito.AdditionalMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.davinci.replication.merge.RmdSerDe;
import com.linkedin.davinci.stats.AggHostLevelIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ActiveActiveStoreIngestionTaskTest {
  /**
   * This test verifies that the IngestionReplicationMetadataLookUpLatency metric is always emitted regardless of
   * traffic. This is useful in setting up health rules against this metric.
   */
  private static final String VALUE_SCHEMA_STR = "{" + "   \"type\" : \"record\","
      + "   \"namespace\" : \"com.linkedin.avro\"," + "   \"name\" : \"Person\"," + "   \"fields\" : ["
      + "      { \"name\" : \"Name\" , \"type\" : \"string\", \"default\" : \"unknown\" },"
      + "      { \"name\" : \"Age\" , \"type\" : \"int\", \"default\" : -1 },"
      + "      { \"name\" : \"Items\" , \"type\" : {\"type\" : \"array\", \"items\" : \"string\"}, \"default\" : [] },"
      + "      { \"name\" : \"PetNameToAge\" , \"type\" : [\"null\" , {\"type\" : \"map\", \"values\" : \"int\"}], \"default\" : null }"
      + "   ]" + "}";
  private ActiveActiveStoreIngestionTask taskUnderTest;
  private PartitionConsumptionState pcsMock;
  private HostLevelIngestionStats hostLevelIngestionStats;
  private AbstractStorageEngine storageEngineMock;
  private ReadOnlySchemaRepository schemaRepository;
  private final byte[] keyBytes = "key".getBytes();

  private void setupMocksPre() {
    storageEngineMock = mock(AbstractStorageEngine.class);
    schemaRepository = mock(ReadOnlySchemaRepository.class);
    pcsMock = mock(PartitionConsumptionState.class);
  }

  private void setupMocks() {
    StorageEngineRepository storageEngineRepositoryMock = mock(StorageEngineRepository.class);
    when(storageEngineRepositoryMock.getLocalStorageEngine(any())).thenReturn(storageEngineMock);
    StoreIngestionTaskFactory.Builder builderMock = mock(StoreIngestionTaskFactory.Builder.class, RETURNS_DEEP_STUBS);
    when(builderMock.getSchemaRepo()).thenReturn(schemaRepository);
    when(builderMock.getStorageEngineRepository()).thenReturn(storageEngineRepositoryMock);
    AggHostLevelIngestionStats ingestionStatsMock = mock(AggHostLevelIngestionStats.class);
    hostLevelIngestionStats = mock(HostLevelIngestionStats.class);
    when(ingestionStatsMock.getStoreStats(any())).thenReturn(hostLevelIngestionStats);
    when(builderMock.getIngestionStats()).thenReturn(ingestionStatsMock);
    VeniceStoreVersionConfig storeConfigMock = mock(VeniceStoreVersionConfig.class);
    when(storeConfigMock.getStoreVersionName()).thenReturn("test_store_name_v1");
    Version versionMock = mock(Version.class, RETURNS_DEFAULTS);
    when(versionMock.getPartitionCount()).thenReturn(2);
    Properties kafkaProps = new Properties();
    taskUnderTest = new ActiveActiveStoreIngestionTask(
        builderMock,
        mock(Store.class),
        versionMock,
        kafkaProps,
        () -> false,
        storeConfigMock,
        0,
        false,
        null,
        null);
  }

  @Test
  public void testIngestionReplicationMetadataLookUpLatencyCached() {
    setupMocksPre();
    // return from cache
    when(pcsMock.getTransientRecord(any())).thenReturn(mock(PartitionConsumptionState.TransientRecord.class));
    setupMocks();

    Optional<RmdWithValueSchemaId> replicationMetadataAndSchemaId =
        taskUnderTest.getReplicationMetadataAndSchemaId(pcsMock, keyBytes, 0);
    Assert.assertTrue(replicationMetadataAndSchemaId.isPresent());

    verify(pcsMock).getTransientRecord(keyBytes);
    verify(hostLevelIngestionStats).recordIngestionReplicationMetadataLookUpLatency(0.0);
  }

  @Test
  public void testIngestionReplicationMetadataLookUpLatencyLookup() {
    setupMocksPre();
    // No cache , generated rmd
    when(pcsMock.getTransientRecord(any())).thenReturn(null);
    final int valueSchemaID = 1234;
    final int rmdVersionID = 0;
    final String storeName = "test_store_name";
    // Generate RMD schema and record from value schema.
    Schema valueSchema = AvroCompatibilityHelper.parse(VALUE_SCHEMA_STR);
    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema);
    GenericRecord rmd = createRmdWithCollectionTimestamp(rmdSchema);
    when(schemaRepository.getReplicationMetadataSchema(storeName, valueSchemaID, rmdVersionID))
        .thenReturn(new RmdSchemaEntry(valueSchemaID, rmdVersionID, rmdSchema));
    RmdSerDe rmdSerDe = new RmdSerDe(schemaRepository, storeName, rmdVersionID);
    ByteBuffer rmdSer = ByteBuffer.allocate(100);
    rmdSer.putInt(valueSchemaID);
    ByteBuffer rmdSer2 = rmdSerDe.serializeRmdRecord(valueSchemaID, rmd);
    rmdSer.put(rmdSer2.array());
    when(storageEngineMock.getReplicationMetadata(0, keyBytes)).thenReturn(rmdSer.array());
    setupMocks();

    Optional<RmdWithValueSchemaId> replicationMetadataAndSchemaId =
        taskUnderTest.getReplicationMetadataAndSchemaId(pcsMock, keyBytes, 0);

    Assert.assertTrue(replicationMetadataAndSchemaId.isPresent());
    verify(pcsMock).getTransientRecord(keyBytes);
    // Latency should be nonzero
    verify(hostLevelIngestionStats).recordIngestionReplicationMetadataLookUpLatency(gt(0.0d));
  }

  @Test
  public void testIngestionReplicationMetadataLookUpLatencyLookupNoRmd() {
    setupMocksPre();
    // No cache, no rmd returned from lookup
    when(pcsMock.getTransientRecord(any())).thenReturn(null);
    when(storageEngineMock.getReplicationMetadata(1, keyBytes)).thenReturn(null);
    setupMocks();

    Optional<RmdWithValueSchemaId> replicationMetadataAndSchemaId =
        taskUnderTest.getReplicationMetadataAndSchemaId(pcsMock, keyBytes, 0);

    Assert.assertFalse(replicationMetadataAndSchemaId.isPresent());
    verify(pcsMock).getTransientRecord(keyBytes);
    // Latency should be nonzero
    verify(hostLevelIngestionStats).recordIngestionReplicationMetadataLookUpLatency(gt(0.0d));
  }

  private GenericRecord createRmdWithCollectionTimestamp(Schema rmdSchema) {
    Schema rmdTimestampSchema = rmdSchema.getField("timestamp").schema().getTypes().get(1);
    GenericRecord rmdTimestamp = new GenericData.Record(rmdTimestampSchema);
    rmdTimestamp.put("Name", 1L);
    rmdTimestamp.put("Age", 1L);
    rmdTimestamp.put(
        "Items",
        createCollectionFieldMetadataRecord(
            rmdTimestampSchema.getField("Items").schema(),
            23L,
            1,
            3,
            Arrays.asList(1L, 2L, 3L),
            Arrays.asList("foo", "bar"),
            Arrays.asList(1L, 100L)));
    rmdTimestamp.put(
        "PetNameToAge",
        createCollectionFieldMetadataRecord(
            rmdTimestampSchema.getField("PetNameToAge").schema(),
            24L,
            2,
            5,
            Arrays.asList(1L, 2L, 3L, 4L, 5L),
            Arrays.asList("foo", "bar", "qaz"),
            Arrays.asList(1L, 2L, 3L)));
    GenericRecord rmd = new GenericData.Record(rmdSchema);
    rmd.put("timestamp", rmdTimestamp);
    rmd.put("replication_checkpoint_vector", Arrays.asList(1L, 2L, 3L));
    return rmd;
  }

  private GenericRecord createCollectionFieldMetadataRecord(
      Schema collectionFieldMetadataSchema,
      long topLevelTimestamp,
      int topLevelColoID,
      int putOnlyPartLen,
      List<Long> activeElementsTimestamps,
      List<Object> deletedElements,
      List<Long> deletedElementsTimestamps) {
    GenericRecord collectionFieldMetadataRecord = new GenericData.Record(collectionFieldMetadataSchema);
    collectionFieldMetadataRecord.put(CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME, topLevelTimestamp);
    collectionFieldMetadataRecord.put(CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME, topLevelTimestamp);
    collectionFieldMetadataRecord.put(CollectionRmdTimestamp.TOP_LEVEL_COLO_ID_FIELD_NAME, topLevelColoID);
    collectionFieldMetadataRecord.put(CollectionRmdTimestamp.PUT_ONLY_PART_LENGTH_FIELD_NAME, putOnlyPartLen);
    collectionFieldMetadataRecord.put(CollectionRmdTimestamp.ACTIVE_ELEM_TS_FIELD_NAME, activeElementsTimestamps);
    collectionFieldMetadataRecord.put(CollectionRmdTimestamp.DELETED_ELEM_FIELD_NAME, deletedElements);
    collectionFieldMetadataRecord.put(CollectionRmdTimestamp.DELETED_ELEM_TS_FIELD_NAME, deletedElementsTimestamps);
    return collectionFieldMetadataRecord;
  }
}
