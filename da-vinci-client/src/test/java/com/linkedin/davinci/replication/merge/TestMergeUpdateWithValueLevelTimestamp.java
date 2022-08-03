package com.linkedin.davinci.replication.merge;

import com.linkedin.davinci.replication.ReplicationMetadataWithValueSchemaId;
import com.linkedin.davinci.serialization.avro.MapOrderingPreservingSerDeFactory;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.SchemaUtils;
import com.linkedin.venice.schema.rmd.ReplicationMetadataConstants;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.schema.rmd.v1.CollectionReplicationMetadata.*;
import static org.mockito.Mockito.*;

public class TestMergeUpdateWithValueLevelTimestamp extends TestMergeUpdate {

  @Test(enabled = false)
  public void testUpdateIgnoredFieldUpdate() {
    final int incomingValueSchemaId = 3;
    final int incomingWriteComputeSchemaId = 3;
    final int oldValueSchemaId = 3;
    // Set up
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(personSchemaV1);
    GenericRecord updateFieldWriteComputeRecord = SchemaUtils.createGenericRecord(writeComputeSchema.getTypes().get(0));
    updateFieldWriteComputeRecord.put("age", 66);
    updateFieldWriteComputeRecord.put("name", "Venice");
    ByteBuffer writeComputeBytes = ByteBuffer.wrap(
        MapOrderingPreservingSerDeFactory.getSerializer(writeComputeSchema).serialize(updateFieldWriteComputeRecord)
    );
    final long valueLevelTimestamp = 10L;
    GenericRecord rmdRecord = createRmdWithValueLevelTimestamp(personRmdSchemaV1, valueLevelTimestamp);
    ReplicationMetadataWithValueSchemaId rmdWithValueSchemaId = new ReplicationMetadataWithValueSchemaId(
        oldValueSchemaId,
        RMD_VERSION_ID,
        rmdRecord
    );
    ReadOnlySchemaRepository readOnlySchemaRepository = mock(ReadOnlySchemaRepository.class);
    doReturn(new DerivedSchemaEntry(incomingValueSchemaId, 1, writeComputeSchema))
        .when(readOnlySchemaRepository).getDerivedSchema(storeName, incomingValueSchemaId, incomingWriteComputeSchemaId);
    doReturn(new SchemaEntry(oldValueSchemaId, personSchemaV1)).when(readOnlySchemaRepository).getValueSchema(storeName, oldValueSchemaId);

    // Update happens below
    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance().createMergeConflictResolver(
        readOnlySchemaRepository,
        new ReplicationMetadataSerDe(readOnlySchemaRepository, storeName, RMD_VERSION_ID),
        storeName
    );
    MergeConflictResult mergeConflictResult = mergeConflictResolver.update(
        Lazy.of(() -> null),
        Optional.of(rmdWithValueSchemaId),
        writeComputeBytes,
        incomingValueSchemaId,
        incomingWriteComputeSchemaId,
        valueLevelTimestamp - 1, // Slightly lower than existing timestamp. Thus update should be ignored.
        1,
        1,
        1
    );
    Assert.assertEquals(mergeConflictResult, MergeConflictResult.getIgnoredResult());
    Assert.assertTrue(
        ((List<?>) rmdRecord.get(ReplicationMetadataConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD)).isEmpty(),
        "When the Update request is ignored, replication_checkpoint_vector should stay the same (empty)."
    );
  }

  @Test(enabled = false)
  public void testUpdateIgnoredFieldUpdateWithEvolvedSchema() {
    /**
     * When the Write Compute request is generated from an evolved value schema, as long as it does not try to update
     * any field that does not exist in the current value, it could still be ignored.
     */
    final int incomingValueSchemaId = 4;
    final int incomingWriteComputeSchemaId = 4;
    final int oldValueSchemaId = 3;
    // Set up
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(personSchemaV2); // Note that a newer schema is used.
    GenericRecord updateFieldWriteComputeRecord = SchemaUtils.createGenericRecord(writeComputeSchema.getTypes().get(0));
    updateFieldWriteComputeRecord.put("age", 66);
    updateFieldWriteComputeRecord.put("name", "Venice");
    ByteBuffer writeComputeBytes = ByteBuffer.wrap(
        MapOrderingPreservingSerDeFactory.getSerializer(writeComputeSchema).serialize(updateFieldWriteComputeRecord)
    );
    final long valueLevelTimestamp = 10L;
    GenericRecord rmdRecord = createRmdWithValueLevelTimestamp(personRmdSchemaV1, valueLevelTimestamp);
    ReplicationMetadataWithValueSchemaId rmdWithValueSchemaId = new ReplicationMetadataWithValueSchemaId(
        oldValueSchemaId,
        RMD_VERSION_ID,
        rmdRecord
    );
    ReadOnlySchemaRepository readOnlySchemaRepository = mock(ReadOnlySchemaRepository.class);
    doReturn(new DerivedSchemaEntry(incomingValueSchemaId, 1, writeComputeSchema))
        .when(readOnlySchemaRepository).getDerivedSchema(storeName, incomingValueSchemaId, incomingWriteComputeSchemaId);
    doReturn(new SchemaEntry(oldValueSchemaId, personSchemaV1)).when(readOnlySchemaRepository).getValueSchema(storeName, oldValueSchemaId);

    // Update happens below
    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance().createMergeConflictResolver(
        readOnlySchemaRepository,
        new ReplicationMetadataSerDe(readOnlySchemaRepository, storeName, RMD_VERSION_ID),
        storeName
    );
    MergeConflictResult mergeConflictResult = mergeConflictResolver.update(
        Lazy.of(() -> null),
        Optional.of(rmdWithValueSchemaId),
        writeComputeBytes,
        incomingValueSchemaId,
        incomingWriteComputeSchemaId,
        valueLevelTimestamp - 1, // Slightly lower than existing timestamp. Thus update should be ignored.
        1,
        1,
        1
    );
    Assert.assertEquals(mergeConflictResult, MergeConflictResult.getIgnoredResult());
    Assert.assertTrue(
        ((List<?>) rmdRecord.get(ReplicationMetadataConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD)).isEmpty(),
        "When the Update request is ignored, replication_checkpoint_vector should stay the same (empty)."
    );
  }

  @Test(enabled = false)
  public void testWholeFieldUpdate() {
    final int incomingValueSchemaId = 3;
    final int incomingWriteComputeSchemaId = 3;
    final int oldValueSchemaId = 3;

    // Set up old/current value.
    GenericRecord oldValueRecord = SchemaUtils.createGenericRecord(personSchemaV1);
    oldValueRecord.put("age", 30);
    oldValueRecord.put("name", "Kafka");
    oldValueRecord.put("intArray", Arrays.asList(1, 2, 3));
    Map<String, String> stringMap = new LinkedHashMap<>();
    stringMap.put("1", "one");
    stringMap.put("2", "two");
    oldValueRecord.put("stringMap", stringMap);
    ByteBuffer oldValueBytes = ByteBuffer.wrap(
        MapOrderingPreservingSerDeFactory.getSerializer(personSchemaV1).serialize(oldValueRecord)
    );

    // Set up Write Compute request.
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(personSchemaV1);
    GenericRecord updateFieldWriteComputeRecord = SchemaUtils.createGenericRecord(writeComputeSchema.getTypes().get(0));
    updateFieldWriteComputeRecord.put("age", 66);
    updateFieldWriteComputeRecord.put("name", "Venice");
    updateFieldWriteComputeRecord.put("intArray", Arrays.asList(6, 7, 8));
    stringMap = new LinkedHashMap<>();
    stringMap.put("4", "four");
    stringMap.put("5", "five");
    updateFieldWriteComputeRecord.put("stringMap", stringMap);
    ByteBuffer writeComputeBytes = ByteBuffer.wrap(
        MapOrderingPreservingSerDeFactory.getSerializer(writeComputeSchema).serialize(updateFieldWriteComputeRecord)
    );

    // Set up current replication metadata.
    final long valueLevelTimestamp = 10L;
    GenericRecord rmdRecord = createRmdWithValueLevelTimestamp(personRmdSchemaV1, valueLevelTimestamp);
    ReplicationMetadataWithValueSchemaId rmdWithValueSchemaId = new ReplicationMetadataWithValueSchemaId(
        oldValueSchemaId,
        RMD_VERSION_ID,
        rmdRecord
    );
    ReadOnlySchemaRepository readOnlySchemaRepository = mock(ReadOnlySchemaRepository.class);
    doReturn(new DerivedSchemaEntry(incomingValueSchemaId, 1, writeComputeSchema))
        .when(readOnlySchemaRepository).getDerivedSchema(storeName, incomingValueSchemaId, incomingWriteComputeSchemaId);
    doReturn(new SchemaEntry(oldValueSchemaId, personSchemaV1)).when(readOnlySchemaRepository).getValueSchema(storeName, oldValueSchemaId);

    // Update happens below
    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance().createMergeConflictResolver(
        readOnlySchemaRepository,
        new ReplicationMetadataSerDe(readOnlySchemaRepository, storeName, RMD_VERSION_ID),
        storeName
    );
    MergeConflictResult mergeConflictResult = mergeConflictResolver.update(
        Lazy.of(() -> oldValueBytes),
        Optional.of(rmdWithValueSchemaId),
        writeComputeBytes,
        incomingValueSchemaId,
        incomingWriteComputeSchemaId,
        valueLevelTimestamp + 1, // Slightly higher than existing timestamp. Thus update is NOT ignored.
        1,
        1,
        1
    );

    // Validate updated replication metadata.
    Assert.assertNotEquals(mergeConflictResult, MergeConflictResult.getIgnoredResult());
    GenericRecord updatedRmd = mergeConflictResult.getReplicationMetadataRecord();
    Assert.assertEquals((List<?>) updatedRmd.get(ReplicationMetadataConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD), Arrays.asList(0L, 1L));

    GenericRecord rmdTimestamp = (GenericRecord) updatedRmd.get(ReplicationMetadataConstants.TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(rmdTimestamp.get("age"), 11L);
    Assert.assertEquals(rmdTimestamp.get("name"), 11L);
    GenericRecord collectionFieldTimestampRecord = (GenericRecord) rmdTimestamp.get("intArray");
    Assert.assertEquals((long) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_TS_FIELD_NAME), 11L);
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(COLLECTION_PUT_ONLY_PART_LENGTH_FIELD_NAME), 3);
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_COLO_ID_FIELD_NAME), 1);
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_TS_FIELD_NAME), Collections.emptyList());

    collectionFieldTimestampRecord = (GenericRecord) rmdTimestamp.get("stringMap");
    Assert.assertEquals((long) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_TS_FIELD_NAME), 11L);
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(COLLECTION_PUT_ONLY_PART_LENGTH_FIELD_NAME), 2);
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_COLO_ID_FIELD_NAME), 1);
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_TS_FIELD_NAME), Collections.emptyList());

    // Validate updated value.
    Assert.assertTrue(mergeConflictResult.getNewValue().isPresent());
    ByteBuffer updatedValueBytes = mergeConflictResult.getNewValue().get();
    GenericRecord updatedValueRecord = MapOrderingPreservingSerDeFactory.getDeserializer(personSchemaV1, personSchemaV1).deserialize(updatedValueBytes.array());

    Assert.assertEquals(updatedValueRecord.get("age"), 66);
    Assert.assertEquals(updatedValueRecord.get("name").toString(), "Venice");
    Assert.assertEquals(updatedValueRecord.get("intArray"), Arrays.asList(6, 7, 8));
    Map<Utf8, Utf8> updatedMapField = (Map<Utf8, Utf8>) updatedValueRecord.get("stringMap");
    Assert.assertEquals(updatedMapField.size(), 2);
    Assert.assertEquals(updatedMapField.get(toUtf8("4")), toUtf8("four"));
    Assert.assertEquals(updatedMapField.get(toUtf8("5")), toUtf8("five"));
  }

  @Test(enabled = false)
  public void testCollectionMerge() {
    final int incomingValueSchemaId = 3;
    final int incomingWriteComputeSchemaId = 3;
    final int oldValueSchemaId = 3;

    // Set up old/current value.
    GenericRecord oldValueRecord = SchemaUtils.createGenericRecord(personSchemaV1);
    oldValueRecord.put("age", 30);
    oldValueRecord.put("name", "Kafka");
    oldValueRecord.put("intArray", Arrays.asList(1, 2, 3));
    Map<String, String> stringMap = new LinkedHashMap<>();
    stringMap.put("1", "one");
    stringMap.put("2", "two");
    oldValueRecord.put("stringMap", stringMap);
    ByteBuffer oldValueBytes = ByteBuffer.wrap(
        MapOrderingPreservingSerDeFactory.getSerializer(personSchemaV1).serialize(oldValueRecord)
    );

    // Set up Write Compute request.
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(personSchemaV1);
    GenericRecord updateFieldWriteComputeRecord = SchemaUtils.createGenericRecord(writeComputeSchema.getTypes().get(0));
    updateFieldWriteComputeRecord.put("age", 99);
    updateFieldWriteComputeRecord.put("name", "Francisco");
    // Try to merge/add 3 numbers to the intArray list field.
    GenericRecord listMerge = createListMergeRecord("intArray", Arrays.asList(6, 7, 8), Collections.emptyList(), writeComputeSchema);
    updateFieldWriteComputeRecord.put("intArray", listMerge);
    ByteBuffer writeComputeBytes = ByteBuffer.wrap(
        MapOrderingPreservingSerDeFactory.getSerializer(writeComputeSchema).serialize(updateFieldWriteComputeRecord)
    );

    // Set up current replication metadata.
    final long valueLevelTimestamp = 10L;
    GenericRecord rmdRecord = createRmdWithValueLevelTimestamp(personRmdSchemaV1, valueLevelTimestamp);
    ReplicationMetadataWithValueSchemaId rmdWithValueSchemaId = new ReplicationMetadataWithValueSchemaId(
        oldValueSchemaId,
        RMD_VERSION_ID,
        rmdRecord
    );
    ReadOnlySchemaRepository readOnlySchemaRepository = mock(ReadOnlySchemaRepository.class);
    doReturn(new DerivedSchemaEntry(incomingValueSchemaId, 1, writeComputeSchema))
        .when(readOnlySchemaRepository).getDerivedSchema(storeName, incomingValueSchemaId, incomingWriteComputeSchemaId);
    doReturn(new SchemaEntry(oldValueSchemaId, personSchemaV1)).when(readOnlySchemaRepository).getValueSchema(storeName, oldValueSchemaId);

    // Update happens below
    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance().createMergeConflictResolver(
        readOnlySchemaRepository,
        new ReplicationMetadataSerDe(readOnlySchemaRepository, storeName, RMD_VERSION_ID),
        storeName
    );

    final int newColoID = 3;
    MergeConflictResult mergeConflictResult = mergeConflictResolver.update(
        Lazy.of(() -> oldValueBytes),
        Optional.of(rmdWithValueSchemaId),
        writeComputeBytes,
        incomingValueSchemaId,
        incomingWriteComputeSchemaId,
        valueLevelTimestamp + 1, // Slightly higher than existing timestamp. Thus update is NOT ignored.
        1,
        1,
        newColoID
    );

    // Validate updated replication metadata.
    Assert.assertNotEquals(mergeConflictResult, MergeConflictResult.getIgnoredResult());
    GenericRecord updatedRmd = mergeConflictResult.getReplicationMetadataRecord();
    Assert.assertEquals((List<?>) updatedRmd.get(ReplicationMetadataConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD), Arrays.asList(0L, 1L));

    GenericRecord rmdTimestamp = (GenericRecord) updatedRmd.get(ReplicationMetadataConstants.TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(rmdTimestamp.get("age"), 11L);
    Assert.assertEquals(rmdTimestamp.get("name"), 11L);
    GenericRecord collectionFieldTimestampRecord = (GenericRecord) rmdTimestamp.get("intArray");
    Assert.assertEquals(
        (long) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_TS_FIELD_NAME), 10L,
        "Collection top-level timestamp does not change because collection merge does not affect top-level timestamp"
    );
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(COLLECTION_PUT_ONLY_PART_LENGTH_FIELD_NAME), 3);
    Assert.assertEquals(
        (int) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_COLO_ID_FIELD_NAME),
        -1,
        "Collection top-level should NOT be changed by collection merge"
    );
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME), Arrays.asList(11L, 11L, 11L));
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_TS_FIELD_NAME), Collections.emptyList());

    collectionFieldTimestampRecord = (GenericRecord) rmdTimestamp.get("stringMap");
    Assert.assertEquals((long) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_TS_FIELD_NAME), 10L);
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(COLLECTION_PUT_ONLY_PART_LENGTH_FIELD_NAME), 2);
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_COLO_ID_FIELD_NAME), -1);
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_TS_FIELD_NAME), Collections.emptyList());

    // Validate updated value.
    Assert.assertTrue(mergeConflictResult.getNewValue().isPresent());
    ByteBuffer updatedValueBytes = mergeConflictResult.getNewValue().get();
    GenericRecord updatedValueRecord = MapOrderingPreservingSerDeFactory.getDeserializer(personSchemaV1, personSchemaV1).deserialize(updatedValueBytes.array());

    Assert.assertEquals(updatedValueRecord.get("age"), 99);
    Assert.assertEquals(updatedValueRecord.get("name").toString(), "Francisco");
    Assert.assertEquals(
        updatedValueRecord.get("intArray"),
        Arrays.asList(1, 2, 3, 6, 7, 8),
        "After applying collection (list) merge, the list field should contain all integers."
    );
    Map<Utf8, Utf8> updatedMapField = (Map<Utf8, Utf8>) updatedValueRecord.get("stringMap");
    Assert.assertEquals(updatedMapField.size(), 2);
    Assert.assertEquals(updatedMapField.get(toUtf8("1")), toUtf8("one"));
    Assert.assertEquals(updatedMapField.get(toUtf8("2")), toUtf8("two"));
  }

  @Test(enabled = false)
  public void testWholeFieldUpdateWithEvolvedSchema() {
    // In this case, the Write Compute request is generated from a value schema that is different from the schema used
    // by the current value. We expect the superset schema to be used in this case.
    final int oldValueSchemaId = 3;
    final int incomingValueSchemaId = 4;
    final int incomingWriteComputeSchemaId = 4;
    final int supersetValueSchemaId = 5;

    // Set up old/current value.
    GenericRecord oldValueRecord = SchemaUtils.createGenericRecord(personSchemaV1);
    oldValueRecord.put("age", 30);
    oldValueRecord.put("name", "Kafka");
    oldValueRecord.put("intArray", Arrays.asList(1, 2, 3));
    ByteBuffer oldValueBytes = ByteBuffer.wrap(
        MapOrderingPreservingSerDeFactory.getSerializer(personSchemaV1).serialize(oldValueRecord)
    );

    // Set up Write Compute request.
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(personSchemaV2);
    GenericRecord updateFieldWriteComputeRecord = SchemaUtils.createGenericRecord(writeComputeSchema.getTypes().get(0));
    updateFieldWriteComputeRecord.put("age", 66);
    updateFieldWriteComputeRecord.put("name", "Venice");
    updateFieldWriteComputeRecord.put("favoritePet", "a random stray cat");
    updateFieldWriteComputeRecord.put("stringArray", Arrays.asList("one", "two", "three"));
    ByteBuffer writeComputeBytes = ByteBuffer.wrap(
        MapOrderingPreservingSerDeFactory.getSerializer(writeComputeSchema).serialize(updateFieldWriteComputeRecord)
    );

    // Set up current replication metadata.
    final long valueLevelTimestamp = 10L;
    GenericRecord rmdRecord = createRmdWithValueLevelTimestamp(personRmdSchemaV1, valueLevelTimestamp);
    ReplicationMetadataWithValueSchemaId rmdWithValueSchemaId = new ReplicationMetadataWithValueSchemaId(
        oldValueSchemaId,
        RMD_VERSION_ID,
        rmdRecord
    );
    ReadOnlySchemaRepository readOnlySchemaRepository = mock(ReadOnlySchemaRepository.class);
    doReturn(new DerivedSchemaEntry(incomingValueSchemaId, 1, writeComputeSchema))
        .when(readOnlySchemaRepository).getDerivedSchema(storeName, incomingValueSchemaId, incomingWriteComputeSchemaId);
    doReturn(new SchemaEntry(oldValueSchemaId, personSchemaV1)).when(readOnlySchemaRepository).getValueSchema(storeName, oldValueSchemaId);
    doReturn(new SchemaEntry(incomingValueSchemaId, personSchemaV2)).when(readOnlySchemaRepository).getValueSchema(storeName, incomingValueSchemaId);
    doReturn(new SchemaEntry(supersetValueSchemaId, personSchemaV3)).when(readOnlySchemaRepository).getValueSchema(storeName, supersetValueSchemaId);
    doReturn(Optional.of(new SchemaEntry(supersetValueSchemaId, personSchemaV3))).when(readOnlySchemaRepository).getSupersetSchema(storeName);
    doReturn(new ReplicationMetadataSchemaEntry(oldValueSchemaId, RMD_VERSION_ID, personRmdSchemaV1))
        .when(readOnlySchemaRepository)
        .getReplicationMetadataSchema(storeName, oldValueSchemaId, RMD_VERSION_ID);
    doReturn(new ReplicationMetadataSchemaEntry(incomingValueSchemaId, RMD_VERSION_ID, personRmdSchemaV2))
        .when(readOnlySchemaRepository)
        .getReplicationMetadataSchema(storeName, incomingValueSchemaId, RMD_VERSION_ID);
    doReturn(new ReplicationMetadataSchemaEntry(supersetValueSchemaId, RMD_VERSION_ID, personRmdSchemaV3))
        .when(readOnlySchemaRepository)
        .getReplicationMetadataSchema(storeName, supersetValueSchemaId, RMD_VERSION_ID);

    // Update happens below
    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance().createMergeConflictResolver(
        readOnlySchemaRepository,
        new ReplicationMetadataSerDe(readOnlySchemaRepository, storeName, RMD_VERSION_ID),
        storeName
    );
    final int newValueColoID = 3;
    MergeConflictResult mergeConflictResult = mergeConflictResolver.update(
        Lazy.of(() -> oldValueBytes),
        Optional.of(rmdWithValueSchemaId),
        writeComputeBytes,
        incomingValueSchemaId,
        incomingWriteComputeSchemaId,
        valueLevelTimestamp + 1, // Slightly higher than existing timestamp. Thus update is NOT ignored.
        1,
        1,
        newValueColoID
    );

    // Validate updated replication metadata.
    Assert.assertNotEquals(mergeConflictResult, MergeConflictResult.getIgnoredResult());
    GenericRecord updatedRmd = mergeConflictResult.getReplicationMetadataRecord();
    Assert.assertEquals((List<?>) updatedRmd.get(ReplicationMetadataConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD), Arrays.asList(0L, 1L));

    GenericRecord rmdTimestamp = (GenericRecord) updatedRmd.get(ReplicationMetadataConstants.TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(rmdTimestamp.get("age"), 11L);
    Assert.assertEquals(rmdTimestamp.get("name"), 11L);
    Assert.assertEquals(rmdTimestamp.get("favoritePet"), 11L);
    GenericRecord collectionFieldTimestampRecord = (GenericRecord) rmdTimestamp.get("intArray");
    Assert.assertEquals((long) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_TS_FIELD_NAME), 10L);
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(COLLECTION_PUT_ONLY_PART_LENGTH_FIELD_NAME), 3);
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_COLO_ID_FIELD_NAME), -1);
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_TS_FIELD_NAME), Collections.emptyList());

    collectionFieldTimestampRecord = (GenericRecord) rmdTimestamp.get("stringMap");
    Assert.assertEquals(
        (long) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_TS_FIELD_NAME),
        10L,
        "This field exists in the original value so that even though the map is empty, the expanded per-field "
            + "timestamp of this field should be equal to the original whole-value level timestamp."
    );
    Assert.assertEquals(
        (int) collectionFieldTimestampRecord.get(COLLECTION_PUT_ONLY_PART_LENGTH_FIELD_NAME),
        0,
        "The map in this field is empty. So this field in the collection field metadata should be 0.");
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_COLO_ID_FIELD_NAME), -1);
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_TS_FIELD_NAME), Collections.emptyList());

    collectionFieldTimestampRecord = (GenericRecord) rmdTimestamp.get("stringArray");
    Assert.assertEquals(
        (long) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_TS_FIELD_NAME),
        11L,
        "This field should be added by the Update request."
    );
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(COLLECTION_PUT_ONLY_PART_LENGTH_FIELD_NAME), 3);
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_COLO_ID_FIELD_NAME), newValueColoID);
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_TS_FIELD_NAME), Collections.emptyList());

    // Validate updated value.
    Assert.assertTrue(mergeConflictResult.getNewValue().isPresent());
    ByteBuffer updatedValueBytes = mergeConflictResult.getNewValue().get();
    GenericRecord updatedValueRecord = MapOrderingPreservingSerDeFactory.getDeserializer(personSchemaV3, personSchemaV3).deserialize(updatedValueBytes.array());

    Assert.assertEquals(updatedValueRecord.get("age"), 66);
    Assert.assertEquals(updatedValueRecord.get("name").toString(), "Venice");
    Assert.assertEquals(updatedValueRecord.get("favoritePet").toString(), "a random stray cat");
    Assert.assertEquals(updatedValueRecord.get("intArray"), Arrays.asList(1, 2, 3));
    Assert.assertEquals(
        updatedValueRecord.get("stringArray"),
        Arrays.asList(toUtf8("one"), toUtf8("two"), toUtf8("three"))
    );
    Map<Utf8, Utf8> updatedMapField = (Map<Utf8, Utf8>) updatedValueRecord.get("stringMap");
    Assert.assertTrue(updatedMapField.isEmpty());
  }

  @Test(enabled = false)
  public void testCollectionMergeWithEvolvedSchema() {
    // Test the situation where the Write Compute request tries to do collection merge on a field that does not exist
    // in the current/old value. We expect the superset schema to be used in the result.
    final int oldValueSchemaId = 3;
    final int incomingValueSchemaId = 4;
    final int incomingWriteComputeSchemaId = 4;
    final int supersetValueSchemaId = 5;

    // Set up old/current value.
    GenericRecord oldValueRecord = SchemaUtils.createGenericRecord(personSchemaV1);
    oldValueRecord.put("age", 30);
    oldValueRecord.put("name", "Kafka");
    oldValueRecord.put("intArray", Arrays.asList(1, 2, 3));
    ByteBuffer oldValueBytes = ByteBuffer.wrap(
        MapOrderingPreservingSerDeFactory.getSerializer(personSchemaV1).serialize(oldValueRecord)
    );

    // Set up Write Compute request.
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(personSchemaV2);
    GenericRecord updateFieldWriteComputeRecord = SchemaUtils.createGenericRecord(writeComputeSchema.getTypes().get(0));

    GenericRecord listMerge = createListMergeRecord(
        "stringArray",
        Arrays.asList("one", "two", "three"),
        Arrays.asList("four", "five", "six"),
        writeComputeSchema
    );
    updateFieldWriteComputeRecord.put("stringArray", listMerge);
    ByteBuffer writeComputeBytes = ByteBuffer.wrap(
        MapOrderingPreservingSerDeFactory.getSerializer(writeComputeSchema).serialize(updateFieldWriteComputeRecord)
    );

    // Set up current replication metadata.
    final long valueLevelTimestamp = 10L;
    GenericRecord rmdRecord = createRmdWithValueLevelTimestamp(personRmdSchemaV1, valueLevelTimestamp);
    ReplicationMetadataWithValueSchemaId rmdWithValueSchemaId = new ReplicationMetadataWithValueSchemaId(
        oldValueSchemaId,
        RMD_VERSION_ID,
        rmdRecord
    );
    ReadOnlySchemaRepository readOnlySchemaRepository = mock(ReadOnlySchemaRepository.class);
    doReturn(new DerivedSchemaEntry(incomingValueSchemaId, 1, writeComputeSchema))
        .when(readOnlySchemaRepository).getDerivedSchema(storeName, incomingValueSchemaId, incomingWriteComputeSchemaId);
    doReturn(new SchemaEntry(oldValueSchemaId, personSchemaV1)).when(readOnlySchemaRepository).getValueSchema(storeName, oldValueSchemaId);
    doReturn(new SchemaEntry(incomingValueSchemaId, personSchemaV2)).when(readOnlySchemaRepository).getValueSchema(storeName, incomingValueSchemaId);
    doReturn(new SchemaEntry(supersetValueSchemaId, personSchemaV3)).when(readOnlySchemaRepository).getValueSchema(storeName, supersetValueSchemaId);
    doReturn(Optional.of(new SchemaEntry(supersetValueSchemaId, personSchemaV3))).when(readOnlySchemaRepository).getSupersetSchema(storeName);
    doReturn(new ReplicationMetadataSchemaEntry(oldValueSchemaId, RMD_VERSION_ID, personRmdSchemaV1))
        .when(readOnlySchemaRepository)
        .getReplicationMetadataSchema(storeName, oldValueSchemaId, RMD_VERSION_ID);
    doReturn(new ReplicationMetadataSchemaEntry(incomingValueSchemaId, RMD_VERSION_ID, personRmdSchemaV2))
        .when(readOnlySchemaRepository)
        .getReplicationMetadataSchema(storeName, incomingValueSchemaId, RMD_VERSION_ID);
    doReturn(new ReplicationMetadataSchemaEntry(supersetValueSchemaId, RMD_VERSION_ID, personRmdSchemaV3))
        .when(readOnlySchemaRepository)
        .getReplicationMetadataSchema(storeName, supersetValueSchemaId, RMD_VERSION_ID);

    // Update happens below
    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance().createMergeConflictResolver(
        readOnlySchemaRepository,
        new ReplicationMetadataSerDe(readOnlySchemaRepository, storeName, RMD_VERSION_ID),
        storeName
    );
    final int newValueColoID = 3;
    MergeConflictResult mergeConflictResult = mergeConflictResolver.update(
        Lazy.of(() -> oldValueBytes),
        Optional.of(rmdWithValueSchemaId),
        writeComputeBytes,
        incomingValueSchemaId,
        incomingWriteComputeSchemaId,
        valueLevelTimestamp + 1, // Slightly higher than existing timestamp. Thus update is NOT ignored.
        1,
        1,
        newValueColoID
    );

    // Validate updated replication metadata.
    Assert.assertNotEquals(mergeConflictResult, MergeConflictResult.getIgnoredResult());
    GenericRecord updatedRmd = mergeConflictResult.getReplicationMetadataRecord();
    Assert.assertEquals((List<?>) updatedRmd.get(ReplicationMetadataConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD), Arrays.asList(0L, 1L));

    GenericRecord rmdTimestamp = (GenericRecord) updatedRmd.get(ReplicationMetadataConstants.TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(rmdTimestamp.get("age"), 10L);
    Assert.assertEquals(rmdTimestamp.get("name"), 10L);
    Assert.assertEquals(rmdTimestamp.get("favoritePet"), 0L);
    GenericRecord collectionFieldTimestampRecord = (GenericRecord) rmdTimestamp.get("intArray");
    Assert.assertEquals((long) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_TS_FIELD_NAME), 10L);
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(COLLECTION_PUT_ONLY_PART_LENGTH_FIELD_NAME), 3);
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_COLO_ID_FIELD_NAME), -1);
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_TS_FIELD_NAME), Collections.emptyList());

    collectionFieldTimestampRecord = (GenericRecord) rmdTimestamp.get("stringMap");
    Assert.assertEquals(
        (long) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_TS_FIELD_NAME),
        10L,
        "This field exists in the original value so that even though the map is empty, the expanded per-field "
            + "timestamp of this field should be equal to the original whole-value level timestamp."
    );
    Assert.assertEquals(
        (int) collectionFieldTimestampRecord.get(COLLECTION_PUT_ONLY_PART_LENGTH_FIELD_NAME),
        0,
        "The map in this field does not a put-only part."
    );
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_COLO_ID_FIELD_NAME), -1);
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_TS_FIELD_NAME), Collections.emptyList());

    collectionFieldTimestampRecord = (GenericRecord) rmdTimestamp.get("stringArray");
    Assert.assertEquals(
        (long) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_TS_FIELD_NAME),
        0L,
        "Doing collection merge on this field does not affect the top-level timestamp."
    );
    Assert.assertEquals(
        (int) collectionFieldTimestampRecord.get(COLLECTION_PUT_ONLY_PART_LENGTH_FIELD_NAME),
        0,
        "This list field does not have a put-only part because all elements are added by collection merge."
    );
    Assert.assertEquals(
        (int) collectionFieldTimestampRecord.get(COLLECTION_TOP_LEVEL_COLO_ID_FIELD_NAME),
        -1,
        "Doing collection merge on this field does not affect the its collection field colo-ID.");
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME), Arrays.asList(11L, 11L, 11L));
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_TS_FIELD_NAME), Arrays.asList(11L, 11L, 11L));

    Assert.assertEquals(
        (List<?>) collectionFieldTimestampRecord.get(COLLECTION_DELETED_ELEM_FIELD_NAME),
        Arrays.asList(toUtf8("five"), toUtf8("four"), toUtf8("six")) // Sorted lexicographically
    );

    // Validate updated value.
    Assert.assertTrue(mergeConflictResult.getNewValue().isPresent());
    ByteBuffer updatedValueBytes = mergeConflictResult.getNewValue().get();
    GenericRecord updatedValueRecord = MapOrderingPreservingSerDeFactory.getDeserializer(personSchemaV3, personSchemaV3).deserialize(updatedValueBytes.array());

    Assert.assertEquals(updatedValueRecord.get("age"), 30);
    Assert.assertEquals(updatedValueRecord.get("name").toString(), "Kafka");
    Assert.assertEquals(updatedValueRecord.get("favoritePet").toString(), "Pancake!");
    Assert.assertEquals(updatedValueRecord.get("intArray"), Arrays.asList(1, 2, 3));
    Assert.assertEquals(
        updatedValueRecord.get("stringArray"),
        Arrays.asList(toUtf8("one"), toUtf8("three"), toUtf8("two"))
    );
    Map<Utf8, Utf8> updatedMapField = (Map<Utf8, Utf8>) updatedValueRecord.get("stringMap");
    Assert.assertTrue(updatedMapField.isEmpty());
  }
}
