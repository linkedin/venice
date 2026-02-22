package com.linkedin.davinci.replication.merge;

import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.ACTIVE_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.PUT_ONLY_PART_LENGTH_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_COLO_ID_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.davinci.serializer.avro.MapOrderPreservingSerDeFactory;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * TODO: Use schema that contains map field after Avro Map deserialization issue is resolved.
 * TODO: Merge with {@link TestMergeUpdateWithValueLevelTimestamp}.
 */
public class TestMergeUpdateWithFieldLevelTimestamp extends TestMergeConflictResolver {
  @Test
  public void testUpdateIgnoredFieldUpdate() {
    final int incomingValueSchemaId = 3;
    final int incomingWriteComputeSchemaId = 3;
    final int oldValueSchemaId = 3;
    // Set up
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(personSchemaV3);
    GenericRecord updateFieldWriteComputeRecord =
        new UpdateBuilderImpl(writeComputeSchema).setNewFieldValue("nullableListField", null)
            .setNewFieldValue("age", 66)
            .setNewFieldValue("name", "Venice")
            .build();
    ByteBuffer writeComputeBytes = ByteBuffer.wrap(
        MapOrderPreservingSerDeFactory.getSerializer(writeComputeSchema).serialize(updateFieldWriteComputeRecord));
    final long valueLevelTimestamp = 10L;
    Map<String, Long> fieldNameToTimestampMap = new HashMap<>();
    fieldNameToTimestampMap.put("nullableListField", 10L);
    fieldNameToTimestampMap.put("age", 10L);
    fieldNameToTimestampMap.put("favoritePet", 10L);
    fieldNameToTimestampMap.put("name", 10L);
    fieldNameToTimestampMap.put("intArray", 10L);
    fieldNameToTimestampMap.put("stringArray", 10L);
    fieldNameToTimestampMap.put("stringMap", 10L);

    GenericRecord rmdRecord = createRmdWithFieldLevelTimestamp(personRmdSchemaV3, fieldNameToTimestampMap);
    RmdWithValueSchemaId rmdWithValueSchemaId = new RmdWithValueSchemaId(oldValueSchemaId, RMD_VERSION_ID, rmdRecord);
    ReadOnlySchemaRepository readOnlySchemaRepository = mock(ReadOnlySchemaRepository.class);
    doReturn(new DerivedSchemaEntry(incomingValueSchemaId, 1, writeComputeSchema)).when(readOnlySchemaRepository)
        .getDerivedSchema(storeName, incomingValueSchemaId, incomingWriteComputeSchemaId);
    doReturn(new SchemaEntry(oldValueSchemaId, personSchemaV3)).when(readOnlySchemaRepository)
        .getValueSchema(storeName, oldValueSchemaId);
    doReturn(new SchemaEntry(oldValueSchemaId, personSchemaV3)).when(readOnlySchemaRepository)
        .getSupersetSchema(storeName);
    StringAnnotatedStoreSchemaCache stringAnnotatedStoreSchemaCache =
        new StringAnnotatedStoreSchemaCache(storeName, readOnlySchemaRepository);
    // Update happens below
    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance()
        .createMergeConflictResolver(
            stringAnnotatedStoreSchemaCache,
            new RmdSerDe(stringAnnotatedStoreSchemaCache, RMD_VERSION_ID),
            storeName);
    MergeConflictResult mergeConflictResult = mergeConflictResolver.update(
        Lazy.of(() -> null),
        rmdWithValueSchemaId,
        writeComputeBytes,
        incomingValueSchemaId,
        incomingWriteComputeSchemaId,
        valueLevelTimestamp - 1, // Slightly lower than existing timestamp. Thus update should be ignored.
        P1,
        1,
        1,
        null);
    Assert.assertEquals(mergeConflictResult, MergeConflictResult.getIgnoredResult());
    Assert.assertTrue(
        ((List<?>) rmdRecord.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME)).isEmpty(),
        "When the Update request is ignored, replication_checkpoint_vector should stay the same (empty).");
  }

  @Test
  public void testUpdateIgnoredFieldUpdateWithNewSchema() {
    final int incomingValueSchemaId = 3;
    final int incomingWriteComputeSchemaId = 1;
    final int oldValueSchemaId = 2;
    // Set up
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(personSchemaV3);
    GenericRecord updateFieldWriteComputeRecord = new UpdateBuilderImpl(writeComputeSchema).setNewFieldValue("age", 66)
        .setNewFieldValue("name", "Venice")
        .build();
    ByteBuffer writeComputeBytes = ByteBuffer.wrap(
        MapOrderPreservingSerDeFactory.getSerializer(writeComputeSchema).serialize(updateFieldWriteComputeRecord));
    final long valueLevelTimestamp = 10L;
    Map<String, Long> fieldNameToTimestampMap = new HashMap<>();
    fieldNameToTimestampMap.put("age", 10L);
    fieldNameToTimestampMap.put("favoritePet", 10L);
    fieldNameToTimestampMap.put("name", 10L);
    fieldNameToTimestampMap.put("intArray", 10L);
    fieldNameToTimestampMap.put("stringArray", 10L);

    GenericRecord rmdRecord = createRmdWithFieldLevelTimestamp(personRmdSchemaV2, fieldNameToTimestampMap);
    RmdWithValueSchemaId rmdWithValueSchemaId = new RmdWithValueSchemaId(oldValueSchemaId, RMD_VERSION_ID, rmdRecord);
    ReadOnlySchemaRepository readOnlySchemaRepository = mock(ReadOnlySchemaRepository.class);

    doReturn(new DerivedSchemaEntry(incomingValueSchemaId, incomingWriteComputeSchemaId, writeComputeSchema))
        .when(readOnlySchemaRepository)
        .getDerivedSchema(storeName, incomingValueSchemaId, incomingWriteComputeSchemaId);
    doReturn(new SchemaEntry(oldValueSchemaId, personSchemaV2)).when(readOnlySchemaRepository)
        .getValueSchema(storeName, oldValueSchemaId);
    doReturn(new SchemaEntry(incomingValueSchemaId, personSchemaV3)).when(readOnlySchemaRepository)
        .getValueSchema(storeName, incomingValueSchemaId);
    doReturn(new SchemaEntry(incomingValueSchemaId, personSchemaV3)).when(readOnlySchemaRepository)
        .getSupersetSchema(storeName);
    StringAnnotatedStoreSchemaCache stringAnnotatedStoreSchemaCache =
        new StringAnnotatedStoreSchemaCache(storeName, readOnlySchemaRepository);
    // Update happens below
    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance()
        .createMergeConflictResolver(
            stringAnnotatedStoreSchemaCache,
            new RmdSerDe(stringAnnotatedStoreSchemaCache, RMD_VERSION_ID),
            storeName);
    MergeConflictResult mergeConflictResult = mergeConflictResolver.update(
        Lazy.of(() -> null),
        rmdWithValueSchemaId,
        writeComputeBytes,
        incomingValueSchemaId,
        incomingWriteComputeSchemaId,
        valueLevelTimestamp - 1, // Slightly lower than existing timestamp. Thus update should be ignored.
        P1,
        1,
        1,
        null);
    Assert.assertEquals(mergeConflictResult, MergeConflictResult.getIgnoredResult());
    Assert.assertTrue(
        ((List<?>) rmdRecord.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME)).isEmpty(),
        "When the Update request is ignored, replication_checkpoint_vector should stay the same (empty).");
  }

  @Test
  public void testUpdateAppliedFieldUpdateWithNewSchema() {
    final int incomingValueSchemaId = 3;
    final int oldValueSchemaId = 2;
    // Set up
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(personSchemaV3);
    GenericRecord updateFieldWriteComputeRecord = new UpdateBuilderImpl(writeComputeSchema).setNewFieldValue("age", 66)
        .setNewFieldValue("name", "Venice")
        .setNewFieldValue("nullableListField", null)
        .build();
    ByteBuffer writeComputeBytes = ByteBuffer.wrap(
        MapOrderPreservingSerDeFactory.getSerializer(writeComputeSchema).serialize(updateFieldWriteComputeRecord));
    final long valueLevelTimestamp = 10L;
    Map<String, Long> fieldNameToTimestampMap = new HashMap<>();
    fieldNameToTimestampMap.put("age", 10L);
    fieldNameToTimestampMap.put("favoritePet", 10L);
    fieldNameToTimestampMap.put("name", 10L);
    fieldNameToTimestampMap.put("intArray", 10L);
    fieldNameToTimestampMap.put("stringArray", 10L);

    GenericRecord rmdRecord = createRmdWithFieldLevelTimestamp(personRmdSchemaV2, fieldNameToTimestampMap);
    RmdWithValueSchemaId rmdWithValueSchemaId = new RmdWithValueSchemaId(oldValueSchemaId, RMD_VERSION_ID, rmdRecord);
    ReadOnlySchemaRepository readOnlySchemaRepository = mock(ReadOnlySchemaRepository.class);

    doReturn(new DerivedSchemaEntry(incomingValueSchemaId, 1, writeComputeSchema)).when(readOnlySchemaRepository)
        .getDerivedSchema(storeName, incomingValueSchemaId, 1);
    doReturn(new SchemaEntry(oldValueSchemaId, personSchemaV2)).when(readOnlySchemaRepository)
        .getValueSchema(storeName, oldValueSchemaId);
    doReturn(new SchemaEntry(incomingValueSchemaId, personSchemaV3)).when(readOnlySchemaRepository)
        .getValueSchema(storeName, incomingValueSchemaId);
    doReturn(new SchemaEntry(incomingValueSchemaId, personSchemaV3)).when(readOnlySchemaRepository)
        .getSupersetSchema(storeName);
    doReturn(new RmdSchemaEntry(oldValueSchemaId, 1, personRmdSchemaV2)).when(readOnlySchemaRepository)
        .getReplicationMetadataSchema(storeName, oldValueSchemaId, 1);
    doReturn(new RmdSchemaEntry(incomingValueSchemaId, 1, personRmdSchemaV3)).when(readOnlySchemaRepository)
        .getReplicationMetadataSchema(storeName, incomingValueSchemaId, 1);
    doReturn(new SchemaEntry(incomingValueSchemaId, personSchemaV3)).when(readOnlySchemaRepository)
        .getValueSchema(storeName, incomingValueSchemaId);

    StringAnnotatedStoreSchemaCache stringAnnotatedStoreSchemaCache =
        new StringAnnotatedStoreSchemaCache(storeName, readOnlySchemaRepository);
    // Update happens below
    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance()
        .createMergeConflictResolver(
            stringAnnotatedStoreSchemaCache,
            new RmdSerDe(stringAnnotatedStoreSchemaCache, RMD_VERSION_ID),
            storeName);
    MergeConflictResult mergeConflictResult = mergeConflictResolver.update(
        Lazy.of(() -> null),
        rmdWithValueSchemaId,
        writeComputeBytes,
        incomingValueSchemaId,
        1,
        valueLevelTimestamp + 1, // Slightly higher than existing timestamp. Thus update should be applied.
        P1,
        1,
        1,
        null);
    Assert.assertFalse(mergeConflictResult.isUpdateIgnored());

    ByteBuffer newValueOptional = mergeConflictResult.getNewValue();
    Assert.assertNotNull(newValueOptional);
    GenericRecord newValueRecord = getDeserializer(personSchemaV3, personSchemaV3).deserialize(newValueOptional);
    Assert.assertEquals(newValueRecord.get("age").toString(), "66");
    Assert.assertEquals(newValueRecord.get("name").toString(), "Venice");
    Assert.assertNull(newValueRecord.get("nullableListField"));
  }

  @Test
  public void testWholeFieldUpdate() {
    final int incomingValueSchemaId = 3;
    final int incomingWriteComputeSchemaId = 3;
    final int oldValueSchemaId = 3;

    // Set up old/current value.
    GenericRecord oldValueRecord = AvroSchemaUtils.createGenericRecord(personSchemaV2);
    oldValueRecord.put("age", 30);
    oldValueRecord.put("name", "Kafka");
    oldValueRecord.put("intArray", Arrays.asList(1, 2, 3));

    // Set up Write Compute request.
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(personSchemaV2);

    // Set up current replication metadata.
    final long valueLevelTimestamp = 10L;
    Map<String, Long> fieldNameToTimestampMap = new HashMap<>();
    fieldNameToTimestampMap.put("age", valueLevelTimestamp);
    fieldNameToTimestampMap.put("favoritePet", valueLevelTimestamp);
    fieldNameToTimestampMap.put("name", valueLevelTimestamp);
    fieldNameToTimestampMap.put("intArray", valueLevelTimestamp);
    fieldNameToTimestampMap.put("stringArray", valueLevelTimestamp);
    GenericRecord rmdRecord = createRmdWithFieldLevelTimestamp(personRmdSchemaV2, fieldNameToTimestampMap);
    RmdWithValueSchemaId rmdWithValueSchemaId = new RmdWithValueSchemaId(oldValueSchemaId, RMD_VERSION_ID, rmdRecord);
    ReadOnlySchemaRepository readOnlySchemaRepository = mock(ReadOnlySchemaRepository.class);
    doReturn(new DerivedSchemaEntry(incomingValueSchemaId, 1, writeComputeSchema)).when(readOnlySchemaRepository)
        .getDerivedSchema(storeName, incomingValueSchemaId, incomingWriteComputeSchemaId);
    doReturn(new SchemaEntry(oldValueSchemaId, personSchemaV2)).when(readOnlySchemaRepository)
        .getValueSchema(storeName, oldValueSchemaId);
    doReturn(new SchemaEntry(oldValueSchemaId, personSchemaV2)).when(readOnlySchemaRepository)
        .getSupersetSchema(storeName);
    StringAnnotatedStoreSchemaCache stringAnnotatedStoreSchemaCache =
        new StringAnnotatedStoreSchemaCache(storeName, readOnlySchemaRepository);
    // Update happens below
    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance()
        .createMergeConflictResolver(
            stringAnnotatedStoreSchemaCache,
            new RmdSerDe(stringAnnotatedStoreSchemaCache, RMD_VERSION_ID),
            storeName);

    GenericRecord updateFieldPartialUpdateRecord1 = AvroSchemaUtils.createGenericRecord(writeComputeSchema);
    updateFieldPartialUpdateRecord1.put("age", 66);
    updateFieldPartialUpdateRecord1.put("name", "Venice");
    updateFieldPartialUpdateRecord1.put("intArray", Arrays.asList(6, 7, 8));
    ByteBuffer writeComputeBytes1 = ByteBuffer.wrap(
        MapOrderPreservingSerDeFactory.getSerializer(writeComputeSchema).serialize(updateFieldPartialUpdateRecord1));
    MergeConflictResult mergeConflictResult = mergeConflictResolver.update(
        Lazy.of(() -> null),
        rmdWithValueSchemaId,
        writeComputeBytes1,
        incomingValueSchemaId,
        incomingWriteComputeSchemaId,
        valueLevelTimestamp + 1,
        P1,
        1,
        1,
        null);

    GenericRecord updateFieldPartialUpdateRecord2 = AvroSchemaUtils.createGenericRecord(writeComputeSchema);
    updateFieldPartialUpdateRecord2.put("intArray", Arrays.asList(10, 20, 30, 40));
    ByteBuffer writeComputeBytes2 = ByteBuffer.wrap(
        MapOrderPreservingSerDeFactory.getSerializer(writeComputeSchema).serialize(updateFieldPartialUpdateRecord2));

    ByteBuffer updatedValueBytes = mergeConflictResult.getNewValue();
    mergeConflictResult = mergeConflictResolver.update(
        Lazy.of(() -> updatedValueBytes),
        rmdWithValueSchemaId,
        writeComputeBytes2,
        incomingValueSchemaId,
        incomingWriteComputeSchemaId,
        valueLevelTimestamp + 2,
        P2,
        0,
        0,
        null);

    // Validate updated replication metadata.
    Assert.assertFalse(mergeConflictResult.isUpdateIgnored());
    GenericRecord updatedRmd = mergeConflictResult.getRmdRecord();
    Assert.assertEquals(
        (List<?>) updatedRmd.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME),
        Collections.emptyList());

    GenericRecord rmdTimestamp = (GenericRecord) updatedRmd.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(rmdTimestamp.get("age"), 11L);
    Assert.assertEquals(rmdTimestamp.get("name"), 11L);
    GenericRecord collectionFieldTimestampRecord = (GenericRecord) rmdTimestamp.get("intArray");
    Assert.assertEquals((long) collectionFieldTimestampRecord.get(TOP_LEVEL_TS_FIELD_NAME), 12L);
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 4);
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(TOP_LEVEL_COLO_ID_FIELD_NAME), 0);
    Assert
        .assertEquals((List<?>) collectionFieldTimestampRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(DELETED_ELEM_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals(
        (List<?>) collectionFieldTimestampRecord.get(DELETED_ELEM_TS_FIELD_NAME),
        Collections.emptyList());

    Assert.assertFalse(mergeConflictResult.isUpdateIgnored());
    ByteBuffer newValueOptional = mergeConflictResult.getNewValue();
    Assert.assertNotNull(newValueOptional);
    GenericRecord newValueRecord = getDeserializer(personSchemaV2, personSchemaV2).deserialize(newValueOptional);
    Assert.assertEquals(newValueRecord.get("age").toString(), "66");
    Assert.assertEquals(newValueRecord.get("name").toString(), "Venice");
    Assert.assertEquals(newValueRecord.get("intArray"), Arrays.asList(10, 20, 30, 40));
  }

  @Test
  public void testCollectionMerge() {
    final int incomingValueSchemaId = 3;
    final int incomingWriteComputeSchemaId = 3;
    final int oldValueSchemaId = 3;

    // Set up old/current value.
    GenericRecord oldValueRecord = AvroSchemaUtils.createGenericRecord(personSchemaV1);
    oldValueRecord.put("age", 30);
    oldValueRecord.put("name", "Kafka");
    oldValueRecord.put("intArray", Arrays.asList(1, 2, 3));
    Map<String, String> stringMap = new LinkedHashMap<>();
    stringMap.put("1", "one");
    stringMap.put("2", "two");
    oldValueRecord.put("stringMap", stringMap);
    ByteBuffer oldValueBytes =
        ByteBuffer.wrap(MapOrderPreservingSerDeFactory.getSerializer(personSchemaV1).serialize(oldValueRecord));

    // Set up Write Compute request.
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(personSchemaV1);
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(writeComputeSchema);
    updateBuilder.setNewFieldValue("age", 99);
    updateBuilder.setNewFieldValue("name", "Francisco");
    // Try to merge/add 3 numbers to the intArray list field.
    updateBuilder.setElementsToAddToListField("intArray", Arrays.asList(6, 7, 8));
    GenericRecord updateFieldRecord = updateBuilder.build();

    ByteBuffer writeComputeBytes =
        ByteBuffer.wrap(MapOrderPreservingSerDeFactory.getSerializer(writeComputeSchema).serialize(updateFieldRecord));

    // Set up current replication metadata.
    final long valueLevelTimestamp = 10L;
    Map<String, Long> fieldNameToTimestampMap = new HashMap<>();
    fieldNameToTimestampMap.put("age", valueLevelTimestamp);
    fieldNameToTimestampMap.put("name", valueLevelTimestamp);
    fieldNameToTimestampMap.put("intArray", valueLevelTimestamp);
    fieldNameToTimestampMap.put("stringMap", valueLevelTimestamp);
    Map<String, Integer> fieldNameToExistingPutPartLengthMap = new HashMap<>();
    fieldNameToExistingPutPartLengthMap.put("intArray", 3);
    fieldNameToExistingPutPartLengthMap.put("stringMap", 2);

    GenericRecord rmdRecord = createRmdWithFieldLevelTimestamp(
        personRmdSchemaV1,
        fieldNameToTimestampMap,
        fieldNameToExistingPutPartLengthMap);
    RmdWithValueSchemaId rmdWithValueSchemaId = new RmdWithValueSchemaId(oldValueSchemaId, RMD_VERSION_ID, rmdRecord);
    ReadOnlySchemaRepository readOnlySchemaRepository = mock(ReadOnlySchemaRepository.class);
    doReturn(new DerivedSchemaEntry(incomingValueSchemaId, 1, writeComputeSchema)).when(readOnlySchemaRepository)
        .getDerivedSchema(storeName, incomingValueSchemaId, incomingWriteComputeSchemaId);
    doReturn(new SchemaEntry(oldValueSchemaId, personSchemaV1)).when(readOnlySchemaRepository)
        .getValueSchema(storeName, oldValueSchemaId);
    doReturn(new SchemaEntry(oldValueSchemaId, personSchemaV1)).when(readOnlySchemaRepository)
        .getSupersetSchema(storeName);

    StringAnnotatedStoreSchemaCache stringAnnotatedStoreSchemaCache =
        new StringAnnotatedStoreSchemaCache(storeName, readOnlySchemaRepository);
    // Update happens below
    MergeConflictResolver mergeConflictResolver = MergeConflictResolverFactory.getInstance()
        .createMergeConflictResolver(
            stringAnnotatedStoreSchemaCache,
            new RmdSerDe(stringAnnotatedStoreSchemaCache, RMD_VERSION_ID),
            storeName);

    final int newColoID = 3;
    MergeConflictResult mergeConflictResult = mergeConflictResolver.update(
        Lazy.of(() -> oldValueBytes),
        rmdWithValueSchemaId,
        writeComputeBytes,
        incomingValueSchemaId,
        incomingWriteComputeSchemaId,
        valueLevelTimestamp + 1, // Slightly higher than existing timestamp. Thus update is NOT ignored.
        P1,
        1,
        newColoID,
        null);

    // Validate updated replication metadata.
    Assert.assertNotEquals(mergeConflictResult, MergeConflictResult.getIgnoredResult());
    GenericRecord updatedRmd = mergeConflictResult.getRmdRecord();
    Assert.assertEquals(
        (List<?>) updatedRmd.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME),
        Collections.emptyList());

    GenericRecord rmdTimestamp = (GenericRecord) updatedRmd.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(rmdTimestamp.get("age"), 11L);
    Assert.assertEquals(rmdTimestamp.get("name"), 11L);
    GenericRecord collectionFieldTimestampRecord = (GenericRecord) rmdTimestamp.get("intArray");
    Assert.assertEquals(
        (long) collectionFieldTimestampRecord.get(TOP_LEVEL_TS_FIELD_NAME),
        10L,
        "Collection top-level timestamp does not change because collection merge does not affect top-level timestamp");
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 3);
    Assert.assertEquals(
        (int) collectionFieldTimestampRecord.get(TOP_LEVEL_COLO_ID_FIELD_NAME),
        -1,
        "Collection top-level should NOT be changed by collection merge");
    Assert.assertEquals(
        (List<?>) collectionFieldTimestampRecord.get(ACTIVE_ELEM_TS_FIELD_NAME),
        Arrays.asList(11L, 11L, 11L));
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(DELETED_ELEM_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals(
        (List<?>) collectionFieldTimestampRecord.get(DELETED_ELEM_TS_FIELD_NAME),
        Collections.emptyList());

    collectionFieldTimestampRecord = (GenericRecord) rmdTimestamp.get("stringMap");
    Assert.assertEquals((long) collectionFieldTimestampRecord.get(TOP_LEVEL_TS_FIELD_NAME), 10L);
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 2);
    Assert.assertEquals((int) collectionFieldTimestampRecord.get(TOP_LEVEL_COLO_ID_FIELD_NAME), -1);
    Assert
        .assertEquals((List<?>) collectionFieldTimestampRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals((List<?>) collectionFieldTimestampRecord.get(DELETED_ELEM_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals(
        (List<?>) collectionFieldTimestampRecord.get(DELETED_ELEM_TS_FIELD_NAME),
        Collections.emptyList());

    // Validate updated value.
    Assert.assertNotNull(mergeConflictResult.getNewValue());
    ByteBuffer updatedValueBytes = mergeConflictResult.getNewValue();
    GenericRecord updatedValueRecord = MapOrderPreservingSerDeFactory.getDeserializer(personSchemaV1, personSchemaV1)
        .deserialize(updatedValueBytes.array());

    Assert.assertEquals(updatedValueRecord.get("age"), 99);
    Assert.assertEquals(updatedValueRecord.get("name").toString(), "Francisco");
    Assert.assertEquals(
        updatedValueRecord.get("intArray"),
        Arrays.asList(1, 2, 3, 6, 7, 8),
        "After applying collection (list) merge, the list field should contain all integers.");
    Map<Utf8, Utf8> updatedMapField = (Map<Utf8, Utf8>) updatedValueRecord.get("stringMap");
    Assert.assertEquals(updatedMapField.size(), 2);
    Assert.assertEquals(updatedMapField.get(toUtf8("1")), toUtf8("one"));
    Assert.assertEquals(updatedMapField.get(toUtf8("2")), toUtf8("two"));
  }
}
