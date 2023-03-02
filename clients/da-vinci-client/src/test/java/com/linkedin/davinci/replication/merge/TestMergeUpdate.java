package com.linkedin.davinci.replication.merge;

import static com.linkedin.davinci.replication.merge.TestMergeConflictResolver.RMD_VERSION_ID;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.ACTIVE_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.PUT_ONLY_PART_LENGTH_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;

import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.utils.IndexedHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestMergeUpdate extends TestMergeBase {
  @Test
  public void testUpdateIgnoredForRegularField() {
    GenericRecord partialUpdateRecord =
        new UpdateBuilderImpl(schemaSet.getUpdateSchema()).setNewFieldValue(REGULAR_FIELD_NAME, "newVenice").build();

    // Case 1: Apply UPDATE operation with smaller TS on value level RMD.
    GenericRecord oldValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "defaultVenice");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.emptyList());

      IndexedHashMap<String, Integer> indexedHashMap = new IndexedHashMap<>();
      indexedHashMap.put("key1", 1);
      indexedHashMap.put("key2", 1);

      r.put(INT_MAP_FIELD_NAME, indexedHashMap);
    });
    GenericRecord oldRmdRecord = initiateValueLevelRmdRecord(2);

    MergeConflictResult result = mergeConflictResolver.update(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeUpdateRecord(partialUpdateRecord),
        schemaSet.getValueSchemaId(),
        schemaSet.getUpdateSchemaProtocolVersion(),
        1L,
        1L,
        0,
        0);
    Assert.assertTrue(result.isUpdateIgnored());

    // Case 2: Apply UPDATE operation with smaller TS on field level RMD.

    oldRmdRecord = initiateValueLevelRmdRecord(2);
    result = mergeConflictResolver.update(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeUpdateRecord(partialUpdateRecord),
        schemaSet.getValueSchemaId(),
        schemaSet.getUpdateSchemaProtocolVersion(),
        1L,
        1L,
        0,
        0);
    Assert.assertTrue(result.isUpdateIgnored());
  }

  @Test
  public void testUpdateIgnoredForMapField() {
    IndexedHashMap<String, Integer> hashMap = new IndexedHashMap<>();
    hashMap.put("key1", 2);
    hashMap.put("key2", 2);
    GenericRecord partialUpdateRecord =
        new UpdateBuilderImpl(schemaSet.getUpdateSchema()).setNewFieldValue(INT_MAP_FIELD_NAME, hashMap).build();

    // Case 1: Apply UPDATE operation putMap with the same TS and smaller COLO ID on value level TS
    GenericRecord oldValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "defaultVenice");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.emptyList());
      IndexedHashMap<String, Integer> indexedHashMap = new IndexedHashMap<>();
      indexedHashMap.put("key1", 1);
      indexedHashMap.put("key2", 1);
      r.put(INT_MAP_FIELD_NAME, indexedHashMap);
    });
    GenericRecord oldRmdRecord = initiateValueLevelRmdRecord(2);
    MergeConflictResult result = mergeConflictResolver.update(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeUpdateRecord(partialUpdateRecord),
        schemaSet.getValueSchemaId(),
        schemaSet.getUpdateSchemaProtocolVersion(),
        2L,
        1L,
        0,
        -2);
    Assert.assertTrue(result.isUpdateIgnored());

    // Case 2: Apply UPDATE operation putMap with the same TS and smaller COLO ID on field level TS
    oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 2);
    result = mergeConflictResolver.update(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeUpdateRecord(partialUpdateRecord),
        schemaSet.getValueSchemaId(),
        schemaSet.getUpdateSchemaProtocolVersion(),
        2L,
        1L,
        0,
        -2);
    Assert.assertTrue(result.isUpdateIgnored());

    // Case 3: Apply UPDATE operation addToMap with the same TS as top level record
    partialUpdateRecord =
        new UpdateBuilderImpl(schemaSet.getUpdateSchema()).setEntriesToAddToMapField(INT_MAP_FIELD_NAME, hashMap)
            .build();
    initiateFieldLevelRmdRecord(oldValueRecord, 2);
    result = mergeConflictResolver.update(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeUpdateRecord(partialUpdateRecord),
        schemaSet.getValueSchemaId(),
        schemaSet.getUpdateSchemaProtocolVersion(),
        2L,
        1L,
        0,
        -2);
    Assert.assertTrue(result.isUpdateIgnored());

    // Case 4: Apply UPDATE operation addToMap with the same TS as activeTS but value is smaller.
    hashMap.put("key1", 0);
    hashMap.put("key2", 0);
    partialUpdateRecord =
        new UpdateBuilderImpl(schemaSet.getUpdateSchema()).setEntriesToAddToMapField(INT_MAP_FIELD_NAME, hashMap)
            .build();
    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord fieldTsRecord = (GenericRecord) timestampRecord.get(INT_MAP_FIELD_NAME);
    fieldTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    fieldTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 0);
    fieldTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 2L));

    result = mergeConflictResolver.update(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeUpdateRecord(partialUpdateRecord),
        schemaSet.getValueSchemaId(),
        schemaSet.getUpdateSchemaProtocolVersion(),
        2L,
        1L,
        0,
        -2);
    Assert.assertTrue(result.isUpdateIgnored());
  }

  @Test
  public void testAddEntriesToList() {
    GenericRecord partialUpdateRecord = new UpdateBuilderImpl(schemaSet.getUpdateSchema()).setElementsToAddToListField(
        STRING_ARRAY_FIELD_NAME,
        Arrays.asList("key1", "key2", "key3", "key4", "key5", "key6")).build();

    GenericRecord oldValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "defaultVenice");
      r.put(STRING_ARRAY_FIELD_NAME, Arrays.asList("key1", "key2", "key3"));
      r.put(INT_MAP_FIELD_NAME, new IndexedHashMap<>());
    });
    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 2);
    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord fieldTsRecord = (GenericRecord) timestampRecord.get(STRING_ARRAY_FIELD_NAME);
    fieldTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    fieldTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    fieldTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 3L));
    fieldTsRecord.put(DELETED_ELEM_FIELD_NAME, Arrays.asList("key4", "key5", "key6"));
    fieldTsRecord.put(DELETED_ELEM_TS_FIELD_NAME, Arrays.asList(1L, 2L, 3L));

    MergeConflictResult result = mergeConflictResolver.update(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeUpdateRecord(partialUpdateRecord),
        schemaSet.getValueSchemaId(),
        schemaSet.getUpdateSchemaProtocolVersion(),
        2L,
        1L,
        0,
        0);

    GenericRecord updatedValueRecord = deserializeValueRecord(result.getNewValue());
    Assert.assertEquals(
        updatedValueRecord.get(STRING_ARRAY_FIELD_NAME),
        Arrays.asList(new Utf8("key1"), new Utf8("key2"), new Utf8("key3"), new Utf8("key4")));
    GenericRecord updatedRmdTsRecord = (GenericRecord) result.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord updatedFieldTsRecord = (GenericRecord) updatedRmdTsRecord.get(STRING_ARRAY_FIELD_NAME);
    Assert.assertEquals(updatedFieldTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 1L);
    Assert.assertEquals(updatedFieldTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Arrays.asList(2L, 2L, 2L, 2L));
    Assert.assertEquals(updatedFieldTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 0);
    Assert.assertEquals(updatedFieldTsRecord.get(DELETED_ELEM_FIELD_NAME), Arrays.asList("key5", "key6"));
    fieldTsRecord.put(DELETED_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 3L));
  }

  @Test
  public void testAddEntriesToMap() {
    IndexedHashMap<String, Integer> hashMap = new IndexedHashMap<>();
    hashMap.put("key1", 2);
    hashMap.put("key2", 2);
    hashMap.put("key3", 2);
    hashMap.put("key4", 2);
    hashMap.put("key5", 2);
    GenericRecord partialUpdateRecord =
        new UpdateBuilderImpl(schemaSet.getUpdateSchema()).setEntriesToAddToMapField(INT_MAP_FIELD_NAME, hashMap)
            .build();

    GenericRecord oldValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "defaultVenice");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.emptyList());
      IndexedHashMap<String, Integer> indexedHashMap = new IndexedHashMap<>();
      indexedHashMap.put("key1", 1);
      indexedHashMap.put("key2", 1); // Value is smaller than incoming update.
      indexedHashMap.put("key3", 4); // Value is bigger than incoming update.
      indexedHashMap.put("key4", 1);
      r.put(INT_MAP_FIELD_NAME, indexedHashMap);
    });
    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 2);
    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord fieldTsRecord = (GenericRecord) timestampRecord.get(INT_MAP_FIELD_NAME);
    fieldTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    fieldTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    fieldTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 2L, 3L));

    MergeConflictResult result = mergeConflictResolver.update(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeUpdateRecord(partialUpdateRecord),
        schemaSet.getValueSchemaId(),
        schemaSet.getUpdateSchemaProtocolVersion(),
        2L,
        1L,
        0,
        0);

    IndexedHashMap<Utf8, Integer> expectedMap = new IndexedHashMap<>();
    expectedMap.put(new Utf8("key1"), 2);
    expectedMap.put(new Utf8("key2"), 2);
    expectedMap.put(new Utf8("key3"), 4);
    expectedMap.put(new Utf8("key4"), 1);
    expectedMap.put(new Utf8("key5"), 2);
    GenericRecord updatedValueRecord = deserializeValueRecord(result.getNewValue());
    Assert.assertEquals(updatedValueRecord.get(INT_MAP_FIELD_NAME), expectedMap);
    GenericRecord updatedRmdTsRecord = (GenericRecord) result.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord updatedFieldTsRecord = (GenericRecord) updatedRmdTsRecord.get(INT_MAP_FIELD_NAME);
    Assert.assertEquals(updatedFieldTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 1L);
    Assert.assertEquals(updatedFieldTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Arrays.asList(2L, 2L, 2L, 2L, 3L));
    Assert.assertEquals(updatedFieldTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 0);
  }

  @Test
  public void testRemoveKeysFromMap() {
    GenericRecord partialUpdateRecord = new UpdateBuilderImpl(schemaSet.getUpdateSchema())
        .setKeysToRemoveFromMapField(INT_MAP_FIELD_NAME, Arrays.asList("key1", "key2", "key3", "key4"))
        .build();

    GenericRecord oldValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "defaultVenice");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.emptyList());
      IndexedHashMap<String, Integer> indexedHashMap = new IndexedHashMap<>();
      indexedHashMap.put("key1", 1);
      indexedHashMap.put("key2", 1);
      indexedHashMap.put("key3", 1);
      r.put(INT_MAP_FIELD_NAME, indexedHashMap);
    });
    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 2);
    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord fieldTsRecord = (GenericRecord) timestampRecord.get(INT_MAP_FIELD_NAME);
    fieldTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    fieldTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    fieldTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 3L));

    MergeConflictResult result = mergeConflictResolver.update(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeUpdateRecord(partialUpdateRecord),
        schemaSet.getValueSchemaId(),
        schemaSet.getUpdateSchemaProtocolVersion(),
        2L,
        1L,
        0,
        0);

    IndexedHashMap<Utf8, Integer> expectedMap = new IndexedHashMap<>();
    expectedMap.put(new Utf8("key3"), 1);
    GenericRecord updatedValueRecord = deserializeValueRecord(result.getNewValue());
    Assert.assertEquals(updatedValueRecord.get(INT_MAP_FIELD_NAME), expectedMap);
    GenericRecord updatedRmdTsRecord = (GenericRecord) result.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord updatedFieldTsRecord = (GenericRecord) updatedRmdTsRecord.get(INT_MAP_FIELD_NAME);
    Assert.assertEquals(updatedFieldTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 1L);
    Assert.assertEquals(updatedFieldTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.singletonList(3L));
    Assert.assertEquals(updatedFieldTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 0);
    Assert.assertEquals(updatedFieldTsRecord.get(DELETED_ELEM_FIELD_NAME), Arrays.asList("key1", "key2", "key4"));
    Assert.assertEquals(updatedFieldTsRecord.get(DELETED_ELEM_TS_FIELD_NAME), Arrays.asList(2L, 2L, 2L));
  }
}
