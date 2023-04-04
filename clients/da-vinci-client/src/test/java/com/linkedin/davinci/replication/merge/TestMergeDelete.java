package com.linkedin.davinci.replication.merge;

import static com.linkedin.davinci.replication.merge.TestMergeConflictResolver.RMD_VERSION_ID;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.ACTIVE_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.PUT_ONLY_PART_LENGTH_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_COLO_ID_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;

import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.utils.IndexedHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * This class aims to test merge logic on {@link com.linkedin.venice.kafka.protocol.Delete}.
 * It contains tests to check delete handling logic under different settings.
 * TODO: Merge {@link TestMergeDeleteWithFieldLevelTimestamp}
 */
public class TestMergeDelete extends TestMergeBase {
  /**
   * This test validates DELETE removes full value of a key and RMD collapses into value-level TS.
   */
  @Test
  public void testFullDelete() {
    GenericRecord oldValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "defaultVenice");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.emptyList());
      IndexedHashMap<String, String> mapValue = new IndexedHashMap<>();
      mapValue.put("key1", "1");
      mapValue.put("key2", "1");
      r.put(STRING_MAP_FIELD_NAME, mapValue);
    });
    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 1);
    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord fieldTsRecord = (GenericRecord) timestampRecord.get(STRING_MAP_FIELD_NAME);
    fieldTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    fieldTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    fieldTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Collections.singletonList(2L));

    MergeConflictResult result = mergeConflictResolver.delete(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        2L,
        1L,
        0,
        0);

    Assert.assertFalse(result.isUpdateIgnored());
    Assert.assertNull(result.getNewValue());
    GenericRecord rmdTimestampRecord = (GenericRecord) result.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(rmdTimestampRecord.get(REGULAR_FIELD_NAME), 2L);
    Assert
        .assertEquals(((GenericRecord) rmdTimestampRecord.get(STRING_MAP_FIELD_NAME)).get(TOP_LEVEL_TS_FIELD_NAME), 2L);
    Assert.assertEquals(
        ((GenericRecord) rmdTimestampRecord.get(STRING_MAP_FIELD_NAME)).get(TOP_LEVEL_COLO_ID_FIELD_NAME),
        0);
    Assert.assertEquals(
        ((GenericRecord) rmdTimestampRecord.get(STRING_ARRAY_FIELD_NAME)).get(TOP_LEVEL_TS_FIELD_NAME),
        2L);
    Assert.assertEquals(
        ((GenericRecord) rmdTimestampRecord.get(STRING_ARRAY_FIELD_NAME)).get(TOP_LEVEL_COLO_ID_FIELD_NAME),
        0);
  }

  /**
   * This test validates DELETE removes part of the value of a key.
   * 1. Case 1: Delete regular field
   * 2. Case 2: Delete collection field
   */
  @Test
  public void testPartiallyDelete() {
    // Case 1: Partially delete regular field.
    IndexedHashMap<String, String> initialIntMap = new IndexedHashMap<>();
    initialIntMap.put("key1", "1");
    initialIntMap.put("key2", "1");
    initialIntMap.put("key3", "1");
    GenericRecord oldValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "DaVinci");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.emptyList());
      r.put(STRING_MAP_FIELD_NAME, initialIntMap);
    });
    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 1);
    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord fieldTsRecord = (GenericRecord) timestampRecord.get(STRING_MAP_FIELD_NAME);
    fieldTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 3L);
    fieldTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    fieldTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(4L, 5L));

    MergeConflictResult result = mergeConflictResolver.delete(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        2L,
        1L,
        0,
        0);

    Assert.assertFalse(result.isUpdateIgnored());
    GenericRecord updatedValueRecord = deserializeValueRecord(result.getNewValue());
    Assert.assertEquals(updatedValueRecord.get(REGULAR_FIELD_NAME), new Utf8("defaultVenice"));
    IndexedHashMap<Utf8, Utf8> utf8Map = new IndexedHashMap<>();
    utf8Map.put(new Utf8("key1"), new Utf8("1"));
    utf8Map.put(new Utf8("key2"), new Utf8("1"));
    utf8Map.put(new Utf8("key3"), new Utf8("1"));
    Assert.assertEquals(updatedValueRecord.get(STRING_MAP_FIELD_NAME), utf8Map);
    GenericRecord updatedRmdTsRecord = (GenericRecord) result.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(updatedRmdTsRecord.get(REGULAR_FIELD_NAME), 2L);
    GenericRecord updatedFieldTsRecord = (GenericRecord) updatedRmdTsRecord.get(STRING_MAP_FIELD_NAME);
    Assert.assertEquals(updatedFieldTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 3L);
    Assert.assertEquals(updatedFieldTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Arrays.asList(4L, 5L));
    Assert.assertEquals(updatedFieldTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 1);

    // Case 2: Partially delete collection merge field.
    result = mergeConflictResolver.delete(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        4L,
        1L,
        0,
        -1); // Default colo ID = -1, any coloID >= -1 will be accepted.
    Assert.assertFalse(result.isUpdateIgnored());
    updatedValueRecord = deserializeValueRecord(result.getNewValue());
    Assert.assertEquals(updatedValueRecord.get(REGULAR_FIELD_NAME), new Utf8("defaultVenice"));
    utf8Map = new IndexedHashMap<>();
    utf8Map.put(new Utf8("key3"), new Utf8("1"));
    Assert.assertEquals(updatedValueRecord.get(STRING_MAP_FIELD_NAME), utf8Map);
    updatedRmdTsRecord = (GenericRecord) result.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(updatedRmdTsRecord.get(REGULAR_FIELD_NAME), 4L);
    updatedFieldTsRecord = (GenericRecord) updatedRmdTsRecord.get(STRING_MAP_FIELD_NAME);
    Assert.assertEquals(updatedFieldTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 4L);
    Assert.assertEquals(updatedFieldTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.singletonList(5L));
    Assert.assertEquals(updatedFieldTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 0);
  }

  /**
   * This test validates DELETE get ignored.
   * 1. Case 1: DELETE TS < all existing fields
   * 2. Case 2: DELETE TS == Collection field TS but smaller COLO ID
   * 3. Case 3: DELETE TS > TOP-LEVEL TS but smaller than active elements (No put-only element exists)
   */
  @Test
  public void testDeleteIgnored() {
    // Case 1: Delete ignored due to smaller TS in all fields
    IndexedHashMap<String, String> initialIntMap = new IndexedHashMap<>();
    initialIntMap.put("key1", "1");
    initialIntMap.put("key2", "1");
    initialIntMap.put("key3", "1");
    GenericRecord oldValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "DaVinci");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.emptyList());
      r.put(STRING_MAP_FIELD_NAME, initialIntMap);
    });
    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 3);
    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord fieldTsRecord = (GenericRecord) timestampRecord.get(STRING_MAP_FIELD_NAME);
    fieldTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 3L);
    fieldTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    fieldTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(4L, 5L));

    MergeConflictResult result = mergeConflictResolver.delete(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        2L,
        1L,
        0,
        0);
    Assert.assertTrue(result.isUpdateIgnored());

    // Case 2: Delete ignored as collection field has same TS but smaller colo ID.
    oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 4);
    timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    fieldTsRecord = (GenericRecord) timestampRecord.get(STRING_MAP_FIELD_NAME);
    fieldTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 3L);
    fieldTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    fieldTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(4L, 5L));
    result = mergeConflictResolver.delete(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        3L,
        1L,
        0,
        -2);
    Assert.assertTrue(result.isUpdateIgnored());

    // Case 3: Delete ignored as collection field has smaller top level TS but active elements has higher TS.
    oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 4);
    timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    fieldTsRecord = (GenericRecord) timestampRecord.get(STRING_MAP_FIELD_NAME);
    fieldTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 2L);
    fieldTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 0);
    fieldTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(4L, 4L, 4L));
    result = mergeConflictResolver.delete(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        3L,
        1L,
        0,
        -2);
    Assert.assertTrue(result.isUpdateIgnored());
  }
}
