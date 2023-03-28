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
import java.util.Collections;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * This class aims to test merge logic on {@link com.linkedin.venice.kafka.protocol.Put}.
 * It contains tests to check put handling logic under different settings.
 * TODO: Merge {@link TestMergePutWithFieldLevelTimestamp}
 */
public class TestMergePut extends TestMergeBase {
  /**
   * This test validates PUT updates full value of a key and RMD collapses into value-level TS.
   */
  @Test
  public void testPutResultsInFullRecordUpdate() {
    GenericRecord oldValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "defaultVenice");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.emptyList());
      r.put(NULLABLE_STRING_ARRAY_FIELD_NAME, Collections.emptyList());
      r.put(STRING_MAP_FIELD_NAME, Collections.emptyMap());
      r.put(NULLABLE_STRING_MAP_FIELD_NAME, Collections.emptyMap());
    });
    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 0);

    GenericRecord newValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "newString");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.singletonList("item1"));
      r.put(STRING_MAP_FIELD_NAME, Collections.singletonMap("key1", "1"));
      r.put(NULLABLE_STRING_ARRAY_FIELD_NAME, null);
      r.put(NULLABLE_STRING_MAP_FIELD_NAME, null);
    });

    MergeConflictResult result = mergeConflictResolver.put(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeValueRecord(newValueRecord),
        1L,
        schemaSet.getValueSchemaId(),
        1L,
        0,
        0);

    Assert.assertFalse(result.isUpdateIgnored());
    GenericRecord updatedValueRecord = deserializeValueRecord(result.getNewValue());
    Assert.assertEquals(updatedValueRecord.get(REGULAR_FIELD_NAME).toString(), "newString");
    Assert.assertEquals(
        updatedValueRecord.get(STRING_MAP_FIELD_NAME),
        Collections.singletonMap(new Utf8("key1"), new Utf8("1")));
    Assert.assertEquals(updatedValueRecord.get(STRING_ARRAY_FIELD_NAME), Collections.singletonList(new Utf8("item1")));
    Assert.assertNull(updatedValueRecord.get(NULLABLE_STRING_ARRAY_FIELD_NAME));
    Assert.assertNull(updatedValueRecord.get(NULLABLE_STRING_MAP_FIELD_NAME));
    GenericRecord rmdTimestampRecord = (GenericRecord) result.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(rmdTimestampRecord.get(REGULAR_FIELD_NAME), 1L);
    Assert
        .assertEquals(((GenericRecord) rmdTimestampRecord.get(STRING_MAP_FIELD_NAME)).get(TOP_LEVEL_TS_FIELD_NAME), 1L);
    Assert.assertEquals(
        ((GenericRecord) rmdTimestampRecord.get(STRING_MAP_FIELD_NAME)).get(TOP_LEVEL_COLO_ID_FIELD_NAME),
        0);
    Assert.assertEquals(
        ((GenericRecord) rmdTimestampRecord.get(STRING_ARRAY_FIELD_NAME)).get(TOP_LEVEL_TS_FIELD_NAME),
        1L);
    Assert.assertEquals(
        ((GenericRecord) rmdTimestampRecord.get(STRING_ARRAY_FIELD_NAME)).get(TOP_LEVEL_COLO_ID_FIELD_NAME),
        0);
  }

  /**
   * This test validates PUT get ignored.
   * 1. Case 1: PUT TS < all existing fields
   * 2. Case 2: PUT TS == Collection field TS but smaller COLO ID
   */
  @Test
  public void testPutIgnored() {
    // Case 1: Apply PUT operation with smaller TS
    GenericRecord oldValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "defaultVenice");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.emptyList());
      r.put(STRING_MAP_FIELD_NAME, new IndexedHashMap<>());
    });
    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 2);

    GenericRecord newValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "defaultVenice");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.singletonList("item1"));
      IndexedHashMap<String, String> hashMap = new IndexedHashMap<>();
      hashMap.put("key1", "1");
      r.put(STRING_MAP_FIELD_NAME, hashMap);
    });

    MergeConflictResult result = mergeConflictResolver.put(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeValueRecord(newValueRecord),
        1L,
        schemaSet.getValueSchemaId(),
        1L,
        0,
        0);
    Assert.assertTrue(result.isUpdateIgnored());

    // Case2: Apply PUT operation with same TS but less colo ID
    result = mergeConflictResolver.put(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeValueRecord(newValueRecord),
        2L,
        schemaSet.getValueSchemaId(),
        1L,
        0,
        -2);
    Assert.assertTrue(result.isUpdateIgnored());
  }

  /**
   * This test validates PUT applied on regular field only:
   * 1. Case 1: Regular field applied, collection field not applied due to smaller TS
   * 2. Case 2: Regular field applied, collection field not applied due to same TS but smaller colo ID
   */
  @Test
  public void testPutResultsInPartialUpdateForRegularField() {
    // Case 1: Regular field is updated, but collection field is not due to smaller top level TS.
    GenericRecord oldValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "defaultVenice");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.singletonList("item1"));
      IndexedHashMap<String, String> hashMap = new IndexedHashMap<>();
      hashMap.put("key1", "1");
      r.put(STRING_MAP_FIELD_NAME, hashMap);
    });
    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 2);
    setRegularFieldTimestamp(oldRmdRecord, 1);

    GenericRecord newValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "newString");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.singletonList("item2"));
      IndexedHashMap<String, String> hashMap = new IndexedHashMap<>();
      hashMap.put("key1", "2");
      r.put(STRING_MAP_FIELD_NAME, hashMap);
    });

    MergeConflictResult result = mergeConflictResolver.put(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeValueRecord(newValueRecord),
        1L,
        schemaSet.getValueSchemaId(),
        1L,
        0,
        -1);

    Assert.assertFalse(result.isUpdateIgnored());
    GenericRecord updatedValueRecord = deserializeValueRecord(result.getNewValue());
    Assert.assertEquals(updatedValueRecord.get(REGULAR_FIELD_NAME).toString(), "newString");
    Assert.assertEquals(
        updatedValueRecord.get(STRING_MAP_FIELD_NAME),
        Collections.singletonMap(new Utf8("key1"), new Utf8("1")));
    Assert.assertEquals(updatedValueRecord.get(STRING_ARRAY_FIELD_NAME), Collections.singletonList(new Utf8("item1")));

    GenericRecord updatedRmdTsRecord = (GenericRecord) result.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(updatedRmdTsRecord.get(REGULAR_FIELD_NAME), 1L);
    Assert.assertEquals(
        ((GenericRecord) updatedRmdTsRecord.get(STRING_ARRAY_FIELD_NAME)).get(TOP_LEVEL_TS_FIELD_NAME),
        2L);
    Assert
        .assertEquals(((GenericRecord) updatedRmdTsRecord.get(STRING_MAP_FIELD_NAME)).get(TOP_LEVEL_TS_FIELD_NAME), 2L);

    // Case 2: Regular field is updated, but collection field is not due to same top level TS but smaller top level colo
    // ID.
    newValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "newString2");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.singletonList("item2"));
      r.put(STRING_MAP_FIELD_NAME, Collections.singletonMap("key1", "2"));
    });

    result = mergeConflictResolver.put(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeValueRecord(newValueRecord),
        2L,
        schemaSet.getValueSchemaId(),
        1L,
        0,
        -2);
    Assert.assertFalse(result.isUpdateIgnored());
    updatedValueRecord = deserializeValueRecord(result.getNewValue());
    Assert.assertEquals(updatedValueRecord.get(REGULAR_FIELD_NAME).toString(), "newString2");
    Assert.assertEquals(
        updatedValueRecord.get(STRING_MAP_FIELD_NAME),
        Collections.singletonMap(new Utf8("key1"), new Utf8("1")));
    Assert.assertEquals(updatedValueRecord.get(STRING_ARRAY_FIELD_NAME), Collections.singletonList(new Utf8("item1")));

    updatedRmdTsRecord = (GenericRecord) result.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(updatedRmdTsRecord.get(REGULAR_FIELD_NAME), 2L);
    Assert.assertEquals(
        ((GenericRecord) updatedRmdTsRecord.get(STRING_ARRAY_FIELD_NAME)).get(TOP_LEVEL_TS_FIELD_NAME),
        2L);
    Assert
        .assertEquals(((GenericRecord) updatedRmdTsRecord.get(STRING_MAP_FIELD_NAME)).get(TOP_LEVEL_TS_FIELD_NAME), 2L);
  }

  /**
   * This test validates PUT applied on collection field only.
   */
  @Test
  public void testPutResultsInPartialUpdateForCollectionMergeField() {
    // Case 1: Map field partial updated.
    GenericRecord oldValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "defaultVenice");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.singletonList("item1"));
      IndexedHashMap<String, String> map = new IndexedHashMap<>();
      map.put("key1", "3");
      map.put("key2", "3");
      r.put(STRING_MAP_FIELD_NAME, map);
    });
    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 2);
    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord fieldTsRecord = (GenericRecord) timestampRecord.get(STRING_MAP_FIELD_NAME);
    fieldTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    fieldTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    fieldTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Collections.singletonList(3L));

    GenericRecord newValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "defaultVenice");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.singletonList("item1"));
      IndexedHashMap<String, String> map = new IndexedHashMap<>();
      map.put("key1", "2");
      map.put("key2", "2");
      r.put(STRING_MAP_FIELD_NAME, map);
    });

    MergeConflictResult result = mergeConflictResolver.put(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeValueRecord(newValueRecord),
        2L,
        schemaSet.getValueSchemaId(),
        1L,
        0,
        -2);
    Assert.assertFalse(result.isUpdateIgnored());
    GenericRecord updatedValueRecord = deserializeValueRecord(result.getNewValue());
    IndexedHashMap<Utf8, Utf8> expectedMap = new IndexedHashMap<>();
    expectedMap.put(new Utf8("key1"), new Utf8("2"));
    expectedMap.put(new Utf8("key2"), new Utf8("3"));
    Assert.assertEquals(updatedValueRecord.get(STRING_MAP_FIELD_NAME), expectedMap);

    GenericRecord updatedRmdTsRecord = (GenericRecord) result.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME);
    Assert
        .assertEquals(((GenericRecord) updatedRmdTsRecord.get(STRING_MAP_FIELD_NAME)).get(TOP_LEVEL_TS_FIELD_NAME), 2L);
    Assert.assertEquals(
        ((GenericRecord) updatedRmdTsRecord.get(STRING_MAP_FIELD_NAME)).get(ACTIVE_ELEM_TS_FIELD_NAME),
        Collections.singletonList(3L));

    newValueRecord = createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "defaultVenice");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.singletonList("item1"));
      IndexedHashMap<String, String> map = new IndexedHashMap<>();
      map.put("key1", "1");
      map.put("key2", "1");
      r.put(STRING_MAP_FIELD_NAME, map);
    });
    setRegularFieldTimestamp(oldRmdRecord, 10L);

    result = mergeConflictResolver.put(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeValueRecord(newValueRecord),
        3L,
        schemaSet.getValueSchemaId(),
        1L,
        0,
        0);
    Assert.assertFalse(result.isUpdateIgnored());
    updatedValueRecord = deserializeValueRecord(result.getNewValue());
    expectedMap = new IndexedHashMap<>();
    expectedMap.put(new Utf8("key1"), new Utf8("1"));
    expectedMap.put(new Utf8("key2"), new Utf8("1"));
    Assert.assertEquals(updatedValueRecord.get(STRING_MAP_FIELD_NAME), expectedMap);

    updatedRmdTsRecord = (GenericRecord) result.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME);
    Assert
        .assertEquals(((GenericRecord) updatedRmdTsRecord.get(STRING_MAP_FIELD_NAME)).get(TOP_LEVEL_TS_FIELD_NAME), 3L);
    Assert.assertEquals(
        ((GenericRecord) updatedRmdTsRecord.get(STRING_MAP_FIELD_NAME)).get(ACTIVE_ELEM_TS_FIELD_NAME),
        Collections.emptyList());
  }
}
