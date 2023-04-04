package com.linkedin.davinci.replication.merge;

import static com.linkedin.davinci.replication.merge.TestMergeConflictResolver.RMD_VERSION_ID;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.ACTIVE_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.PUT_ONLY_PART_LENGTH_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;

import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IndexedHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * This class aims to test merge logic on {@link com.linkedin.venice.kafka.protocol.Update}.
 * It contains tests to check field partial update and collection merge operations handling logic under different settings.
 * TODO: Merge {@link TestMergeUpdateWithValueLevelTimestamp} and {@link TestMergeDeleteWithFieldLevelTimestamp}.
 */
public class TestMergeUpdate extends TestMergeBase {
  /**
   * This test validates:
   * 1. SetField UPDATE to regular field with smaller TS on field-level / value-level RMD.
   * 2. SetField UPDATE to regular field with same TS but smaller value on field-level / value-level RMD.
   */
  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testRegularFieldUpdateIgnored(boolean rmdStartsWithFieldLevelTs, boolean isUpdateTsSmallerThanRmdTs) {
    long baselineRmdTs = 2L;
    GenericRecord partialUpdateRecord =
        new UpdateBuilderImpl(schemaSet.getUpdateSchema()).setNewFieldValue(REGULAR_FIELD_NAME, "Da Vinci").build();
    GenericRecord oldValueRecord = createDefaultValueRecord();
    GenericRecord oldRmdRecord = rmdStartsWithFieldLevelTs
        ? initiateFieldLevelRmdRecord(oldValueRecord, baselineRmdTs)
        : initiateValueLevelRmdRecord(baselineRmdTs);

    long updateTs = isUpdateTsSmallerThanRmdTs ? baselineRmdTs - 1 : baselineRmdTs;
    MergeConflictResult result = mergeConflictResolver.update(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeUpdateRecord(partialUpdateRecord),
        schemaSet.getValueSchemaId(),
        schemaSet.getUpdateSchemaProtocolVersion(),
        updateTs,
        1L,
        0,
        0);
    Assert.assertTrue(result.isUpdateIgnored());
  }

  /**
   * This test validates:
   * 1. SetField UPDATE to collection field with smaller TS on field-level / value-level RMD.
   * 2. SetField UPDATE to collection field with same TS but smaller colo ID on field-level / value-level RMD.
   */
  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testCollectionFieldSetFieldUpdateIgnored(
      boolean rmdStartsWithFieldLevelTs,
      boolean isUpdateTsSmallerThanRmdTs) {
    long baselineRmdTs = 2L;
    int baselineColoID = -1;
    IndexedHashMap<String, String> updateMapValue = new IndexedHashMap<>();
    updateMapValue.put("key1", "2");
    updateMapValue.put("key2", "2");
    List<String> updateListValue = Arrays.asList("item1", "item2");
    GenericRecord partialUpdateRecord =
        new UpdateBuilderImpl(schemaSet.getUpdateSchema()).setNewFieldValue(STRING_MAP_FIELD_NAME, updateMapValue)
            .setNewFieldValue(STRING_ARRAY_FIELD_NAME, updateListValue)
            .build();

    GenericRecord oldValueRecord = createDefaultValueRecord();
    IndexedHashMap<String, String> initialMapValue = new IndexedHashMap<>();
    initialMapValue.put("key1", "1");
    initialMapValue.put("key2", "1");
    oldValueRecord.put(STRING_MAP_FIELD_NAME, initialMapValue);
    oldValueRecord.put(STRING_ARRAY_FIELD_NAME, Arrays.asList("item0", "item1"));

    GenericRecord oldRmdRecord = rmdStartsWithFieldLevelTs
        ? initiateFieldLevelRmdRecord(oldValueRecord, baselineRmdTs)
        : initiateValueLevelRmdRecord(baselineRmdTs);
    long updateTs = isUpdateTsSmallerThanRmdTs ? baselineRmdTs - 1 : baselineRmdTs;
    int updateColoId = isUpdateTsSmallerThanRmdTs ? baselineColoID : baselineColoID - 1;
    MergeConflictResult result = mergeConflictResolver.update(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeUpdateRecord(partialUpdateRecord),
        schemaSet.getValueSchemaId(),
        schemaSet.getUpdateSchemaProtocolVersion(),
        updateTs,
        1L,
        0,
        updateColoId);
    Assert.assertTrue(result.isUpdateIgnored());
  }

  /**
   * This test validates:
   * 1. AddToMap/AddToList UPDATE to collection field with smaller TS than putOnly / active element on field-level TS.
   * 2. AddToMap/AddToList UPDATE to collection field with same TS but (smaller value than / same item as) putOnly / active element on field-level TS.
   */
  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testCollectionFieldAddElementsUpdateIgnoredOnFieldLevelTs(
      boolean updateOnActiveElement,
      boolean isUpdateTsSmallerThanElementTs) {
    long baselineRmdTs = 2L;
    IndexedHashMap<String, String> addToMapValue = new IndexedHashMap<>();
    List<String> addToListValue;
    if (updateOnActiveElement) {
      addToMapValue.put("key2", "0");
      addToListValue = Collections.singletonList("item2");
    } else {
      addToMapValue.put("key1", "0");
      addToListValue = Collections.singletonList("item1");
    }

    GenericRecord partialUpdateRecord = new UpdateBuilderImpl(schemaSet.getUpdateSchema())
        .setElementsToAddToListField(STRING_ARRAY_FIELD_NAME, addToListValue)
        .setEntriesToAddToMapField(STRING_MAP_FIELD_NAME, addToMapValue)
        .build();

    GenericRecord oldValueRecord = createDefaultValueRecord();
    IndexedHashMap<String, String> initialMapValue = new IndexedHashMap<>();
    initialMapValue.put("key1", "1");
    initialMapValue.put("key2", "1");
    oldValueRecord.put(STRING_MAP_FIELD_NAME, initialMapValue);
    oldValueRecord.put(STRING_ARRAY_FIELD_NAME, Arrays.asList("item1", "item2"));
    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, baselineRmdTs);

    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord mapTsRecord = (GenericRecord) timestampRecord.get(STRING_MAP_FIELD_NAME);
    mapTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, baselineRmdTs);
    mapTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    mapTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Collections.singletonList(baselineRmdTs + 1));
    GenericRecord listTsRecord = (GenericRecord) timestampRecord.get(STRING_ARRAY_FIELD_NAME);
    listTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, baselineRmdTs);
    listTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    listTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Collections.singletonList(baselineRmdTs + 1));

    long updateTs = updateOnActiveElement ? baselineRmdTs + 1 : baselineRmdTs;
    if (isUpdateTsSmallerThanElementTs) {
      updateTs -= 1;
    }
    MergeConflictResult result = mergeConflictResolver.update(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeUpdateRecord(partialUpdateRecord),
        schemaSet.getValueSchemaId(),
        schemaSet.getUpdateSchemaProtocolVersion(),
        updateTs,
        1L,
        0,
        -2);
    Assert.assertTrue(result.isUpdateIgnored());
  }

  /**
   * This test validates:
   * 1. RemoveFromMap/RemoveFromList UPDATE to collection field with smaller TS than putOnly / active element on field-level TS.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testCollectionFieldRemoveElementsUpdateIgnoredOnFieldLevelTs(boolean updateOnActiveElement) {
    long baselineRmdTs = 2L;
    List<String> removeFromMapValue;
    List<String> removeFromListValue;
    if (updateOnActiveElement) {
      removeFromMapValue = Arrays.asList("key2", "key3");
      removeFromListValue = Arrays.asList("item2", "item3");
    } else {
      removeFromMapValue = Collections.singletonList("key1");
      removeFromListValue = Collections.singletonList("item1");
    }

    GenericRecord partialUpdateRecord = new UpdateBuilderImpl(schemaSet.getUpdateSchema())
        .setElementsToRemoveFromListField(STRING_ARRAY_FIELD_NAME, removeFromListValue)
        .setKeysToRemoveFromMapField(STRING_MAP_FIELD_NAME, removeFromMapValue)
        .build();

    GenericRecord oldValueRecord = createDefaultValueRecord();
    IndexedHashMap<String, String> initialMapValue = new IndexedHashMap<>();
    initialMapValue.put("key1", "1");
    initialMapValue.put("key2", "1");
    oldValueRecord.put(STRING_MAP_FIELD_NAME, initialMapValue);
    oldValueRecord.put(STRING_ARRAY_FIELD_NAME, Arrays.asList("item1", "item2"));
    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, baselineRmdTs);

    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord mapTsRecord = (GenericRecord) timestampRecord.get(STRING_MAP_FIELD_NAME);
    mapTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, baselineRmdTs);
    mapTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    mapTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Collections.singletonList(baselineRmdTs + 1));
    mapTsRecord.put(DELETED_ELEM_TS_FIELD_NAME, Collections.singletonList(baselineRmdTs + 1));
    mapTsRecord.put(DELETED_ELEM_FIELD_NAME, Collections.singletonList("key3"));
    GenericRecord listTsRecord = (GenericRecord) timestampRecord.get(STRING_ARRAY_FIELD_NAME);
    listTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, baselineRmdTs);
    listTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    listTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Collections.singletonList(baselineRmdTs + 1));
    listTsRecord.put(DELETED_ELEM_TS_FIELD_NAME, Collections.singletonList(baselineRmdTs + 1));
    listTsRecord.put(DELETED_ELEM_FIELD_NAME, Collections.singletonList("item3"));

    long updateTs = updateOnActiveElement ? baselineRmdTs : baselineRmdTs - 1;
    MergeConflictResult result = mergeConflictResolver.update(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeUpdateRecord(partialUpdateRecord),
        schemaSet.getValueSchemaId(),
        schemaSet.getUpdateSchemaProtocolVersion(),
        updateTs,
        1L,
        0,
        -2);
    Assert.assertTrue(result.isUpdateIgnored());
  }

  /**
   * This test validates:
   * 1. AddToMap/AddToList UPDATE to collection field with smaller TS on value-level TS.
   * 2. AddToMap/AddToList UPDATE to collection field with same TS on value-level TS.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testMapFieldAddEntriesUpdateIgnoredOnValueLevelTs(boolean updateTsSmallerThanRmdTs) {
    long baselineRmdTs = 2L;

    IndexedHashMap<String, String> addToMapValue = new IndexedHashMap<>();
    addToMapValue.put("key1", "2");
    addToMapValue.put("key2", "2");
    List<String> addToListValue = Collections.singletonList("item1");
    GenericRecord partialUpdateRecord = new UpdateBuilderImpl(schemaSet.getUpdateSchema())
        .setElementsToAddToListField(STRING_ARRAY_FIELD_NAME, addToListValue)
        .setEntriesToAddToMapField(STRING_MAP_FIELD_NAME, addToMapValue)
        .build();

    GenericRecord oldValueRecord = createDefaultValueRecord();
    IndexedHashMap<String, String> initialMapValue = new IndexedHashMap<>();
    initialMapValue.put("key1", "1");
    initialMapValue.put("key2", "1");
    oldValueRecord.put(STRING_MAP_FIELD_NAME, initialMapValue);
    oldValueRecord.put(STRING_ARRAY_FIELD_NAME, Collections.singletonList("item2"));
    GenericRecord oldRmdRecord = initiateValueLevelRmdRecord(baselineRmdTs);
    long updateTs = updateTsSmallerThanRmdTs ? baselineRmdTs - 1 : baselineRmdTs;
    MergeConflictResult result = mergeConflictResolver.update(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeUpdateRecord(partialUpdateRecord),
        schemaSet.getValueSchemaId(),
        schemaSet.getUpdateSchemaProtocolVersion(),
        updateTs,
        1L,
        0,
        -2);
    Assert.assertTrue(result.isUpdateIgnored());
  }

  @Test
  public void testSetCollectionFieldOnFieldValueTs() {
    IndexedHashMap<String, String> updateMapValue = new IndexedHashMap<>();
    updateMapValue.put("key1", "2");
    updateMapValue.put("key2", "2");
    updateMapValue.put("key3", "2");
    updateMapValue.put("key4", "2");
    updateMapValue.put("key5", "2");
    updateMapValue.put("key6", "2");
    GenericRecord partialUpdateRecord =
        new UpdateBuilderImpl(schemaSet.getUpdateSchema()).setNewFieldValue(STRING_MAP_FIELD_NAME, updateMapValue)
            .setNewFieldValue(STRING_ARRAY_FIELD_NAME, Arrays.asList("key1", "key2", "key3", "key4", "key5", "key6"))
            .setNewFieldValue(NULLABLE_STRING_MAP_FIELD_NAME, null)
            .setNewFieldValue(NULLABLE_STRING_ARRAY_FIELD_NAME, null)
            .build();

    GenericRecord oldValueRecord = createDefaultValueRecord();
    List<String> listValue = Arrays.asList("key1", "key2", "key3");
    oldValueRecord.put(STRING_ARRAY_FIELD_NAME, listValue);
    oldValueRecord.put(NULLABLE_STRING_ARRAY_FIELD_NAME, listValue);
    IndexedHashMap<String, String> mapValue = new IndexedHashMap<>();
    mapValue.put("key1", "1"); // Put Only Part
    mapValue.put("key2", "1"); // Same TS, value is smaller than incoming update.
    mapValue.put("key3", "4"); // Same TS, value is bigger than incoming update.
    mapValue.put("key4", "1"); // Higher TS
    oldValueRecord.put(STRING_MAP_FIELD_NAME, mapValue);
    oldValueRecord.put(NULLABLE_STRING_MAP_FIELD_NAME, mapValue);

    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 2);
    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord listTsRecord = (GenericRecord) timestampRecord.get(STRING_ARRAY_FIELD_NAME);
    listTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    listTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    listTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 3L));
    listTsRecord.put(DELETED_ELEM_FIELD_NAME, Arrays.asList("key4", "key5"));
    listTsRecord.put(DELETED_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 3L));

    GenericRecord nullableListTsRecord = (GenericRecord) timestampRecord.get(NULLABLE_STRING_ARRAY_FIELD_NAME);
    nullableListTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    nullableListTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    nullableListTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 3L));
    nullableListTsRecord.put(DELETED_ELEM_FIELD_NAME, Arrays.asList("key4", "key5"));
    nullableListTsRecord.put(DELETED_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 3L));

    GenericRecord mapTsRecord = (GenericRecord) timestampRecord.get(STRING_MAP_FIELD_NAME);
    mapTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    mapTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    mapTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 2L, 3L));
    mapTsRecord.put(DELETED_ELEM_FIELD_NAME, Arrays.asList("key5", "key6"));
    mapTsRecord.put(DELETED_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 3L));

    GenericRecord nullableMapTsRecord = (GenericRecord) timestampRecord.get(NULLABLE_STRING_MAP_FIELD_NAME);
    nullableMapTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    nullableMapTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    nullableMapTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 2L, 3L));
    nullableMapTsRecord.put(DELETED_ELEM_FIELD_NAME, Arrays.asList("key5", "key6"));
    nullableMapTsRecord.put(DELETED_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 3L));

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
        updatedValueRecord.get(NULLABLE_STRING_ARRAY_FIELD_NAME),
        Collections.singletonList(new Utf8("key3")));
    IndexedHashMap<Utf8, Utf8> expectedNullableMap = new IndexedHashMap<>();
    expectedNullableMap.put(new Utf8("key4"), new Utf8("1"));
    Assert.assertEquals(updatedValueRecord.get(NULLABLE_STRING_MAP_FIELD_NAME), expectedNullableMap);
    Assert.assertEquals(
        updatedValueRecord.get(STRING_ARRAY_FIELD_NAME),
        Arrays.asList(new Utf8("key1"), new Utf8("key2"), new Utf8("key6"), new Utf8("key3")));

    IndexedHashMap<Utf8, Utf8> expectedMap = new IndexedHashMap<>();
    expectedMap.put(new Utf8("key1"), new Utf8("2"));
    expectedMap.put(new Utf8("key2"), new Utf8("2"));
    expectedMap.put(new Utf8("key3"), new Utf8("2"));
    expectedMap.put(new Utf8("key4"), new Utf8("1"));

    Assert.assertEquals(updatedValueRecord.get(STRING_MAP_FIELD_NAME), expectedMap);

    GenericRecord updateRmdRecord = result.getRmdRecord();
    GenericRecord updatedRmdTsRecord = (GenericRecord) updateRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord updatedListTsRecord = (GenericRecord) updatedRmdTsRecord.get(STRING_ARRAY_FIELD_NAME);
    Assert.assertEquals(updatedListTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 2L);
    Assert.assertEquals(updatedListTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 3);
    Assert.assertEquals(updatedListTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.singletonList(3L));
    Assert.assertEquals(updatedListTsRecord.get(DELETED_ELEM_FIELD_NAME), Arrays.asList("key4", "key5"));
    Assert.assertEquals(updatedListTsRecord.get(DELETED_ELEM_TS_FIELD_NAME), Arrays.asList(2L, 3L));

    GenericRecord updatedMapTsRecord = (GenericRecord) updatedRmdTsRecord.get(STRING_MAP_FIELD_NAME);
    Assert.assertEquals(updatedMapTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 2L);
    Assert.assertEquals(updatedMapTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 3);
    Assert.assertEquals(updatedMapTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.singletonList(3L));
    Assert.assertEquals(updatedMapTsRecord.get(DELETED_ELEM_FIELD_NAME), Arrays.asList("key5", "key6"));
    Assert.assertEquals(updatedMapTsRecord.get(DELETED_ELEM_TS_FIELD_NAME), Arrays.asList(2L, 3L));

    GenericRecord updatedNullableListTsRecord =
        (GenericRecord) updatedRmdTsRecord.get(NULLABLE_STRING_ARRAY_FIELD_NAME);
    Assert.assertEquals(updatedNullableListTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 2L);
    Assert.assertEquals(updatedNullableListTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 0);
    Assert.assertEquals(updatedNullableListTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.singletonList(3L));
    Assert.assertEquals(updatedNullableListTsRecord.get(DELETED_ELEM_FIELD_NAME), Arrays.asList("key4", "key5"));
    Assert.assertEquals(updatedNullableListTsRecord.get(DELETED_ELEM_TS_FIELD_NAME), Arrays.asList(2L, 3L));

    GenericRecord updatedNullableMapTsRecord = (GenericRecord) updatedRmdTsRecord.get(NULLABLE_STRING_MAP_FIELD_NAME);
    Assert.assertEquals(updatedNullableMapTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 2L);
    Assert.assertEquals(updatedNullableMapTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 0);
    Assert.assertEquals(updatedNullableMapTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.singletonList(3L));
    Assert.assertEquals(updatedNullableMapTsRecord.get(DELETED_ELEM_FIELD_NAME), Arrays.asList("key5", "key6"));
    Assert.assertEquals(updatedNullableMapTsRecord.get(DELETED_ELEM_TS_FIELD_NAME), Arrays.asList(2L, 3L));

    // Set nullable collection field to NULL value by updating it in a large enough TS.
    partialUpdateRecord =
        new UpdateBuilderImpl(schemaSet.getUpdateSchema()).setNewFieldValue(NULLABLE_STRING_MAP_FIELD_NAME, null)
            .setNewFieldValue(NULLABLE_STRING_ARRAY_FIELD_NAME, null)
            .build();
    result = mergeConflictResolver.update(
        Lazy.of(() -> serializeValueRecord(updatedValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, updateRmdRecord),
        serializeUpdateRecord(partialUpdateRecord),
        schemaSet.getValueSchemaId(),
        schemaSet.getUpdateSchemaProtocolVersion(),
        10L,
        1L,
        0,
        0);

    GenericRecord newUpdatedValueRecord = deserializeValueRecord(result.getNewValue());
    Assert.assertNull(newUpdatedValueRecord.get(NULLABLE_STRING_ARRAY_FIELD_NAME));
    Assert.assertNull(newUpdatedValueRecord.get(NULLABLE_STRING_MAP_FIELD_NAME));
    GenericRecord newUpdatedRmdRecord = result.getRmdRecord();
    GenericRecord newUpdatedRmdTsRecord = (GenericRecord) newUpdatedRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);

    updatedNullableListTsRecord = (GenericRecord) newUpdatedRmdTsRecord.get(NULLABLE_STRING_ARRAY_FIELD_NAME);
    Assert.assertEquals(updatedNullableListTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 10L);
    Assert.assertEquals(updatedNullableListTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 0);
    Assert.assertEquals(updatedNullableListTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals(updatedNullableListTsRecord.get(DELETED_ELEM_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals(updatedNullableListTsRecord.get(DELETED_ELEM_TS_FIELD_NAME), Collections.emptyList());

    updatedNullableMapTsRecord = (GenericRecord) newUpdatedRmdTsRecord.get(NULLABLE_STRING_MAP_FIELD_NAME);
    Assert.assertEquals(updatedNullableMapTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 10L);
    Assert.assertEquals(updatedNullableMapTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 0);
    Assert.assertEquals(updatedNullableMapTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals(updatedNullableMapTsRecord.get(DELETED_ELEM_FIELD_NAME), Collections.emptyList());
    Assert.assertEquals(updatedNullableMapTsRecord.get(DELETED_ELEM_TS_FIELD_NAME), Collections.emptyList());
  }

  @Test
  public void testAddToCollectionFieldOnFieldValueTs() {
    IndexedHashMap<String, String> addToMapValue = new IndexedHashMap<>();
    addToMapValue.put("key1", "2");
    addToMapValue.put("key2", "2");
    addToMapValue.put("key3", "2");
    addToMapValue.put("key4", "2");
    addToMapValue.put("key5", "2");
    GenericRecord partialUpdateRecord = new UpdateBuilderImpl(schemaSet.getUpdateSchema())
        .setEntriesToAddToMapField(STRING_MAP_FIELD_NAME, addToMapValue)
        .setElementsToAddToListField(
            STRING_ARRAY_FIELD_NAME,
            Arrays.asList("key1", "key2", "key3", "key4", "key5", "key6"))
        .setElementsToAddToListField(NULLABLE_STRING_ARRAY_FIELD_NAME, Collections.singletonList("key1"))
        .setEntriesToAddToMapField(NULLABLE_STRING_MAP_FIELD_NAME, Collections.singletonMap("key1", "1"))
        .build();

    GenericRecord oldValueRecord = createDefaultValueRecord();
    oldValueRecord.put(STRING_ARRAY_FIELD_NAME, Arrays.asList("key1", "key2", "key3"));
    IndexedHashMap<String, String> mapValue = new IndexedHashMap<>();
    mapValue.put("key1", "1");
    mapValue.put("key2", "1"); // Value is smaller than incoming update.
    mapValue.put("key3", "4"); // Value is bigger than incoming update.
    mapValue.put("key4", "1");
    oldValueRecord.put(STRING_MAP_FIELD_NAME, mapValue);

    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 2);
    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);

    GenericRecord nullableListTsRecord = (GenericRecord) timestampRecord.get(NULLABLE_STRING_ARRAY_FIELD_NAME);
    nullableListTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);

    GenericRecord nullableMapTsRecord = (GenericRecord) timestampRecord.get(NULLABLE_STRING_MAP_FIELD_NAME);
    nullableMapTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);

    GenericRecord listTsRecord = (GenericRecord) timestampRecord.get(STRING_ARRAY_FIELD_NAME);
    listTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    listTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    listTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 3L));
    listTsRecord.put(DELETED_ELEM_FIELD_NAME, Arrays.asList("key4", "key5", "key6"));
    listTsRecord.put(DELETED_ELEM_TS_FIELD_NAME, Arrays.asList(1L, 2L, 3L));

    GenericRecord mapTsRecord = (GenericRecord) timestampRecord.get(STRING_MAP_FIELD_NAME);
    mapTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    mapTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    mapTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 2L, 3L));

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
        updatedValueRecord.get(NULLABLE_STRING_ARRAY_FIELD_NAME),
        Collections.singletonList(new Utf8("key1")));
    IndexedHashMap<Utf8, Utf8> expectedNullableMap = new IndexedHashMap<>();
    expectedNullableMap.put(new Utf8("key1"), new Utf8("1"));
    Assert.assertEquals(updatedValueRecord.get(NULLABLE_STRING_MAP_FIELD_NAME), expectedNullableMap);

    Assert.assertEquals(
        updatedValueRecord.get(STRING_ARRAY_FIELD_NAME),
        Arrays.asList(new Utf8("key1"), new Utf8("key2"), new Utf8("key3"), new Utf8("key4")));

    IndexedHashMap<Utf8, Utf8> expectedMap = new IndexedHashMap<>();
    expectedMap.put(new Utf8("key1"), new Utf8("2"));
    expectedMap.put(new Utf8("key2"), new Utf8("2"));
    expectedMap.put(new Utf8("key3"), new Utf8("4"));
    expectedMap.put(new Utf8("key4"), new Utf8("1"));
    expectedMap.put(new Utf8("key5"), new Utf8("2"));
    Assert.assertEquals(updatedValueRecord.get(STRING_MAP_FIELD_NAME), expectedMap);

    GenericRecord updatedRmdTsRecord = (GenericRecord) result.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord updatedListTsRecord = (GenericRecord) updatedRmdTsRecord.get(STRING_ARRAY_FIELD_NAME);
    Assert.assertEquals(updatedListTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 1L);
    Assert.assertEquals(updatedListTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Arrays.asList(2L, 2L, 2L, 2L));
    Assert.assertEquals(updatedListTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 0);
    Assert.assertEquals(updatedListTsRecord.get(DELETED_ELEM_FIELD_NAME), Arrays.asList("key5", "key6"));
    Assert.assertEquals(updatedListTsRecord.get(DELETED_ELEM_TS_FIELD_NAME), Arrays.asList(2L, 3L));

    GenericRecord updatedMapTsRecord = (GenericRecord) updatedRmdTsRecord.get(STRING_MAP_FIELD_NAME);
    Assert.assertEquals(updatedMapTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 1L);
    Assert.assertEquals(updatedMapTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Arrays.asList(2L, 2L, 2L, 2L, 3L));
    Assert.assertEquals(updatedMapTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 0);
  }

  @Test
  public void testRemoveFromCollectionFieldOnFieldLevelTs() {
    GenericRecord partialUpdateRecord = new UpdateBuilderImpl(schemaSet.getUpdateSchema())
        .setKeysToRemoveFromMapField(STRING_MAP_FIELD_NAME, Arrays.asList("key1", "key2", "key3", "key4"))
        .setElementsToRemoveFromListField(
            STRING_ARRAY_FIELD_NAME,
            Arrays.asList("key1", "key2", "key3", "key4", "key5", "key6"))
        .setKeysToRemoveFromMapField(NULLABLE_STRING_MAP_FIELD_NAME, Collections.singletonList("key1"))
        .setElementsToRemoveFromListField(NULLABLE_STRING_ARRAY_FIELD_NAME, Collections.singletonList("key1"))
        .build();

    GenericRecord oldValueRecord = createDefaultValueRecord();
    IndexedHashMap<String, String> mapValue = new IndexedHashMap<>();
    mapValue.put("key1", "1");
    mapValue.put("key2", "1");
    mapValue.put("key3", "1");
    oldValueRecord.put(STRING_MAP_FIELD_NAME, mapValue);
    oldValueRecord.put(STRING_ARRAY_FIELD_NAME, Arrays.asList("key1", "key2", "key3"));

    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 2);
    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord mapTsRecord = (GenericRecord) timestampRecord.get(STRING_MAP_FIELD_NAME);
    mapTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    mapTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    mapTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(3L, 4L));

    GenericRecord listTsRecord = (GenericRecord) timestampRecord.get(STRING_ARRAY_FIELD_NAME);
    listTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    listTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    listTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(3L, 4L));
    listTsRecord.put(DELETED_ELEM_FIELD_NAME, Arrays.asList("key4", "key5", "key6"));
    listTsRecord.put(DELETED_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 3L, 4L));

    MergeConflictResult result = mergeConflictResolver.update(
        Lazy.of(() -> serializeValueRecord(oldValueRecord)),
        new RmdWithValueSchemaId(schemaSet.getValueSchemaId(), RMD_VERSION_ID, oldRmdRecord),
        serializeUpdateRecord(partialUpdateRecord),
        schemaSet.getValueSchemaId(),
        schemaSet.getUpdateSchemaProtocolVersion(),
        3L,
        1L,
        0,
        0);
    GenericRecord updatedValueRecord = deserializeValueRecord(result.getNewValue());
    Assert.assertEquals(updatedValueRecord.get(NULLABLE_STRING_ARRAY_FIELD_NAME), Collections.emptyList());
    IndexedHashMap<Utf8, Utf8> expectedNullableMap = new IndexedHashMap<>();
    Assert.assertEquals(updatedValueRecord.get(NULLABLE_STRING_MAP_FIELD_NAME), expectedNullableMap);

    IndexedHashMap<Utf8, Utf8> expectedMap = new IndexedHashMap<>();
    expectedMap.put(new Utf8("key3"), new Utf8("1"));
    Assert.assertEquals(updatedValueRecord.get(STRING_MAP_FIELD_NAME), expectedMap);
    Assert.assertEquals(updatedValueRecord.get(STRING_ARRAY_FIELD_NAME), Collections.singletonList(new Utf8("key3")));

    GenericRecord updatedRmdTsRecord = (GenericRecord) result.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord updatedMapTsRecord = (GenericRecord) updatedRmdTsRecord.get(STRING_MAP_FIELD_NAME);
    Assert.assertEquals(updatedMapTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 1L);
    Assert.assertEquals(updatedMapTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.singletonList(4L));
    Assert.assertEquals(updatedMapTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 0);
    Assert.assertEquals(updatedMapTsRecord.get(DELETED_ELEM_FIELD_NAME), Arrays.asList("key1", "key2", "key4"));
    Assert.assertEquals(updatedMapTsRecord.get(DELETED_ELEM_TS_FIELD_NAME), Arrays.asList(3L, 3L, 3L));

    GenericRecord updatedNullableMapTsRecord = (GenericRecord) updatedRmdTsRecord.get(NULLABLE_STRING_MAP_FIELD_NAME);
    Assert.assertEquals(updatedNullableMapTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 2L);
    Assert.assertEquals(updatedNullableMapTsRecord.get(DELETED_ELEM_FIELD_NAME), Collections.singletonList("key1"));
    Assert.assertEquals(updatedNullableMapTsRecord.get(DELETED_ELEM_TS_FIELD_NAME), Collections.singletonList(3L));

    GenericRecord updatedListTsRecord = (GenericRecord) updatedRmdTsRecord.get(STRING_ARRAY_FIELD_NAME);
    Assert.assertEquals(updatedListTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 1L);
    Assert.assertEquals(updatedListTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.singletonList(4L));
    Assert.assertEquals(updatedListTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 0);
    Assert.assertEquals(
        updatedListTsRecord.get(DELETED_ELEM_FIELD_NAME),
        Arrays.asList("key1", "key2", "key4", "key5", "key6"));
    Assert.assertEquals(updatedListTsRecord.get(DELETED_ELEM_TS_FIELD_NAME), Arrays.asList(3L, 3L, 3L, 3L, 4L));

    GenericRecord updatedNullableListTsRecord =
        (GenericRecord) updatedRmdTsRecord.get(NULLABLE_STRING_ARRAY_FIELD_NAME);
    Assert.assertEquals(updatedNullableListTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 2L);
    Assert.assertEquals(updatedNullableListTsRecord.get(DELETED_ELEM_FIELD_NAME), Collections.singletonList("key1"));
    Assert.assertEquals(updatedNullableListTsRecord.get(DELETED_ELEM_TS_FIELD_NAME), Collections.singletonList(3L));
  }

  /**
   * This unit test chains up different partial update operations on nullable collection field to make sure all operations
   * are executed correctly and successfully.
   */
  @Test
  public void testNullableCollectionFieldUpdateOperations() {
    MergeConflictResult result;
    IndexedHashMap<Utf8, Utf8> expectedMapValue = new IndexedHashMap<>();
    List<Utf8> expectedListValue = new ArrayList<>();
    long operationTs = 1L;

    // Operation 1: Set field to empty collection.
    result = updateNullableCollection(
        null,
        operationTs,
        UpdateOperationType.SET_FIELD,
        Collections.emptyMap(),
        Collections.emptyList());
    validateNullableCollectionUpdateResult(result, expectedMapValue, expectedListValue);

    // Operation 2: Set field to null collection.
    operationTs++;
    result = updateNullableCollection(result, operationTs, UpdateOperationType.SET_FIELD, null, null);
    validateNullableCollectionUpdateResult(result, null, null);

    // Operation 3: Add entries to collection starting from null.
    for (int i = 0; i < 3; i++) {
      String itemKey = "key_" + i;
      expectedMapValue.put(new Utf8(itemKey), new Utf8("1"));
      expectedListValue.add(new Utf8(itemKey));
      operationTs++;
      result = updateNullableCollection(
          result,
          operationTs,
          UpdateOperationType.ADD_ENTRY,
          Collections.singletonMap(itemKey, "1"),
          Collections.singletonList(itemKey));
      validateNullableCollectionUpdateResult(result, expectedMapValue, expectedListValue);
    }

    // Operation 4: Remove entries to collection
    for (int i = 0; i < 3; i++) {
      String itemKey = "key_" + i;
      expectedMapValue.remove(new Utf8(itemKey));
      expectedListValue.remove(0);
      operationTs++;
      result = updateNullableCollection(
          result,
          operationTs,
          UpdateOperationType.REMOVE_ENTRY,
          Collections.singletonList(itemKey),
          Collections.singletonList(itemKey));
      validateNullableCollectionUpdateResult(result, expectedMapValue, expectedListValue);
    }

    // Operation 5: Set collection to null
    operationTs++;
    result = updateNullableCollection(result, operationTs, UpdateOperationType.SET_FIELD, null, null);
    validateNullableCollectionUpdateResult(result, null, null);

    // Operation 6: Remove entries from null results in empty collection.
    operationTs++;
    result = updateNullableCollection(
        result,
        operationTs,
        UpdateOperationType.REMOVE_ENTRY,
        Collections.singletonList("dummyKey"),
        Collections.singletonList("dummyKey"));
    validateNullableCollectionUpdateResult(result, Collections.emptyMap(), Collections.emptyList());

    // Operation 7: Add entries to collection starting from empty collection.
    for (int i = 0; i < 3; i++) {
      String itemKey = "key_" + i;
      expectedMapValue.put(new Utf8(itemKey), new Utf8("1"));
      expectedListValue.add(new Utf8(itemKey));
      operationTs++;
      result = updateNullableCollection(
          result,
          operationTs,
          UpdateOperationType.ADD_ENTRY,
          Collections.singletonMap(itemKey, "1"),
          Collections.singletonList(itemKey));
      validateNullableCollectionUpdateResult(result, expectedMapValue, expectedListValue);
    }

    // Operation 8: Add entries to collection starting from empty collection.
    expectedMapValue = new IndexedHashMap<>();
    expectedListValue = new ArrayList<>();
    expectedMapValue.put(new Utf8("putOnly"), new Utf8("1"));
    expectedListValue.add(new Utf8("putOnly"));
    operationTs++;
    result = updateNullableCollection(
        result,
        operationTs,
        UpdateOperationType.SET_FIELD,
        Collections.singletonMap("putOnly", "1"),
        Collections.singletonList("putOnly"));
    validateNullableCollectionUpdateResult(result, expectedMapValue, expectedListValue);

    // Operation 9: Add entries to collection starting from null.
    for (int i = 0; i < 3; i++) {
      String itemKey = "key_" + i;
      expectedMapValue.put(new Utf8(itemKey), new Utf8("1"));
      expectedListValue.add(new Utf8(itemKey));
      operationTs++;
      result = updateNullableCollection(
          result,
          operationTs,
          UpdateOperationType.ADD_ENTRY,
          Collections.singletonMap(itemKey, "1"),
          Collections.singletonList(itemKey));
      validateNullableCollectionUpdateResult(result, expectedMapValue, expectedListValue);
    }

    // Operation 10: Remove entries to collection
    for (int i = 0; i < 3; i++) {
      String itemKey = "key_" + i;
      expectedMapValue.remove(new Utf8(itemKey));
      expectedListValue.remove(1);
      operationTs++;
      result = updateNullableCollection(
          result,
          operationTs,
          UpdateOperationType.REMOVE_ENTRY,
          Collections.singletonList(itemKey),
          Collections.singletonList(itemKey));
      validateNullableCollectionUpdateResult(result, expectedMapValue, expectedListValue);
    }
  }

  private MergeConflictResult updateNullableCollection(
      MergeConflictResult result,
      long timestamp,
      UpdateOperationType operationType,
      Object mapFieldValue,
      Object listFieldValue) {
    GenericRecord oldRmdRecord = result == null ? null : result.getRmdRecord();
    ByteBuffer oldValueByteBuffer = result == null ? null : result.getNewValue();
    GenericRecord partialUpdateRecord;
    if (operationType.equals(UpdateOperationType.ADD_ENTRY)) {
      partialUpdateRecord = new UpdateBuilderImpl(schemaSet.getUpdateSchema())
          .setElementsToAddToListField(NULLABLE_STRING_ARRAY_FIELD_NAME, (List<?>) listFieldValue)
          .setEntriesToAddToMapField(NULLABLE_STRING_MAP_FIELD_NAME, (Map<String, ?>) mapFieldValue)
          .build();
    } else if (operationType.equals(UpdateOperationType.REMOVE_ENTRY)) {
      partialUpdateRecord = new UpdateBuilderImpl(schemaSet.getUpdateSchema())
          .setElementsToRemoveFromListField(NULLABLE_STRING_ARRAY_FIELD_NAME, (List<?>) listFieldValue)
          .setKeysToRemoveFromMapField(NULLABLE_STRING_MAP_FIELD_NAME, (List<String>) mapFieldValue)
          .build();
    } else {
      partialUpdateRecord = new UpdateBuilderImpl(schemaSet.getUpdateSchema())
          .setNewFieldValue(NULLABLE_STRING_ARRAY_FIELD_NAME, listFieldValue)
          .setNewFieldValue(NULLABLE_STRING_MAP_FIELD_NAME, mapFieldValue)
          .build();
    }

    return mergeConflictResolver.update(
        Lazy.of(() -> oldValueByteBuffer),
        oldRmdRecord == null
            ? null
            : new RmdWithValueSchemaId(
                schemaSet.getValueSchemaId(),
                schemaSet.getRmdSchemaProtocolVersion(),
                oldRmdRecord),
        serializeUpdateRecord(partialUpdateRecord),
        schemaSet.getValueSchemaId(),
        schemaSet.getUpdateSchemaProtocolVersion(),
        timestamp,
        1L,
        0,
        0);
  }

  private void validateNullableCollectionUpdateResult(
      MergeConflictResult result,
      Map expectedMapValue,
      List expectedStringValue) {
    GenericRecord valueRecord = deserializeValueRecord(result.getNewValue());
    Assert.assertEquals(valueRecord.get(NULLABLE_STRING_ARRAY_FIELD_NAME), expectedStringValue);
    Assert.assertEquals(valueRecord.get(NULLABLE_STRING_MAP_FIELD_NAME), expectedMapValue);
  }

  enum UpdateOperationType {
    SET_FIELD, ADD_ENTRY, REMOVE_ENTRY
  }
}
