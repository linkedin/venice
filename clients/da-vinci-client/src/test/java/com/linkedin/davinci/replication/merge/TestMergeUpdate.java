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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
    IndexedHashMap<String, Integer> updateMapValue = new IndexedHashMap<>();
    updateMapValue.put("key1", 2);
    updateMapValue.put("key2", 2);
    List<String> updateListValue = Arrays.asList("item1", "item2");
    GenericRecord partialUpdateRecord =
        new UpdateBuilderImpl(schemaSet.getUpdateSchema()).setNewFieldValue(INT_MAP_FIELD_NAME, updateMapValue)
            .setNewFieldValue(STRING_ARRAY_FIELD_NAME, updateListValue)
            .build();

    GenericRecord oldValueRecord = createDefaultValueRecord();
    IndexedHashMap<String, Integer> initialMapValue = new IndexedHashMap<>();
    initialMapValue.put("key1", 1);
    initialMapValue.put("key2", 1);
    oldValueRecord.put(INT_MAP_FIELD_NAME, initialMapValue);
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
    IndexedHashMap<String, Integer> addToMapValue = new IndexedHashMap<>();
    List<String> addToListValue;
    if (updateOnActiveElement) {
      addToMapValue.put("key2", 0);
      addToListValue = Collections.singletonList("item2");
    } else {
      addToMapValue.put("key1", 0);
      addToListValue = Collections.singletonList("item1");
    }

    GenericRecord partialUpdateRecord = new UpdateBuilderImpl(schemaSet.getUpdateSchema())
        .setElementsToAddToListField(STRING_ARRAY_FIELD_NAME, addToListValue)
        .setEntriesToAddToMapField(INT_MAP_FIELD_NAME, addToMapValue)
        .build();

    GenericRecord oldValueRecord = createDefaultValueRecord();
    IndexedHashMap<String, Integer> initialMapValue = new IndexedHashMap<>();
    initialMapValue.put("key1", 1);
    initialMapValue.put("key2", 1);
    oldValueRecord.put(INT_MAP_FIELD_NAME, initialMapValue);
    oldValueRecord.put(STRING_ARRAY_FIELD_NAME, Arrays.asList("item1", "item2"));
    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, baselineRmdTs);

    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord mapTsRecord = (GenericRecord) timestampRecord.get(INT_MAP_FIELD_NAME);
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
        .setKeysToRemoveFromMapField(INT_MAP_FIELD_NAME, removeFromMapValue)
        .build();

    GenericRecord oldValueRecord = createDefaultValueRecord();
    IndexedHashMap<String, Integer> initialMapValue = new IndexedHashMap<>();
    initialMapValue.put("key1", 1);
    initialMapValue.put("key2", 1);
    oldValueRecord.put(INT_MAP_FIELD_NAME, initialMapValue);
    oldValueRecord.put(STRING_ARRAY_FIELD_NAME, Arrays.asList("item1", "item2"));
    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, baselineRmdTs);

    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord mapTsRecord = (GenericRecord) timestampRecord.get(INT_MAP_FIELD_NAME);
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

    IndexedHashMap<String, Integer> addToMapValue = new IndexedHashMap<>();
    addToMapValue.put("key1", 2);
    addToMapValue.put("key2", 2);
    List<String> addToListValue = Collections.singletonList("item1");
    GenericRecord partialUpdateRecord = new UpdateBuilderImpl(schemaSet.getUpdateSchema())
        .setElementsToAddToListField(STRING_ARRAY_FIELD_NAME, addToListValue)
        .setEntriesToAddToMapField(INT_MAP_FIELD_NAME, addToMapValue)
        .build();

    GenericRecord oldValueRecord = createDefaultValueRecord();
    IndexedHashMap<String, Integer> initialMapValue = new IndexedHashMap<>();
    initialMapValue.put("key1", 1);
    initialMapValue.put("key2", 1);
    oldValueRecord.put(INT_MAP_FIELD_NAME, initialMapValue);
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
    IndexedHashMap<String, Integer> updateMapValue = new IndexedHashMap<>();
    updateMapValue.put("key1", 2);
    updateMapValue.put("key2", 2);
    updateMapValue.put("key3", 2);
    updateMapValue.put("key4", 2);
    updateMapValue.put("key5", 2);
    updateMapValue.put("key6", 2);
    GenericRecord partialUpdateRecord =
        new UpdateBuilderImpl(schemaSet.getUpdateSchema()).setNewFieldValue(INT_MAP_FIELD_NAME, updateMapValue)
            .setNewFieldValue(STRING_ARRAY_FIELD_NAME, Arrays.asList("key1", "key2", "key3", "key4", "key5", "key6"))
            .build();

    GenericRecord oldValueRecord = createDefaultValueRecord();
    oldValueRecord.put(STRING_ARRAY_FIELD_NAME, Arrays.asList("key1", "key2", "key3"));
    IndexedHashMap<String, Integer> mapValue = new IndexedHashMap<>();
    mapValue.put("key1", 1); // Put Only Part
    mapValue.put("key2", 1); // Same TS, value is smaller than incoming update.
    mapValue.put("key3", 4); // Same TS, value is bigger than incoming update.
    mapValue.put("key4", 1); // Higher TS
    oldValueRecord.put(INT_MAP_FIELD_NAME, mapValue);

    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 2);
    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord listTsRecord = (GenericRecord) timestampRecord.get(STRING_ARRAY_FIELD_NAME);
    listTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    listTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    listTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 3L));
    listTsRecord.put(DELETED_ELEM_FIELD_NAME, Arrays.asList("key4", "key5"));
    listTsRecord.put(DELETED_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 3L));

    GenericRecord mapTsRecord = (GenericRecord) timestampRecord.get(INT_MAP_FIELD_NAME);
    mapTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    mapTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    mapTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 2L, 3L));
    mapTsRecord.put(DELETED_ELEM_FIELD_NAME, Arrays.asList("key5", "key6"));
    mapTsRecord.put(DELETED_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 3L));

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
        Arrays.asList(new Utf8("key1"), new Utf8("key2"), new Utf8("key6"), new Utf8("key3")));

    IndexedHashMap<Utf8, Integer> expectedMap = new IndexedHashMap<>();
    expectedMap.put(new Utf8("key1"), 2);
    expectedMap.put(new Utf8("key2"), 2);
    expectedMap.put(new Utf8("key3"), 2);
    expectedMap.put(new Utf8("key4"), 1);

    Assert.assertEquals(updatedValueRecord.get(INT_MAP_FIELD_NAME), expectedMap);

    GenericRecord updatedRmdTsRecord = (GenericRecord) result.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord updatedListTsRecord = (GenericRecord) updatedRmdTsRecord.get(STRING_ARRAY_FIELD_NAME);
    Assert.assertEquals(updatedListTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 2L);
    Assert.assertEquals(updatedListTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 3);
    Assert.assertEquals(updatedListTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.singletonList(3L));
    Assert.assertEquals(updatedListTsRecord.get(DELETED_ELEM_FIELD_NAME), Arrays.asList("key4", "key5"));
    Assert.assertEquals(updatedListTsRecord.get(DELETED_ELEM_TS_FIELD_NAME), Arrays.asList(2L, 3L));

    GenericRecord updatedMapTsRecord = (GenericRecord) updatedRmdTsRecord.get(INT_MAP_FIELD_NAME);
    Assert.assertEquals(updatedMapTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 2L);
    Assert.assertEquals(updatedMapTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 3);
    Assert.assertEquals(updatedMapTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.singletonList(3L));
    Assert.assertEquals(updatedMapTsRecord.get(DELETED_ELEM_FIELD_NAME), Arrays.asList("key5", "key6"));
    Assert.assertEquals(updatedMapTsRecord.get(DELETED_ELEM_TS_FIELD_NAME), Arrays.asList(2L, 3L));
  }

  @Test
  public void testAddToCollectionFieldOnFieldValueTs() {
    IndexedHashMap<String, Integer> addToMapValue = new IndexedHashMap<>();
    addToMapValue.put("key1", 2);
    addToMapValue.put("key2", 2);
    addToMapValue.put("key3", 2);
    addToMapValue.put("key4", 2);
    addToMapValue.put("key5", 2);
    GenericRecord partialUpdateRecord =
        new UpdateBuilderImpl(schemaSet.getUpdateSchema()).setEntriesToAddToMapField(INT_MAP_FIELD_NAME, addToMapValue)
            .setElementsToAddToListField(
                STRING_ARRAY_FIELD_NAME,
                Arrays.asList("key1", "key2", "key3", "key4", "key5", "key6"))
            .build();

    GenericRecord oldValueRecord = createDefaultValueRecord();
    oldValueRecord.put(STRING_ARRAY_FIELD_NAME, Arrays.asList("key1", "key2", "key3"));
    IndexedHashMap<String, Integer> mapValue = new IndexedHashMap<>();
    mapValue.put("key1", 1);
    mapValue.put("key2", 1); // Value is smaller than incoming update.
    mapValue.put("key3", 4); // Value is bigger than incoming update.
    mapValue.put("key4", 1);
    oldValueRecord.put(INT_MAP_FIELD_NAME, mapValue);

    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 2);
    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord listTsRecord = (GenericRecord) timestampRecord.get(STRING_ARRAY_FIELD_NAME);
    listTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    listTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    listTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 3L));
    listTsRecord.put(DELETED_ELEM_FIELD_NAME, Arrays.asList("key4", "key5", "key6"));
    listTsRecord.put(DELETED_ELEM_TS_FIELD_NAME, Arrays.asList(1L, 2L, 3L));

    GenericRecord mapTsRecord = (GenericRecord) timestampRecord.get(INT_MAP_FIELD_NAME);
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
        updatedValueRecord.get(STRING_ARRAY_FIELD_NAME),
        Arrays.asList(new Utf8("key1"), new Utf8("key2"), new Utf8("key3"), new Utf8("key4")));

    IndexedHashMap<Utf8, Integer> expectedMap = new IndexedHashMap<>();
    expectedMap.put(new Utf8("key1"), 2);
    expectedMap.put(new Utf8("key2"), 2);
    expectedMap.put(new Utf8("key3"), 4);
    expectedMap.put(new Utf8("key4"), 1);
    expectedMap.put(new Utf8("key5"), 2);
    Assert.assertEquals(updatedValueRecord.get(INT_MAP_FIELD_NAME), expectedMap);

    GenericRecord updatedRmdTsRecord = (GenericRecord) result.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord updatedListTsRecord = (GenericRecord) updatedRmdTsRecord.get(STRING_ARRAY_FIELD_NAME);
    Assert.assertEquals(updatedListTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 1L);
    Assert.assertEquals(updatedListTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Arrays.asList(2L, 2L, 2L, 2L));
    Assert.assertEquals(updatedListTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 0);
    Assert.assertEquals(updatedListTsRecord.get(DELETED_ELEM_FIELD_NAME), Arrays.asList("key5", "key6"));
    Assert.assertEquals(updatedListTsRecord.get(DELETED_ELEM_TS_FIELD_NAME), Arrays.asList(2L, 3L));

    GenericRecord updatedMapTsRecord = (GenericRecord) updatedRmdTsRecord.get(INT_MAP_FIELD_NAME);
    Assert.assertEquals(updatedMapTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 1L);
    Assert.assertEquals(updatedMapTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Arrays.asList(2L, 2L, 2L, 2L, 3L));
    Assert.assertEquals(updatedMapTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 0);
  }

  @Test
  public void testRemoveFromCollectionFieldOnFieldLevelTs() {
    GenericRecord partialUpdateRecord = new UpdateBuilderImpl(schemaSet.getUpdateSchema())
        .setKeysToRemoveFromMapField(INT_MAP_FIELD_NAME, Arrays.asList("key1", "key2", "key3", "key4"))
        .setElementsToRemoveFromListField(
            STRING_ARRAY_FIELD_NAME,
            Arrays.asList("key1", "key2", "key3", "key4", "key5", "key6"))
        .build();

    GenericRecord oldValueRecord = createDefaultValueRecord();
    IndexedHashMap<String, Integer> mapValue = new IndexedHashMap<>();
    mapValue.put("key1", 1);
    mapValue.put("key2", 1);
    mapValue.put("key3", 1);
    oldValueRecord.put(INT_MAP_FIELD_NAME, mapValue);
    oldValueRecord.put(STRING_ARRAY_FIELD_NAME, Arrays.asList("key1", "key2", "key3"));

    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord(oldValueRecord, 2);
    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord mapTsRecord = (GenericRecord) timestampRecord.get(INT_MAP_FIELD_NAME);
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

    IndexedHashMap<Utf8, Integer> expectedMap = new IndexedHashMap<>();
    expectedMap.put(new Utf8("key3"), 1);
    Assert.assertEquals(updatedValueRecord.get(INT_MAP_FIELD_NAME), expectedMap);
    Assert.assertEquals(updatedValueRecord.get(STRING_ARRAY_FIELD_NAME), Collections.singletonList(new Utf8("key3")));

    GenericRecord updatedRmdTsRecord = (GenericRecord) result.getRmdRecord().get(RmdConstants.TIMESTAMP_FIELD_NAME);
    GenericRecord updatedMapTsRecord = (GenericRecord) updatedRmdTsRecord.get(INT_MAP_FIELD_NAME);
    Assert.assertEquals(updatedMapTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 1L);
    Assert.assertEquals(updatedMapTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.singletonList(4L));
    Assert.assertEquals(updatedMapTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 0);
    Assert.assertEquals(updatedMapTsRecord.get(DELETED_ELEM_FIELD_NAME), Arrays.asList("key1", "key2", "key4"));
    Assert.assertEquals(updatedMapTsRecord.get(DELETED_ELEM_TS_FIELD_NAME), Arrays.asList(3L, 3L, 3L));

    GenericRecord updatedListTsRecord = (GenericRecord) updatedRmdTsRecord.get(STRING_ARRAY_FIELD_NAME);
    Assert.assertEquals(updatedListTsRecord.get(TOP_LEVEL_TS_FIELD_NAME), 1L);
    Assert.assertEquals(updatedListTsRecord.get(ACTIVE_ELEM_TS_FIELD_NAME), Collections.singletonList(4L));
    Assert.assertEquals(updatedListTsRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME), 0);
    Assert.assertEquals(
        updatedListTsRecord.get(DELETED_ELEM_FIELD_NAME),
        Arrays.asList("key1", "key2", "key4", "key5", "key6"));
    Assert.assertEquals(updatedListTsRecord.get(DELETED_ELEM_TS_FIELD_NAME), Arrays.asList(3L, 3L, 3L, 3L, 4L));
  }
}
