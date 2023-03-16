package com.linkedin.venice.schema.merge;

import static com.linkedin.venice.schema.Utils.loadSchemaFileAsString;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.ACTIVE_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.PUT_ONLY_PART_LENGTH_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_COLO_ID_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;

import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.SchemaUtils;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp;
import com.linkedin.venice.utils.IndexedHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CollectionMergeTest {
  private static final Schema VALUE_SCHEMA =
      AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(loadSchemaFileAsString("testMergeSchema.avsc"));
  protected static final String LIST_FIELD_NAME = "StringListField";
  protected static final String NULLABLE_LIST_FIELD_NAME = "NullableStringListField";
  protected static final String MAP_FIELD_NAME = "IntMapField";
  protected static final String NULLABLE_MAP_FIELD_NAME = "NullableIntMapField";

  private static final Schema RMD_SCHEMA =
      SchemaUtils.annotateRmdSchema(RmdSchemaGenerator.generateMetadataSchema(VALUE_SCHEMA));
  private static final Schema RMD_TIMESTAMP_SCHEMA =
      RMD_SCHEMA.getField(RmdConstants.TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);

  @Test
  public void testHandleListOpWithNullValue() {
    GenericRecord oldRecord = new GenericData.Record(VALUE_SCHEMA);
    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord();
    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    List<String> listValue = Collections.singletonList("item1");
    GenericRecord nullableListTsRecord = (GenericRecord) timestampRecord.get(NULLABLE_LIST_FIELD_NAME);
    nullableListTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 0L);
    nullableListTsRecord.put(TOP_LEVEL_COLO_ID_FIELD_NAME, 0);
    nullableListTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 0);
    GenericRecord listTsRecord = (GenericRecord) timestampRecord.get(LIST_FIELD_NAME);
    listTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    listTsRecord.put(TOP_LEVEL_COLO_ID_FIELD_NAME, 1);
    listTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    oldRecord.put(LIST_FIELD_NAME, listValue);

    UpdateResultStatus resultStatus;
    CollectionTimestampMergeRecordHelper collectionTimestampMergeRecordHelper =
        new CollectionTimestampMergeRecordHelper();
    SortBasedCollectionFieldOpHandler handlerToTest =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE);

    List<String> newListValue = Collections.singletonList("item2");
    // Case 1: Test LIST can handle setField to new list.
    resultStatus = collectionTimestampMergeRecordHelper.putOnField(
        oldRecord,
        (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME),
        NULLABLE_LIST_FIELD_NAME,
        newListValue,
        2,
        1);
    Assert.assertEquals(UpdateResultStatus.COMPLETELY_UPDATED, resultStatus);
    Assert.assertEquals(((List<String>) oldRecord.get(NULLABLE_LIST_FIELD_NAME)).get(0), "item2");
    resultStatus = collectionTimestampMergeRecordHelper.putOnField(
        oldRecord,
        (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME),
        LIST_FIELD_NAME,
        newListValue,
        2,
        1);
    Assert.assertEquals(UpdateResultStatus.COMPLETELY_UPDATED, resultStatus);
    Assert.assertEquals(((List<String>) oldRecord.get(LIST_FIELD_NAME)).get(0), "item2");

    // Case 2: Test nullable LIST can handle setField to null.
    resultStatus = collectionTimestampMergeRecordHelper.putOnField(
        oldRecord,
        (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME),
        NULLABLE_LIST_FIELD_NAME,
        null,
        3,
        1);
    Assert.assertNull(oldRecord.get(NULLABLE_LIST_FIELD_NAME));
    Assert.assertEquals(UpdateResultStatus.COMPLETELY_UPDATED, resultStatus);

    // Case 3: Test nullable LIST can handle collection merge from null value.
    List<Object> newEntries = Collections.singletonList("item3");
    List<Object> toRemoveKeys = new LinkedList<>();
    CollectionRmdTimestamp<Object> collectionRmdTimestamp = new CollectionRmdTimestamp<>(nullableListTsRecord);
    resultStatus = handlerToTest
        .handleModifyList(4L, collectionRmdTimestamp, oldRecord, NULLABLE_LIST_FIELD_NAME, newEntries, toRemoveKeys);
    Assert.assertEquals(UpdateResultStatus.PARTIALLY_UPDATED, resultStatus);
    Assert.assertEquals(((List<String>) oldRecord.get(NULLABLE_LIST_FIELD_NAME)).get(0), "item3");

    // Case 4: Test nullable List can handle setField to null when there is active elements.
    resultStatus = collectionTimestampMergeRecordHelper.putOnField(
        oldRecord,
        (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME),
        NULLABLE_LIST_FIELD_NAME,
        newListValue,
        3,
        2);
    Assert.assertEquals(UpdateResultStatus.PARTIALLY_UPDATED, resultStatus);
    Assert.assertEquals(((List<String>) oldRecord.get(NULLABLE_LIST_FIELD_NAME)).get(0), "item2");
    resultStatus = collectionTimestampMergeRecordHelper.putOnField(
        oldRecord,
        (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME),
        NULLABLE_LIST_FIELD_NAME,
        null,
        3,
        3);
    Assert.assertEquals(UpdateResultStatus.PARTIALLY_UPDATED, resultStatus);
    Assert.assertEquals(((List<String>) oldRecord.get(NULLABLE_LIST_FIELD_NAME)).get(0), "item3");
  }

  @Test
  public void testHandleMapOpWithNullValue() {
    GenericRecord oldRecord = new GenericData.Record(VALUE_SCHEMA);
    GenericRecord oldRmdRecord = initiateFieldLevelRmdRecord();
    GenericRecord timestampRecord = (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    IndexedHashMap<String, Integer> mapValue = new IndexedHashMap<>();
    mapValue.put("key1", 1);
    GenericRecord nullableMapTsRecord = (GenericRecord) timestampRecord.get(NULLABLE_MAP_FIELD_NAME);
    nullableMapTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 0L);
    nullableMapTsRecord.put(TOP_LEVEL_COLO_ID_FIELD_NAME, 0);
    nullableMapTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 0);
    GenericRecord mapTsRecord = (GenericRecord) timestampRecord.get(MAP_FIELD_NAME);
    mapTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    mapTsRecord.put(TOP_LEVEL_COLO_ID_FIELD_NAME, 1);
    mapTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    oldRecord.put(MAP_FIELD_NAME, mapValue);

    UpdateResultStatus resultStatus;
    CollectionTimestampMergeRecordHelper collectionTimestampMergeRecordHelper =
        new CollectionTimestampMergeRecordHelper();
    SortBasedCollectionFieldOpHandler handlerToTest =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE);

    IndexedHashMap<String, Integer> newMapValue = new IndexedHashMap<>();
    newMapValue.put("key1", 2);
    // Case 1: Test MAP can handle setField to new map.
    resultStatus = collectionTimestampMergeRecordHelper.putOnField(
        oldRecord,
        (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME),
        NULLABLE_MAP_FIELD_NAME,
        newMapValue,
        2,
        1);
    Assert.assertEquals(UpdateResultStatus.COMPLETELY_UPDATED, resultStatus);
    Assert.assertEquals(((Map<String, Object>) oldRecord.get(NULLABLE_MAP_FIELD_NAME)).get("key1"), 2);
    resultStatus = collectionTimestampMergeRecordHelper.putOnField(
        oldRecord,
        (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME),
        MAP_FIELD_NAME,
        newMapValue,
        2,
        1);
    Assert.assertEquals(UpdateResultStatus.COMPLETELY_UPDATED, resultStatus);
    Assert.assertEquals(((Map<String, Object>) oldRecord.get(MAP_FIELD_NAME)).get("key1"), 2);

    // Case 2: Test nullable MAP can handle setField to null.
    resultStatus = collectionTimestampMergeRecordHelper.putOnField(
        oldRecord,
        (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME),
        NULLABLE_MAP_FIELD_NAME,
        null,
        3,
        1);
    Assert.assertNull(oldRecord.get(NULLABLE_MAP_FIELD_NAME));
    Assert.assertEquals(UpdateResultStatus.COMPLETELY_UPDATED, resultStatus);

    // Case 3: Test nullable MAP can handle collection merge from null value.
    Map<String, Object> newEntries = Collections.singletonMap("key2", 2);
    List<String> toRemoveKeys = new LinkedList<>();
    CollectionRmdTimestamp<String> collectionRmdTimestamp = new CollectionRmdTimestamp<>(nullableMapTsRecord);
    resultStatus = handlerToTest
        .handleModifyMap(4L, collectionRmdTimestamp, oldRecord, NULLABLE_MAP_FIELD_NAME, newEntries, toRemoveKeys);
    Assert.assertEquals(UpdateResultStatus.PARTIALLY_UPDATED, resultStatus);
    Assert.assertEquals(((Map<String, Object>) oldRecord.get(NULLABLE_MAP_FIELD_NAME)).get("key2"), 2);

    // Case 4: Test nullable MAP can handle setField to null when there is active elements.
    resultStatus = collectionTimestampMergeRecordHelper.putOnField(
        oldRecord,
        (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME),
        NULLABLE_MAP_FIELD_NAME,
        newMapValue,
        3,
        2);
    Assert.assertEquals(UpdateResultStatus.PARTIALLY_UPDATED, resultStatus);
    Assert.assertEquals(((Map<String, Object>) oldRecord.get(NULLABLE_MAP_FIELD_NAME)).get("key1"), 2);
    resultStatus = collectionTimestampMergeRecordHelper.putOnField(
        oldRecord,
        (GenericRecord) oldRmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME),
        NULLABLE_MAP_FIELD_NAME,
        null,
        3,
        3);
    Assert.assertEquals(UpdateResultStatus.PARTIALLY_UPDATED, resultStatus);
    Assert.assertNull(((Map<String, Object>) oldRecord.get(NULLABLE_MAP_FIELD_NAME)).get("key1"));
    Assert.assertEquals(((Map<String, Object>) oldRecord.get(NULLABLE_MAP_FIELD_NAME)).get("key2"), 2);
  }

  @Test
  public void testHandleCollectionMergeMapOp() {
    GenericRecord currValueRecord = new GenericData.Record(VALUE_SCHEMA);
    CollectionTimestampBuilder collectionTimestampBuilder =
        new CollectionTimestampBuilder(Schema.create(Schema.Type.LONG));
    collectionTimestampBuilder.setTopLevelTimestamps(1L);
    collectionTimestampBuilder.setTopLevelColoID(1);
    collectionTimestampBuilder.setPutOnlyPartLength(1);

    collectionTimestampBuilder.setActiveElementsTimestamps(Arrays.asList(2L, 3L, 4L));
    collectionTimestampBuilder.setDeletedElementTimestamps(Arrays.asList(2L, 3L, 4L));
    collectionTimestampBuilder
        .setDeletedElements(Schema.create(Schema.Type.STRING), Arrays.asList("key5", "key6", "key7"));
    collectionTimestampBuilder
        .setCollectionTimestampSchema(RMD_TIMESTAMP_SCHEMA.getField(NULLABLE_MAP_FIELD_NAME).schema());
    CollectionRmdTimestamp<String> collectionMetadata =
        new CollectionRmdTimestamp<>(collectionTimestampBuilder.build());
    SortBasedCollectionFieldOpHandler handlerToTest =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE);

    // Test not-put-only state.
    IndexedHashMap<String, Integer> mapValue = new IndexedHashMap<>();
    mapValue.put("key1", 1);
    mapValue.put("key2", 1);
    mapValue.put("key3", 1);
    mapValue.put("key4", 1);
    currValueRecord.put(NULLABLE_MAP_FIELD_NAME, mapValue);

    Map<String, Object> newEntries = new HashMap<>();
    newEntries.put("key1", 2);
    newEntries.put("key2", 2);
    newEntries.put("key3", 2);
    newEntries.put("key4", 2);
    newEntries.put("key5", 2);
    newEntries.put("key6", 2);
    newEntries.put("key7", 2);
    List<String> toRemoveKeys = new LinkedList<>();
    handlerToTest
        .handleModifyMap(3L, collectionMetadata, currValueRecord, NULLABLE_MAP_FIELD_NAME, newEntries, toRemoveKeys);

    Map<String, Integer> updatedMap = (Map<String, Integer>) currValueRecord.get(NULLABLE_MAP_FIELD_NAME);
    Map<String, Integer> expectedMap = new HashMap<>();
    expectedMap.put("key1", 2);
    expectedMap.put("key2", 2);
    expectedMap.put("key3", 2);
    expectedMap.put("key4", 1);
    expectedMap.put("key5", 2);
    Assert.assertEquals(updatedMap, expectedMap);
  }

  @Test
  public void testHandleCollectionMergeListOp() {
    GenericRecord currValueRecord = new GenericData.Record(VALUE_SCHEMA);
    CollectionTimestampBuilder collectionTimestampBuilder =
        new CollectionTimestampBuilder(Schema.create(Schema.Type.STRING));
    collectionTimestampBuilder.setTopLevelTimestamps(1L);
    collectionTimestampBuilder.setTopLevelColoID(1);
    collectionTimestampBuilder.setPutOnlyPartLength(1);
    collectionTimestampBuilder.setActiveElementsTimestamps(Arrays.asList(2L, 3L));
    collectionTimestampBuilder.setDeletedElementTimestamps(Arrays.asList(1L, 2L, 3L));
    collectionTimestampBuilder
        .setDeletedElements(Schema.create(Schema.Type.STRING), Arrays.asList("key4", "key5", "key6"));
    collectionTimestampBuilder.setCollectionTimestampSchema(RMD_TIMESTAMP_SCHEMA.getField(LIST_FIELD_NAME).schema());
    CollectionRmdTimestamp<Object> collectionMetadata =
        new CollectionRmdTimestamp<>(collectionTimestampBuilder.build());
    SortBasedCollectionFieldOpHandler handlerToTest =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE);

    // Test not-put-only state.
    currValueRecord.put(LIST_FIELD_NAME, Arrays.asList("key1", "key2", "key3"));

    List<Object> toAddItems = Arrays.asList("key1", "key2", "key3", "key4", "key5", "key6");
    List<Object> toRemoveItems = new LinkedList<>();
    handlerToTest.handleModifyList(2L, collectionMetadata, currValueRecord, LIST_FIELD_NAME, toAddItems, toRemoveItems);

    List<String> updatedMap = (List<String>) currValueRecord.get(LIST_FIELD_NAME);
    Assert.assertEquals(updatedMap, Arrays.asList("key1", "key2", "key3", "key4"));
  }

  private GenericRecord initiateFieldLevelRmdRecord() {
    GenericRecord rmdRecord = new GenericData.Record(RMD_SCHEMA);
    Schema fieldLevelTimestampSchema =
        rmdRecord.getSchema().getField(RmdConstants.TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);

    GenericRecord timestampsRecord = new GenericData.Record(fieldLevelTimestampSchema);

    for (Schema.Field field: timestampsRecord.getSchema().getFields()) {
      // Below logic only works for collection field.
      GenericRecord fieldRecord = new GenericData.Record(field.schema());
      fieldRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Collections.emptyList());
      fieldRecord.put(DELETED_ELEM_TS_FIELD_NAME, Collections.emptyList());
      fieldRecord.put(DELETED_ELEM_FIELD_NAME, Collections.emptyList());
      timestampsRecord.put(field.name(), fieldRecord);
    }
    rmdRecord.put(RmdConstants.TIMESTAMP_FIELD_NAME, timestampsRecord);
    rmdRecord.put(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD, new ArrayList<>());
    return rmdRecord;
  }
}
