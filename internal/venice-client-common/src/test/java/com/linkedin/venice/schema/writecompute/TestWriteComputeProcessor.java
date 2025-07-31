package com.linkedin.venice.schema.writecompute;

import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.MAP_DIFF;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.MAP_UNION;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.SET_DIFF;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.SET_UNION;
import static org.apache.avro.Schema.Type.INT;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestWriteComputeProcessor {
  private final static String recordSchemaStr = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"testRecord\",\n"
      + "  \"namespace\" : \"com.linkedin.avro\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"hits\",\n"
      + "    \"type\" : {\n" + "      \"type\" : \"array\",\n" + "      \"items\" : {\n"
      + "        \"type\" : \"record\",\n" + "        \"name\" : \"JobAlertHit\",\n" + "        \"fields\" : [ {\n"
      + "          \"name\" : \"memberId\",\n" + "          \"type\" : \"long\"\n" + "        }, {\n"
      + "          \"name\" : \"searchId\",\n" + "          \"type\" : \"long\"\n" + "        } ]\n" + "      }\n"
      + "    },\n" + "    \"default\" : [ ]\n" + "  }, {\n" + "    \"name\" : \"hasNext\",\n"
      + "    \"type\" : \"boolean\",\n" + "    \"default\" : false\n" + "  } ]\n" + "}";

  private final WriteComputeSchemaConverter writeComputeSchemaConverter = WriteComputeSchemaConverter.getInstance();
  private static final Schema VALUE_SCHEMA =
      AvroCompatibilityHelper.parse(TestUtils.loadFileAsString("MergeUpdateV1.avsc"));
  private static final Schema UPDATE_SCHEMA =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA);

  protected WriteComputeHandlerV1 getWriteComputeHandler() {
    return new WriteComputeHandlerV1();
  }

  @Test
  public void testMergeTwoMapUpdate() {
    WriteComputeHandlerV1 updateHandler = new WriteComputeHandlerV1();
    GenericRecord updateV1;
    GenericRecord updateV2;
    GenericRecord resultUpdate;
    Map<String, Integer> mapUnion;
    List<String> mapDiff;

    // Case 1: partial update -> collection merge
    Map<String, Integer> map = new HashMap<>();
    map.put("a", 1);
    map.put("b", 1);
    map.put("c", 1);
    updateV1 = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", "abc")
        .setNewFieldValue("IntMapField", map)
        .setNewFieldValue("NullableIntMapField", null)
        .build();
    Map<String, Integer> map2 = new HashMap<>();
    map2.put("a", 2);
    map2.put("d", 1);
    updateV2 = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", "def")
        .setEntriesToAddToMapField("IntMapField", map2)
        .setKeysToRemoveFromMapField("IntMapField", Collections.singletonList("c"))
        .setEntriesToAddToMapField("NullableIntMapField", map2)
        .setKeysToRemoveFromMapField("NullableIntMapField", Collections.singletonList("c"))
        .build();
    resultUpdate = updateHandler.mergeUpdateRecord(VALUE_SCHEMA, updateV1, updateV2);
    Assert.assertTrue(resultUpdate.get("IntMapField") instanceof Map);
    Map<String, Integer> updatedMap = (Map<String, Integer>) resultUpdate.get("IntMapField");
    Assert.assertEquals(updatedMap.size(), 3);
    Assert.assertEquals((int) updatedMap.get("a"), 2);
    Assert.assertEquals((int) updatedMap.get("b"), 1);
    Assert.assertEquals((int) updatedMap.get("d"), 1);
    updatedMap = (Map<String, Integer>) resultUpdate.get("NullableIntMapField");
    Assert.assertEquals(updatedMap.size(), 2);
    Assert.assertEquals((int) updatedMap.get("a"), 2);
    Assert.assertEquals((int) updatedMap.get("d"), 1);

    // Case 2: collection merge -> partial update
    map = new HashMap<>();
    map.put("a", 1);
    map.put("b", 1);
    map.put("d", 1);
    map2 = new HashMap<>();
    map2.put("a", 2);
    map2.put("c", 2);
    updateV1 = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", "def")
        .setEntriesToAddToMapField("IntMapField", map)
        .setKeysToRemoveFromMapField("IntMapField", Collections.singletonList("c"))
        .setEntriesToAddToMapField("NullableIntMapField", map)
        .setKeysToRemoveFromMapField("NullableIntMapField", Collections.singletonList("c"))
        .build();
    updateV2 = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", "abc")
        .setNewFieldValue("IntMapField", map2)
        .setNewFieldValue("NullableIntMapField", null)
        .build();
    resultUpdate = updateHandler.mergeUpdateRecord(VALUE_SCHEMA, updateV1, updateV2);
    Map<String, Integer> updatedField = (Map<String, Integer>) resultUpdate.get("IntMapField");
    Assert.assertEquals(updatedField.size(), 2);
    Assert.assertEquals((int) updatedField.get("a"), 2);
    Assert.assertEquals((int) updatedField.get("c"), 2);
    Assert.assertNull(resultUpdate.get("NullableIntMapField"));

    // Case 3: two collection merge
    map = new HashMap<>();
    map.put("a", 1);
    map.put("b", 1);
    map.put("c", 1);

    map2 = new HashMap<>();
    map2.put("a", 2);
    map2.put("d", 2);

    updateV1 = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", "def")
        .setEntriesToAddToMapField("IntMapField", map)
        .setKeysToRemoveFromMapField("IntMapField", Collections.singletonList("d"))
        .build();
    updateV2 = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", "abc")
        .setEntriesToAddToMapField("IntMapField", map2)
        .setKeysToRemoveFromMapField("IntMapField", Collections.singletonList("c"))
        .build();
    resultUpdate = updateHandler.mergeUpdateRecord(VALUE_SCHEMA, updateV1, updateV2);
    Assert.assertTrue(resultUpdate.get("IntMapField") instanceof IndexedRecord);
    mapUnion = (Map<String, Integer>) ((GenericRecord) resultUpdate.get("IntMapField")).get(MAP_UNION);
    mapDiff = (List<String>) ((GenericRecord) resultUpdate.get("IntMapField")).get(MAP_DIFF);
    Assert.assertEquals(mapUnion.size(), 4);
    Assert.assertEquals(mapDiff.size(), 1);
    Assert.assertEquals((int) mapUnion.get("a"), 2);

    // Case 4: one update is NoOp
    updateV1 = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", "def")
        .setEntriesToAddToMapField("IntMapField", Collections.singletonMap("a", 1))
        .setKeysToRemoveFromMapField("IntMapField", Collections.singletonList("d"))
        .build();
    updateV2 = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", "abc")
        .setEntriesToAddToMapField("NullableIntMapField", Collections.singletonMap("a", 1))
        .setKeysToRemoveFromMapField("NullableIntMapField", Collections.singletonList("d"))
        .build();
    resultUpdate = updateHandler.mergeUpdateRecord(VALUE_SCHEMA, updateV1, updateV2);
    Assert.assertTrue(resultUpdate.get("IntMapField") instanceof IndexedRecord);
    mapUnion = (Map<String, Integer>) ((GenericRecord) resultUpdate.get("IntMapField")).get(MAP_UNION);
    mapDiff = (List<String>) ((GenericRecord) resultUpdate.get("IntMapField")).get(MAP_DIFF);
    Assert.assertEquals(mapUnion.size(), 1);
    Assert.assertEquals(mapDiff.size(), 1);
    Assert.assertTrue(resultUpdate.get("NullableIntMapField") instanceof IndexedRecord);
    mapUnion = (Map<String, Integer>) ((GenericRecord) resultUpdate.get("NullableIntMapField")).get(MAP_UNION);
    mapDiff = (List<String>) ((GenericRecord) resultUpdate.get("NullableIntMapField")).get(MAP_DIFF);
    Assert.assertEquals(mapUnion.size(), 1);
    Assert.assertEquals(mapDiff.size(), 1);
  }

  @Test
  public void testMergeTwoListUpdate() {
    WriteComputeHandlerV1 updateHandler = new WriteComputeHandlerV1();
    GenericRecord updateV1;
    GenericRecord updateV2;
    GenericRecord resultUpdate;
    List<String> unionList;
    List<String> diffList;

    // Case 1: partial update -> collection merge
    updateV1 = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", "abc")
        .setNewFieldValue("StringListField", Arrays.asList("abc", "def", "xyz"))
        .setNewFieldValue("NullableStringListField", null)
        .build();
    updateV2 = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("name", "def")
        .setElementsToAddToListField("StringListField", Arrays.asList("def", "ghi"))
        .setElementsToRemoveFromListField("StringListField", Collections.singletonList("xyz"))
        .setElementsToAddToListField("NullableStringListField", Arrays.asList("def", "ghi"))
        .setElementsToRemoveFromListField("NullableStringListField", Collections.singletonList("xyz"))
        .build();

    resultUpdate = updateHandler.mergeUpdateRecord(VALUE_SCHEMA, updateV1, updateV2);

    Assert.assertTrue(resultUpdate.get("StringListField") instanceof List);
    Assert.assertTrue(resultUpdate.get("NullableStringListField") instanceof List);

    List<String> updatedList = (List<String>) resultUpdate.get("StringListField");
    Assert.assertEquals(updatedList.size(), 3);
    Assert.assertTrue(updatedList.contains("abc"));
    Assert.assertTrue(updatedList.contains("def"));
    Assert.assertTrue(updatedList.contains("ghi"));
    updatedList = (List<String>) resultUpdate.get("NullableStringListField");
    Assert.assertEquals(updatedList.size(), 2);
    Assert.assertTrue(updatedList.contains("def"));
    Assert.assertTrue(updatedList.contains("ghi"));

    // Case 2: collection merge -> partial update
    updateV1 = new UpdateBuilderImpl(UPDATE_SCHEMA)
        .setElementsToAddToListField("NullableStringListField", Arrays.asList("def", "ghi"))
        .setElementsToRemoveFromListField("NullableStringListField", Collections.singletonList("xyz"))
        .build();
    updateV2 = new UpdateBuilderImpl(UPDATE_SCHEMA).setNewFieldValue("NullableStringListField", null).build();
    resultUpdate = updateHandler.mergeUpdateRecord(VALUE_SCHEMA, updateV1, updateV2);
    Assert.assertNull(resultUpdate.get("NullableStringListField"));
    Assert.assertEquals(
        WriteComputeOperation.getFieldOperationType(resultUpdate.get("StringListField")),
        WriteComputeOperation.NO_OP_ON_FIELD);

    // Case 3: two collection merge
    updateV1 =
        new UpdateBuilderImpl(UPDATE_SCHEMA).setElementsToAddToListField("StringListField", Arrays.asList("a", "b"))
            .setElementsToRemoveFromListField("StringListField", Collections.singletonList("c"))
            .build();
    updateV2 =
        new UpdateBuilderImpl(UPDATE_SCHEMA).setElementsToAddToListField("StringListField", Arrays.asList("d", "c"))
            .setElementsToRemoveFromListField("StringListField", Collections.singletonList("b"))
            .build();
    resultUpdate = updateHandler.mergeUpdateRecord(VALUE_SCHEMA, updateV1, updateV2);
    Assert.assertTrue(resultUpdate.get("StringListField") instanceof IndexedRecord);
    unionList = (List<String>) ((GenericRecord) resultUpdate.get("StringListField")).get(SET_UNION);
    diffList = (List<String>) ((GenericRecord) resultUpdate.get("StringListField")).get(SET_DIFF);
    Assert.assertEquals(unionList.size(), 4);
    Assert.assertEquals(diffList.size(), 1);

    // Case 4: one update is NoOp
    updateV1 =
        new UpdateBuilderImpl(UPDATE_SCHEMA).setElementsToAddToListField("StringListField", Arrays.asList("a", "b"))
            .setElementsToRemoveFromListField("StringListField", Collections.singletonList("c"))
            .build();
    updateV2 = new UpdateBuilderImpl(UPDATE_SCHEMA)
        .setElementsToAddToListField("NullableStringListField", Arrays.asList("d", "c"))
        .setElementsToRemoveFromListField("NullableStringListField", Collections.singletonList("b"))
        .build();
    resultUpdate = updateHandler.mergeUpdateRecord(VALUE_SCHEMA, updateV1, updateV2);
    Assert.assertTrue(resultUpdate.get("StringListField") instanceof IndexedRecord);
    unionList = (List<String>) ((GenericRecord) resultUpdate.get("NullableStringListField")).get(SET_UNION);
    diffList = (List<String>) ((GenericRecord) resultUpdate.get("NullableStringListField")).get(SET_DIFF);
    Assert.assertEquals(unionList.size(), 2);
    Assert.assertEquals(diffList.size(), 1);
    unionList = (List<String>) ((GenericRecord) resultUpdate.get("StringListField")).get(SET_UNION);
    diffList = (List<String>) ((GenericRecord) resultUpdate.get("StringListField")).get(SET_DIFF);
    Assert.assertEquals(unionList.size(), 2);
    Assert.assertEquals(diffList.size(), 1);
  }

  @Test
  public void testCanUpdateArray() {
    Schema arraySchema = Schema.createArray(Schema.create(INT));
    Schema arrayWriteComputeSchema = writeComputeSchemaConverter.convert(arraySchema);
    WriteComputeHandlerV1 writeComputeHandler = getWriteComputeHandler();

    GenericData.Record collectionUpdateRecord = new GenericData.Record(arrayWriteComputeSchema.getTypes().get(0));
    collectionUpdateRecord.put(SET_UNION, Arrays.asList(1, 2));
    collectionUpdateRecord.put(SET_DIFF, Arrays.asList(3, 4));

    GenericData.Array originalArray = new GenericData.Array(arraySchema, Arrays.asList(1, 3));
    Object result = writeComputeHandler.updateArray(arraySchema, originalArray, collectionUpdateRecord);
    Assert.assertTrue(result instanceof List);
    Assert.assertTrue(((List) result).contains(1));
    Assert.assertFalse(((List) result).contains(3));

    // test passing a "null" as the original value. WriteComputeAdapter is supposed to construct
    // a new list
    result = writeComputeHandler.updateArray(arraySchema, null, collectionUpdateRecord);

    Assert.assertTrue(((List) result).contains(1));
    Assert.assertTrue(((List) result).contains(2));

    // test replacing original array entirely
    GenericData.Array updatedArray = new GenericData.Array(arraySchema, Arrays.asList(2));
    result = writeComputeHandler.updateArray(arraySchema, originalArray, updatedArray);
    Assert.assertTrue(((List) result).contains(2));
    Assert.assertFalse(((List) result).contains(1));
    Assert.assertFalse(((List) result).contains(3));
  }

  @Test
  public void testCanUpdateMap() {
    Schema mapSchema = Schema.createMap(Schema.create(INT));
    Schema mapWriteComputeSchema = writeComputeSchemaConverter.convert(mapSchema);
    WriteComputeHandlerV1 writeComputeHandler = getWriteComputeHandler();

    GenericData.Record mapUpdateRecord = new GenericData.Record(mapWriteComputeSchema.getTypes().get(0));
    Map<Integer, Integer> map = new HashMap<>();
    map.put(2, 2);
    map.put(3, 3);
    mapUpdateRecord.put(MAP_UNION, map);
    mapUpdateRecord.put(MAP_DIFF, Collections.singletonList(4));

    Map<Integer, Integer> originalMap = new HashMap<>();
    originalMap.put(1, 1);
    originalMap.put(4, 4);

    Object result = writeComputeHandler.updateMap(originalMap, mapUpdateRecord);
    Assert.assertTrue(result instanceof Map);
    Assert.assertEquals(((Map<?, ?>) result).get(1), 1);
    Assert.assertEquals(((Map<?, ?>) result).get(2), 2);
    Assert.assertEquals(((Map<?, ?>) result).get(3), 3);
    Assert.assertFalse(((Map<?, ?>) result).containsKey(4));

    // test passing a "null" as the original value
    result = writeComputeHandler.updateMap(null, mapUpdateRecord);
    Assert.assertEquals(((Map<?, ?>) result).get(2), 2);
    Assert.assertEquals(((Map<?, ?>) result).get(3), 3);

    // test replacing original map entirely
    Map<Integer, Integer> updatedMap = new HashMap<>();
    updatedMap.put(5, 5);

    result = writeComputeHandler.updateMap(originalMap, updatedMap);
    Assert.assertEquals(((Map<?, ?>) result).get(5), 5);
    Assert.assertFalse(((Map<?, ?>) result).containsKey(1));
    Assert.assertFalse(((Map<?, ?>) result).containsKey(4));
  }

  @Test
  public void testCanUpdateRecord() {
    Schema recordSchema = AvroCompatibilityHelper.parse(recordSchemaStr);
    Schema recordWriteComputeSchema = writeComputeSchemaConverter.convertFromValueRecordSchema(recordSchema);
    WriteComputeHandlerV1 writeComputeHandler = getWriteComputeHandler();

    // construct original record
    Schema innerArraySchema = recordSchema.getField("hits").schema();
    Schema innerRecordSchema = innerArraySchema.getElementType();

    GenericData.Record innerRecord = new GenericData.Record(innerRecordSchema);
    innerRecord.put("memberId", 1L);
    innerRecord.put("searchId", 10L);
    GenericData.Array innerArray = new GenericData.Array(1, innerArraySchema);
    innerArray.add(innerRecord);

    GenericData.Record originalRecord = new GenericData.Record(recordSchema);
    originalRecord.put("hits", innerArray);
    originalRecord.put("hasNext", true);

    // construct write compute operation record
    Schema noOpSchema = recordWriteComputeSchema.getField("hits").schema().getTypes().get(0);
    GenericData.Record noOpRecord = new GenericData.Record(noOpSchema);

    // update "hasNext" to false
    GenericData.Record recordUpdateRecord = new GenericData.Record(recordWriteComputeSchema);
    recordUpdateRecord.put("hits", noOpRecord);
    recordUpdateRecord.put("hasNext", true);

    Object result = writeComputeHandler.updateValueRecord(recordSchema, originalRecord, recordUpdateRecord);
    Assert.assertTrue(result instanceof GenericData.Record);
    Assert.assertEquals(((GenericData.Record) result).get("hits"), innerArray);
    Assert.assertEquals(((GenericData.Record) result).get("hasNext"), true);

    // add new element to the list
    GenericData.Record newInnerRecord = new GenericData.Record(innerRecordSchema);
    newInnerRecord.put("memberId", 1L);
    newInnerRecord.put("searchId", 20L);

    GenericData.Record collectionUpdateRecord =
        new GenericData.Record(writeComputeSchemaConverter.convert(innerArraySchema).getTypes().get(0));
    collectionUpdateRecord.put(SET_UNION, Collections.singletonList(newInnerRecord));
    collectionUpdateRecord.put(SET_DIFF, Collections.emptyList());
    recordUpdateRecord.put("hits", collectionUpdateRecord);

    result = writeComputeHandler.updateValueRecord(recordSchema, originalRecord, recordUpdateRecord);
    List hitsList = (List) ((GenericData.Record) result).get("hits");
    Assert.assertEquals(hitsList.size(), 2);
    Assert.assertTrue(hitsList.contains(innerRecord));
    Assert.assertTrue(hitsList.contains(newInnerRecord));

    // test passing a "null" as the original value. The write compute adapter should set noOp field to
    // its default value if it's possible
    recordUpdateRecord.put("hasNext", noOpRecord);
    result = writeComputeHandler.updateValueRecord(recordSchema, null, recordUpdateRecord);
    Assert.assertEquals(((GenericData.Record) result).get("hasNext"), false);
  }
}
