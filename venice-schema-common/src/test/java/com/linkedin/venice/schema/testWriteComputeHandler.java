package com.linkedin.venice.schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.schema.WriteComputeSchemaConverter.*;

import static org.apache.avro.Schema.Type.*;


public class testWriteComputeHandler {
  private String recordSchemaStr = "{\n" +
      "  \"type\" : \"record\",\n" +
      "  \"name\" : \"testRecord\",\n" +
      "  \"namespace\" : \"com.linkedin.avro\",\n" +
      "  \"fields\" : [ {\n" +
      "    \"name\" : \"hits\",\n" +
      "    \"type\" : {\n" +
      "      \"type\" : \"array\",\n" +
      "      \"items\" : {\n" +
      "        \"type\" : \"record\",\n" +
      "        \"name\" : \"JobAlertHit\",\n" +
      "        \"fields\" : [ {\n" +
      "          \"name\" : \"memberId\",\n" +
      "          \"type\" : \"long\"\n" +
      "        }, {\n" +
      "          \"name\" : \"searchId\",\n" +
      "          \"type\" : \"long\"\n" +
      "        } ]\n"
      + "      }\n" +
      "    },\n" +
      "    \"default\" : [ ]\n" +
      "  }, {\n" +
      "    \"name\" : \"hasNext\",\n" +
      "    \"type\" : \"boolean\",\n" +
      "    \"default\" : false\n" +
      "  } ]\n" +
      "}";


  private String simpleRecordSchemaStr = "{\n" +
      "  \"type\" : \"record\",\n" +
      "  \"name\" : \"simpleTestRecord\",\n" +
      "  \"namespace\" : \"com.linkedin.avro\",\n" +
      "  \"fields\" : [ {\n" +
      "    \"name\" : \"field1\",\n" +
      "    \"type\" : \"int\",\n" +
      "    \"default\" : 0\n" +
      "  }, {\n" +
      "    \"name\" : \"field2\",\n" +
      "    \"type\" : \"int\",\n" +
      "    \"default\" : 0\n" +
      " } ]\n" +
      "}";

  private String nullableRecordStr = "{\n" +
      "  \"type\" : \"record\",\n" +
      "  \"name\" : \"nullableRecord\",\n" +
      "  \"fields\" : [ {\n" + "    \"name\" : \"nullableArray\",\n" +
      "    \"type\" : [ \"null\", {\n" +
      "      \"type\" : \"array\",\n" +
      "      \"items\" : \"int\"\n" +
      "    } ],\n" + "    \"default\" : null\n" +
      "  }, {\n" + "    \"name\" : \"intField\",\n" +
      "    \"type\" : \"int\",\n" +
      "    \"default\" : 0\n" +
      "  } ]\n" +
      "}";

  private String nestedRecordStr = "{\n" +
      "  \"type\" : \"record\",\n" +
      "  \"name\" : \"testRecord\",\n" +
      "  \"fields\" : [ {\n" +
      "    \"name\" : \"nestedRecord\",\n" +
      "    \"type\" : {\n" +
      "      \"type\" : \"record\",\n" +
      "      \"name\" : \"nestedRecord\",\n" +
      "      \"fields\" : [ {\n" +
      "        \"name\" : \"intField\",\n" +
      "        \"type\" : \"int\"\n" +
      "      } ]\n" +
      "    },\n" +
      "    \"default\" : {\n" +
      "      \"intField\" : 1\n" +
      "    }\n" +
      "  } ]\n" +
      "}";

  @Test
  public void testCanUpdateArray() {
    Schema arraySchema = Schema.createArray(Schema.create(INT));
    Schema arrayWriteComputeSchema = WriteComputeSchemaConverter.convert(arraySchema);

    WriteComputeHandler arrayAdapter = new WriteComputeHandler(arraySchema, arrayWriteComputeSchema);

    GenericData.Record collectionUpdateRecord = new GenericData.Record(arrayWriteComputeSchema.getTypes().get(0));
    collectionUpdateRecord.put(SET_UNION, Arrays.asList(1, 2));
    collectionUpdateRecord.put(SET_DIFF, Arrays.asList(3, 4));

    GenericData.Array originalArray = new GenericData.Array(arraySchema, Arrays.asList(1, 3));
    Object result = arrayAdapter.updateArray(arraySchema, originalArray, collectionUpdateRecord);
    Assert.assertTrue(result instanceof List);
    Assert.assertTrue(((List) result).contains(1));
    Assert.assertFalse(((List) result).contains(3));

    //test passing a "null" as the original value. WriteComputeAdapter is supposed to construct
    //a new list
    result = arrayAdapter.updateArray(arraySchema, null, collectionUpdateRecord);

    Assert.assertTrue(((List) result).contains(1));
    Assert.assertTrue(((List) result).contains(2));

    //test replacing original array entirely
    GenericData.Array updatedArray = new GenericData.Array(arraySchema, Arrays.asList(2));
    result = arrayAdapter.updateArray(arraySchema, originalArray, updatedArray);
    Assert.assertTrue(((List) result).contains(2));
    Assert.assertFalse(((List) result).contains(1));
    Assert.assertFalse(((List) result).contains(3));
  }

  @Test
  public void testCanUpdateMap() {
    Schema mapSchema = Schema.createMap(Schema.create(INT));
    Schema mapWriteComputeSchema = WriteComputeSchemaConverter.convert(mapSchema);

    WriteComputeHandler mapAdapter = new WriteComputeHandler(mapSchema, mapWriteComputeSchema);

    GenericData.Record mapUpdateRecord = new GenericData.Record(mapWriteComputeSchema.getTypes().get(0));
    Map map = new HashMap();
    map.put(2, 2);
    map.put(3, 3);
    mapUpdateRecord.put(MAP_UNION, map);
    mapUpdateRecord.put(MAP_DIFF, Arrays.asList(4));

    Map originalMap = new HashMap();
    originalMap.put(1, 1);
    originalMap.put(4, 4);

    Object result = mapAdapter.updateMap(originalMap, mapUpdateRecord);
    Assert.assertTrue(result instanceof Map);
    Assert.assertEquals(((Map) result).get(1), 1);
    Assert.assertEquals(((Map) result).get(2), 2);
    Assert.assertEquals(((Map) result).get(3), 3);
    Assert.assertFalse( ((Map) result).containsKey(4));

    //test passing a "null" as the original value
    result = mapAdapter.updateMap(null, mapUpdateRecord);
    Assert.assertEquals(((Map) result).get(2), 2);
    Assert.assertEquals(((Map) result).get(3), 3);

    //test replacing original map entirely
    Map updatedMap = new HashMap();
    updatedMap.put(5, 5);

    result = mapAdapter.updateMap(originalMap, updatedMap);
    Assert.assertEquals(((Map) result).get(5), 5);
    Assert.assertFalse( ((Map) result).containsKey(1));
    Assert.assertFalse( ((Map) result).containsKey(4));
  }

  @Test
  public void testCanUpdateRecord() {
    Schema recordSchema = Schema.parse(recordSchemaStr);
    Schema recordWriteComputeSchema = WriteComputeSchemaConverter.convert(recordSchema);

    WriteComputeHandler
        recordAdapter = WriteComputeHandler.getWriteComputeAdapter(recordSchema, recordWriteComputeSchema);

    //construct original record
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

    //construct write compute operation record
    Schema noOpSchema = recordWriteComputeSchema.getTypes().get(0).getField("hits").schema().getTypes().get(0);
    GenericData.Record noOpRecord = new GenericData.Record(noOpSchema);

    //update "hasNext" to false
    GenericData.Record recordUpdateRecord = new GenericData.Record(recordWriteComputeSchema.getTypes().get(0));
    recordUpdateRecord.put("hits", noOpRecord);
    recordUpdateRecord.put("hasNext", true);

    Object result = recordAdapter.updateRecord(originalRecord, recordUpdateRecord);
    Assert.assertTrue(result instanceof GenericData.Record);
    Assert.assertEquals(((GenericData.Record)result).get("hits"), innerArray);
    Assert.assertEquals(((GenericData.Record)result).get("hasNext"), true);

    //add new element to the list
    GenericData.Record newInnerRecord = new GenericData.Record(innerRecordSchema);
    newInnerRecord.put("memberId", 1L);
    newInnerRecord.put("searchId", 20L);

    GenericData.Record collectionUpdateRecord =
        new GenericData.Record(WriteComputeSchemaConverter.convert(innerArraySchema).getTypes().get(0));
    collectionUpdateRecord.put(SET_UNION, Collections.singletonList(newInnerRecord));
    collectionUpdateRecord.put(SET_DIFF, Collections.emptyList());
    recordUpdateRecord.put("hits", collectionUpdateRecord);

    result = recordAdapter.updateRecord(originalRecord, recordUpdateRecord);
    List hitsList = (List) ((GenericData.Record) result).get("hits");
    Assert.assertEquals(hitsList.size(), 2);
    Assert.assertTrue(hitsList.contains(innerRecord));
    Assert.assertTrue(hitsList.contains(newInnerRecord));

    //test passing a "null" as the original value. The write compute adapter should set noOp field to
    //its default value if it's possible
    recordUpdateRecord.put("hasNext", noOpRecord);
    result = recordAdapter.updateRecord(null, recordUpdateRecord);
    Assert.assertEquals(((GenericData.Record)result).get("hasNext"), false);
  }

  @Test
  public void testCanUpdateNullableUnion() {
    Schema nullableRecord = Schema.parse(nullableRecordStr);
    Schema writeComputeSchema = WriteComputeSchemaConverter.convert(nullableRecord);

    WriteComputeHandler recordAdapter =
        WriteComputeHandler.getWriteComputeAdapter(nullableRecord, writeComputeSchema);

    //construct an empty write compute schema. WC adapter is supposed to construct the
    //original value by using default values.
    GenericData.Record writeComputeRecord = new GenericData.Record(writeComputeSchema.getTypes().get(0));

    Schema noOpSchema = writeComputeSchema.getTypes().get(0).getField("nullableArray").schema().getTypes().get(0);
    GenericData.Record noOpRecord = new GenericData.Record(noOpSchema);

    writeComputeRecord.put("nullableArray", noOpRecord);
    writeComputeRecord.put("intField", noOpRecord);

    GenericData.Record result = (GenericData.Record) recordAdapter.updateRecord(null, writeComputeRecord);
    Assert.assertNull(result.get("nullableArray"));
    Assert.assertEquals(result.get("intField"), 0);

    //use a array operation to update the nullable field
    GenericData.Record listOpsRecord =
        new GenericData.Record(writeComputeSchema.getTypes().get(0).getField("nullableArray").schema().getTypes().get(2));
    listOpsRecord.put(SET_UNION, Arrays.asList(1, 2));
    listOpsRecord.put(SET_DIFF, Collections.emptyList());
    writeComputeRecord.put("nullableArray", listOpsRecord);

    result = (GenericData.Record) recordAdapter.updateRecord(result, writeComputeRecord);
    GenericArray array = (GenericArray) result.get("nullableArray");
    Assert.assertEquals(array.size(), 2);
    Assert.assertTrue(array.contains(1) && array.contains(2));
  }

  @Test
  public void testCanDeleteFullRecord() {
    Schema recordSchema = Schema.parse(simpleRecordSchemaStr);
    Schema recordWriteComputeSchema = WriteComputeSchemaConverter.convert(recordSchema);

    WriteComputeHandler recordAdapter =
        WriteComputeHandler.getWriteComputeAdapter(recordSchema, recordWriteComputeSchema);

    //construct original record
    GenericData.Record originalRecord = new GenericData.Record(recordSchema);
    originalRecord.put("field1", 0);
    originalRecord.put("field2", 1);

    //construct write compute operation record
    Schema deleteSchema = recordWriteComputeSchema.getTypes().get(1);
    GenericData.Record deleteRecord = new GenericData.Record(deleteSchema);

    Object result = recordAdapter.updateRecord(originalRecord, deleteRecord);
    Assert.assertEquals(result, null);
  }

  @Test
  public void testCanHandleNestedRecord() {
    Schema recordSchema = Schema.parse(nestedRecordStr);
    Schema recordWriteComputeUnionSchema = WriteComputeSchemaConverter.convert(recordSchema);

    WriteComputeHandler recordAdapter =
        WriteComputeHandler.getWriteComputeAdapter(recordSchema, recordWriteComputeUnionSchema);

    Schema nestedRecordSchema =  recordSchema.getField("nestedRecord").schema();
    GenericData.Record nestedRecord = new GenericData.Record(nestedRecordSchema);
    nestedRecord.put("intField", 1);

    Schema writeComputeRecordSchema = recordWriteComputeUnionSchema.getTypes().get(0);
    GenericData.Record writeComputeRecord = new GenericData.Record(writeComputeRecordSchema);
    writeComputeRecord.put("nestedRecord", nestedRecord);

    GenericData.Record result = (GenericData.Record) recordAdapter.updateRecord(null, writeComputeRecord);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.get("nestedRecord"), nestedRecord);
  }
}
