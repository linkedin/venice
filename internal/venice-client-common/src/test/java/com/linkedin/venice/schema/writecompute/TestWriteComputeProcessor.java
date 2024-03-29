package com.linkedin.venice.schema.writecompute;

import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.MAP_DIFF;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.MAP_UNION;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.SET_DIFF;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.SET_UNION;
import static org.apache.avro.Schema.Type.INT;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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

  protected WriteComputeHandlerV1 getWriteComputeHandler() {
    return new WriteComputeHandlerV1();
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
