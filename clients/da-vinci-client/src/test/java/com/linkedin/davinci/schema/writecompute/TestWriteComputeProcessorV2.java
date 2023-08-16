package com.linkedin.davinci.schema.writecompute;

import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.*;
import static org.apache.avro.Schema.Type.*;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.schema.merge.CollectionTimestampMergeRecordHelper;
import com.linkedin.venice.schema.writecompute.TestWriteComputeProcessor;
import com.linkedin.venice.schema.writecompute.WriteComputeHandlerV1;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestWriteComputeProcessorV2 extends TestWriteComputeProcessor {
  private final static String nullableRecordStr =
      "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"nullableRecord\",\n" + "  \"fields\" : [ {\n"
          + "    \"name\" : \"nullableArray\",\n" + "    \"type\" : [ \"null\", {\n" + "      \"type\" : \"array\",\n"
          + "      \"items\" : \"int\"\n" + "    } ],\n" + "    \"default\" : null\n" + "  }, {\n"
          + "    \"name\" : \"intField\",\n" + "    \"type\" : \"int\",\n" + "    \"default\" : 0\n" + "  } ]\n" + "}";

  private final static String nestedRecordStr = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"testRecord\",\n"
      + "  \"fields\" : [ {\n" + "    \"name\" : \"nestedRecord\",\n" + "    \"type\" : {\n"
      + "      \"type\" : \"record\",\n" + "      \"name\" : \"nestedRecord\",\n" + "      \"fields\" : [ {\n"
      + "        \"name\" : \"intField\",\n" + "        \"type\" : \"int\"\n" + "      } ]\n" + "    },\n"
      + "    \"default\" : {\n" + "      \"intField\" : 1\n" + "    }\n" + "  } ]\n" + "}";

  private final WriteComputeSchemaConverter writeComputeSchemaConverter = WriteComputeSchemaConverter.getInstance();

  @Override
  protected WriteComputeHandlerV1 getWriteComputeHandler() {
    return new WriteComputeHandlerV2(new CollectionTimestampMergeRecordHelper());
  }

  @Test
  public void testCanUpdateNullableUnion() {
    Schema nullableRecordSchema = AvroCompatibilityHelper.parse(nullableRecordStr);
    Schema writeComputeSchema = writeComputeSchemaConverter.convertFromValueRecordSchema(nullableRecordSchema);
    WriteComputeProcessor writeComputeProcessor = new WriteComputeProcessor(new CollectionTimestampMergeRecordHelper());

    // construct an empty write compute schema. WC adapter is supposed to construct the
    // original value by using default values.
    GenericData.Record writeComputeRecord = new GenericData.Record(writeComputeSchema);

    Schema noOpSchema = writeComputeSchema.getField("nullableArray").schema().getTypes().get(0);
    GenericData.Record noOpRecord = new GenericData.Record(noOpSchema);

    writeComputeRecord.put("nullableArray", noOpRecord);
    writeComputeRecord.put("intField", noOpRecord);

    GenericData.Record result =
        (GenericData.Record) writeComputeProcessor.updateRecord(nullableRecordSchema, null, writeComputeRecord);

    Assert.assertNull(result.get("nullableArray"));
    Assert.assertEquals(result.get("intField"), 0);

    // use a array operation to update the nullable field
    GenericData.Record listOpsRecord =
        new GenericData.Record(writeComputeSchema.getField("nullableArray").schema().getTypes().get(2));
    listOpsRecord.put(SET_UNION, Arrays.asList(1, 2));
    listOpsRecord.put(SET_DIFF, Collections.emptyList());
    writeComputeRecord.put("nullableArray", listOpsRecord);

    result = (GenericData.Record) writeComputeProcessor.updateRecord(nullableRecordSchema, result, writeComputeRecord);
    GenericArray array = (GenericArray) result.get("nullableArray");
    Assert.assertEquals(array.size(), 2);
    Assert.assertTrue(array.contains(1) && array.contains(2));
  }

  @Test
  public void testCanHandleNestedRecord() {
    Schema recordSchema = AvroCompatibilityHelper.parse(nestedRecordStr);
    Schema recordWriteComputeUnionSchema = writeComputeSchemaConverter.convertFromValueRecordSchema(recordSchema);
    WriteComputeProcessor writeComputeProcessor = new WriteComputeProcessor(new CollectionTimestampMergeRecordHelper());

    Schema nestedRecordSchema = recordSchema.getField("nestedRecord").schema();
    GenericData.Record nestedRecord = new GenericData.Record(nestedRecordSchema);
    nestedRecord.put("intField", 1);

    GenericData.Record writeComputeRecord = new GenericData.Record(recordWriteComputeUnionSchema);
    writeComputeRecord.put("nestedRecord", nestedRecord);

    GenericData.Record result =
        (GenericData.Record) writeComputeProcessor.updateRecord(recordSchema, null, writeComputeRecord);

    Assert.assertNotNull(result);
    Assert.assertEquals(result.get("nestedRecord"), nestedRecord);
  }
}
