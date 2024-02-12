package com.linkedin.venice.hadoop.recordreader.avro;

import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V2_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_TO_NAME_RECORD_V1_SCHEMA;

import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.hadoop.input.recordreader.avro.VeniceAvroRecordReader;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.TestWriteUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceAvroRecordReader {
  @Test
  public void testGeneratePartialUpdate() {
    Schema updateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(NAME_RECORD_V2_SCHEMA);
    VeniceAvroRecordReader recordReader = new VeniceAvroRecordReader(
        STRING_TO_NAME_RECORD_V1_SCHEMA,
        "key",
        "value",
        ETLValueSchemaTransformation.NONE,
        updateSchema);

    GenericRecord record = new GenericData.Record(STRING_TO_NAME_RECORD_V1_SCHEMA);
    record.put("key", "123");
    GenericRecord valueRecord = new GenericData.Record(TestWriteUtils.NAME_RECORD_V1_SCHEMA);
    valueRecord.put("firstName", "FN");
    valueRecord.put("lastName", "LN");
    record.put("value", valueRecord);
    Object result = recordReader.getAvroValue(new AvroWrapper<>(record), NullWritable.get());
    Assert.assertTrue(result instanceof IndexedRecord);

    Assert.assertEquals(((IndexedRecord) result).get(updateSchema.getField("firstName").pos()), "FN");
    Assert.assertEquals(((IndexedRecord) result).get(updateSchema.getField("lastName").pos()), "LN");
    Assert.assertEquals(
        ((IndexedRecord) result).get(updateSchema.getField("age").pos()),
        new GenericData.Record(updateSchema.getField("age").schema().getTypes().get(0)));
  }
}
