package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJob.GENERATE_PARTIAL_UPDATE_RECORD_FROM_INPUT;
import static com.linkedin.venice.hadoop.VenicePushJob.KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.SCHEMA_STRING_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.TOPIC_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.UPDATE_SCHEMA_STRING_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.VALUE_FIELD_PROP;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V2_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_TO_NAME_RECORD_V1_SCHEMA;

import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
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
    Properties properties = new Properties();
    properties.put(TOPIC_PROP, "test_store_rt");
    properties.put(SCHEMA_STRING_PROP, STRING_TO_NAME_RECORD_V1_SCHEMA.toString());
    properties.put(GENERATE_PARTIAL_UPDATE_RECORD_FROM_INPUT, true);
    properties.put(UPDATE_SCHEMA_STRING_PROP, updateSchema);
    properties.put(KEY_FIELD_PROP, "key");
    properties.put(VALUE_FIELD_PROP, "value");
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    VeniceAvroRecordReader recordReader = new VeniceAvroRecordReader(veniceProperties);

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
