package com.linkedin.venice.utils;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroRecordUtilsTest {
  @Test
  public void testClearRecord() {
    GenericRecord record = new GenericData.Record(
        SchemaBuilder.record("SampleSchema").fields().requiredInt("field1").optionalString("field2").endRecord());
    record.put("field1", 42);
    record.put("field2", "value");
    AvroRecordUtils.clearRecord(record);
    Assert.assertNull(record.get("field1"));
    Assert.assertNull(record.get("field2"));
  }
}
