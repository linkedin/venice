package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class FastClientQueryToolTest {
  @Test
  public void testConvertKeyString() {
    Schema schema = Schema.create(Schema.Type.STRING);
    Object key = FastClientQueryTool.convertKey("hello", schema);
    assertEquals(key, "hello");
  }

  @Test
  public void testConvertKeyInt() {
    Schema schema = Schema.create(Schema.Type.INT);
    Object key = FastClientQueryTool.convertKey("42", schema);
    assertEquals(key, 42);
  }

  @Test
  public void testConvertKeyLong() {
    Schema schema = Schema.create(Schema.Type.LONG);
    Object key = FastClientQueryTool.convertKey("123456789", schema);
    assertEquals(key, 123456789L);
  }

  @Test
  public void testConvertKeyFloat() {
    Schema schema = Schema.create(Schema.Type.FLOAT);
    Object key = FastClientQueryTool.convertKey("1.5", schema);
    assertEquals(key, 1.5f);
  }

  @Test
  public void testConvertKeyDouble() {
    Schema schema = Schema.create(Schema.Type.DOUBLE);
    Object key = FastClientQueryTool.convertKey("1.5", schema);
    assertEquals(key, 1.5);
  }

  @Test
  public void testConvertKeyBoolean() {
    Schema schema = Schema.create(Schema.Type.BOOLEAN);
    Object key = FastClientQueryTool.convertKey("true", schema);
    assertEquals(key, true);

    key = FastClientQueryTool.convertKey("false", schema);
    assertEquals(key, false);
  }

  @Test(expectedExceptions = NumberFormatException.class)
  public void testConvertKeyIntWithInvalidInput() {
    Schema schema = Schema.create(Schema.Type.INT);
    FastClientQueryTool.convertKey("not_a_number", schema);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testConvertKeyComplexSchemaWithInvalidJson() {
    Schema schema = Schema.createRecord("TestRecord", null, "test", false);
    schema.setFields(
        java.util.Collections.singletonList(
            AvroCompatibilityHelper.createSchemaField("field1", Schema.create(Schema.Type.STRING), null, null)));
    FastClientQueryTool.convertKey("not_valid_json", schema);
  }

  @Test
  public void testConvertKeyComplexSchemaWithValidJson() {
    Schema schema = Schema.createRecord("TestRecord", null, "test", false);
    schema.setFields(
        java.util.Collections.singletonList(
            AvroCompatibilityHelper.createSchemaField("field1", Schema.create(Schema.Type.STRING), null, null)));
    Object key = FastClientQueryTool.convertKey("{\"field1\": \"value1\"}", schema);
    assertTrue(key.toString().contains("value1"));
  }
}
