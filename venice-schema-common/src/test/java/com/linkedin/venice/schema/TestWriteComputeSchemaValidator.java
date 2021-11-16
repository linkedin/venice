package com.linkedin.venice.schema;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.schema.TestWriteComputeSchemaAdapter.recordOfNullableArrayStr;

public class TestWriteComputeSchemaValidator {
  static String nestedRecordStr = "{\n" +
      "  \"type\" : \"record\",\n" +
      "  \"name\" : \"testRecord\",\n" +
      "  \"fields\" : [ {\n" +
      "    \"name\" : \"recordField\",\n" +
      "    \"type\" : {\n" +
      "      \"type\" : \"record\",\n" +
      "      \"name\" : \"nestedRecord\",\n" +
      "      \"fields\" : [ {\n" +
      "        \"name\" : \"intField\",\n" +
      "        \"type\" : \"int\"\n" +
      "      } ]\n" + "    },\n" +
      "    \"default\" : {\n" +
      "      \"intField\" : 1\n" +
      "    }\n" +
      "  } ]\n" +
      "}";

  @Test
  public void testCanValidateNullableUnionField() {
    Schema originalSchema = Schema.parse(recordOfNullableArrayStr);
    Schema writeComputeSchema = WriteComputeSchemaConverter.convert(originalSchema);

    validate(originalSchema, writeComputeSchema);
  }

  @Test
  public void testCanValidateNestedRecord() {
    Schema originalSchema = Schema.parse(nestedRecordStr);
    Schema writeComputeSchema = WriteComputeSchemaConverter.convert(originalSchema);

    validate(originalSchema, writeComputeSchema);
  }

  private void validate(Schema originalSchema, Schema writeComputeSchema) {
    try {
      WriteComputeSchemaValidator.validate(originalSchema, writeComputeSchema);
    } catch (WriteComputeSchemaValidator.InvalidWriteComputeException e) {
      Assert.fail();
    }
  }
}
