package com.linkedin.venice.schema.writecompute;

import com.linkedin.venice.schema.TestAvroSchemaStrConstants;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestWriteComputeSchemaValidator {
  static String nestedRecordStr = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"testRecord\",\n"
      + "  \"fields\" : [ {\n" + "    \"name\" : \"recordField\",\n" + "    \"type\" : {\n"
      + "      \"type\" : \"record\",\n" + "      \"name\" : \"nestedRecord\",\n" + "      \"fields\" : [ {\n"
      + "        \"name\" : \"intField\",\n" + "        \"type\" : \"int\"\n" + "      } ]\n" + "    },\n"
      + "    \"default\" : {\n" + "      \"intField\" : 1\n" + "    }\n" + "  } ]\n" + "}";

  private final WriteComputeSchemaConverter writeComputeSchemaConverter = WriteComputeSchemaConverter.getInstance();

  @Test
  public void testCanValidateNullableUnionField() {
    Schema originalSchema = Schema.parse(TestAvroSchemaStrConstants.recordOfNullableArrayStr);
    Schema writeComputeSchema = writeComputeSchemaConverter.convert(originalSchema);

    validate(originalSchema, writeComputeSchema);
  }

  @Test
  public void testCanValidateNestedRecord() {
    Schema originalSchema = Schema.parse(nestedRecordStr);
    Schema writeComputeSchema = writeComputeSchemaConverter.convert(originalSchema);

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
