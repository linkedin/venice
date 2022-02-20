package com.linkedin.venice.schema.rmd.v2;

import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaGenerator;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestRmdSchemaCompatibility {

  // A value schema that contains primitive types and both collection types, namely Array/List and Map.
  private static String VALUE_SCHEMA_STR = "{"
      + "   \"type\" : \"record\","
      + "   \"namespace\" : \"com.linkedin.avro\","
      + "   \"name\" : \"Person\","
      + "   \"fields\" : ["
      + "      { \"name\" : \"Name\" , \"type\" : \"string\", \"default\" : \"unknown\" },"
      + "      { \"name\" : \"Age\" , \"type\" : \"int\", \"default\" : -1 },"
      + "      { \"name\" : \"ANullableFieldName\", \"type\": [\"null\", \"long\"], \"default\" : \"null\"},"
      + "      { \"name\" : \"AUnionFieldName\", \"type\": [\"string\", \"long\"], \"default\" : \"aString\"},"
      + "      { \"name\" : \"Items\" , \"type\" : {\"type\" : \"array\", \"items\" : \"string\"}, \"default\" : [] },"
      + "      { \"name\" : \"PetNameToAge\" , \"type\" : [\"null\" , {\"type\" : \"map\", \"values\" : \"int\"}], \"default\" : null }"
      + "   ]"
      + "}";

  @Test
  public void testRmdV2IsBackwardCompatibleWithV1() {
    Schema rmdV1 = ReplicationMetadataSchemaGenerator.generateMetadataSchema(VALUE_SCHEMA_STR, 1);
    Schema rmdV2 = ReplicationMetadataSchemaGenerator.generateMetadataSchema(VALUE_SCHEMA_STR, 2);
    SchemaCompatibility.SchemaPairCompatibility backwardCompatibility = SchemaCompatibility.checkReaderWriterCompatibility(rmdV2, rmdV1);
    Assert.assertEquals(backwardCompatibility.getType(), SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE);
  }
}
