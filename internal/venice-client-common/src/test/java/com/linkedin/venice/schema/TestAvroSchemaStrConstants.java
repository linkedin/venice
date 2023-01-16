package com.linkedin.venice.schema;

public class TestAvroSchemaStrConstants {
  public final static String recordOfNullableArrayStr = "{\n" + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"testRecord\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"nullableArrayField\",\n"
      + "    \"type\" : [ \"null\", {\n" + "      \"type\" : \"array\",\n" + "      \"items\" : {\n"
      + "        \"type\" : \"record\",\n" + "        \"name\" : \"simpleRecord\",\n" + "        \"fields\" : [ {\n"
      + "          \"name\" : \"intField\",\n" + "          \"type\" : \"int\",\n" + "          \"default\" : 0\n"
      + "        } ]\n" + "      }\n" + "    } ],\n" + "    \"default\" : null\n" + "  } ]\n" + "}";
}
