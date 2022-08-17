package com.linkedin.venice.schema;

public class TestAvroSchemaStrConstants {
  static String recordSchemaStr =
      "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"User\",     "
          + "  \"fields\": [           " + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"},  "
          + "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"id\"},  "
          + "       { \"name\": \"age\", \"type\": \"int\", \"default\": -1 }" + "  ] " + " } ";

  static String recordOfArraySchemaStr =
      "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"testRecord\",\n" + "  \"namespace\" : \"avro.example\",\n"
          + "  \"fields\" : [ {\n" + "    \"name\" : \"intArray\",\n" + "    \"type\" : {\n"
          + "      \"type\" : \"array\",\n" + "      \"items\" : \"int\"\n" + "    },\n" + "    \"default\" : [ ]\n"
          + "  }, {\n" + "    \"name\" : \"floatArray\",\n" + "    \"type\" : {\n" + "      \"type\" : \"array\",\n"
          + "      \"items\" : \"float\"\n" + "    },\n" + "    \"default\" : [ ]\n" + "  } ]\n" + "}";

  static String recordOfUnionWithCollectionStr = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"testRecord\",\n"
      + "  \"namespace\": \"avro.example\",\n" + "  \"fields\": [\n" + "    {\n" + "      \"name\": \"intArray\",\n"
      + "      \"type\":[\n" + "      {\n" + "        \"type\": \"array\",\n" + "        \"items\": \"int\"\n"
      + "      },\n" + "      \"boolean\"\n" + "      ],\n" + "      \"default\": [\n" + "      ]\n" + "    },\n"
      + "    {\n" + "      \"name\": \"floatArray\",\n" + "      \"type\": {\n" + "        \"type\": \"array\",\n"
      + "        \"items\": \"float\"\n" + "      },\n" + "      \"default\": [\n" + "        \n" + "      ]\n"
      + "    }\n" + "  ]\n" + "}";

  static String recordOfUnionWithTwoCollectionsStr = "{\n" + "  \"type\": \"record\",\n"
      + "  \"name\": \"testRecord\",\n" + "  \"namespace\": \"avro.example\",\n" + "  \"fields\": [\n" + "    {\n"
      + "      \"name\": \"intArray\",\n" + "      \"type\":[\n" + "      {\n" + "        \"type\": \"array\",\n"
      + "        \"items\": \"int\"\n" + "      },\n" + "      {\n" + "        \"type\": \"map\",\n"
      + "        \"values\": \"long\"\n" + "      },\n" + "      \"boolean\"\n" + "      ],\n"
      + "      \"default\": [\n" + "      ]\n" + "    },\n" + "    {\n" + "      \"name\": \"floatArray\",\n"
      + "      \"type\": {\n" + "        \"type\": \"array\",\n" + "        \"items\": \"float\"\n" + "      },\n"
      + "      \"default\": [\n" + "        \n" + "      ]\n" + "    }\n" + "  ]\n" + "}";

  public static String recordOfNullableArrayStr = "{\n" + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"testRecord\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"nullableArrayField\",\n"
      + "    \"type\" : [ \"null\", {\n" + "      \"type\" : \"array\",\n" + "      \"items\" : {\n"
      + "        \"type\" : \"record\",\n" + "        \"name\" : \"simpleRecord\",\n" + "        \"fields\" : [ {\n"
      + "          \"name\" : \"intField\",\n" + "          \"type\" : \"int\",\n" + "          \"default\" : 0\n"
      + "        } ]\n" + "      }\n" + "    } ],\n" + "    \"default\" : null\n" + "  } ]\n" + "}";
}
