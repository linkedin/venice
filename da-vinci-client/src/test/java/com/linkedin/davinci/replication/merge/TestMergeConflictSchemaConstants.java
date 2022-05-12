package com.linkedin.davinci.replication.merge;

class TestMergeConflictSchemaConstants {

  private TestMergeConflictSchemaConstants() {
    // Private utility class
  }

  static final String VALUE_RECORD_SCHEMA_STR_V1 = "{\n"
      + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"User\",\n"
      + "  \"namespace\" : \"example.avro\",\n"
      + "  \"fields\" : [ {\n"
      + "    \"name\" : \"id\",\n"
      + "    \"type\" : \"string\",\n"
      + "    \"default\" : \"default_id\"\n"
      + "  }, {\n"
      + "    \"name\" : \"name\",\n"
      + "    \"type\" : \"string\",\n"
      + "    \"default\" : \"default_name\"\n"
      + "  }, {\n"
      + "    \"name\" : \"age\",\n"
      + "    \"type\" : \"int\",\n"
      + "    \"default\" : -1\n"
      + "  } ]\n"
      + "}";

  static final String VALUE_RECORD_SCHEMA_STR_V2 = "{\n"
      + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"User\",\n"
      + "  \"namespace\" : \"example.avro\",\n"
      + "  \"fields\" : [ {\n"
      + "    \"name\" : \"id\",\n"
      + "    \"type\" : \"string\",\n"
      + "    \"default\" : \"id\"\n"
      + "  }, {\n"
      + "    \"name\" : \"name\",\n"
      + "    \"type\" : \"string\",\n"
      + "    \"default\" : \"name\"\n"
      + "  }, {\n"
      + "    \"name\" : \"age\",\n"
      + "    \"type\" : \"int\",\n"
      + "    \"default\" : -1\n"
      + "  }, {\n"
      + "    \"name\" : \"age_2\",\n" // New field compared to V1
      + "    \"type\" : \"int\",\n"
      + "    \"default\" : -1\n"
      + "  }]\n"
      + "}";


  static final String USER_SCHEMA_STR_V3 = "{" +
      "  \"namespace\" : \"example.avro\",  " +
      "  \"type\": \"record\",   " +
      "  \"name\": \"User\",     " +
      "  \"fields\": [           " +
      "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"}, " +
      "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"venice\"}" +
      "  ] " +
      " } ";

  static final String USER_SCHEMA_STR_V4 = "{" +
      "  \"namespace\" : \"example.avro\",  " +
      "  \"type\": \"record\",   " +
      "  \"name\": \"User\",     " +
      "  \"fields\": [           " +
      "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"venice\"},  " +
      "       { \"name\": \"weight\", \"type\": \"float\", \"default\": 0.0} " +
      "  ] " +
      " } ";

  // A superset schema of schema V3 and V4.
  static final String USER_SCHEMA_STR_V5 = "{" +
      "  \"namespace\" : \"example.avro\",  " +
      "  \"type\": \"record\",   " +
      "  \"name\": \"User\",     " +
      "  \"fields\": [           " +
      "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"},  " +
      "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"venice\"},  " +
      "       { \"name\": \"weight\", \"type\": \"float\", \"default\": 0.0}" +
      "  ] " +
      " } ";
}
