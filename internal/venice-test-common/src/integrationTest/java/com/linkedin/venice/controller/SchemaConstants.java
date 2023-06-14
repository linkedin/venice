package com.linkedin.venice.controller;

/**
 * This is a utility class that contains schema Strings that are used in tests.
 */
public class SchemaConstants {
  private SchemaConstants() {
    // Util class
  }

  static final String VALUE_SCHEMA_FOR_WRITE_COMPUTE_V1 = " {" + "  \"namespace\" : \"example.avro\",  "
      + "  \"type\": \"record\",   " + "  \"name\": \"User\",     " + "  \"fields\": [           "
      + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"\"} " + "  ] " + " } ";

  static final String BAD_VALUE_SCHEMA_FOR_WRITE_COMPUTE_V2 =
      " {" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"User\",     "
          + "  \"fields\": [           " + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"\"}, "
          + "       { \"name\": \"age\", \"type\": \"int\"}  " + "  ] " + " } ";

  static final String VALUE_SCHEMA_FOR_WRITE_COMPUTE_V3 =
      " {" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"User\",     "
          + "  \"fields\": [           " + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"\"}, "
          + "       { \"name\": \"age\", \"type\": \"int\", \"default\": 30},  "
          + "       { \"name\": \"gender\", \"type\": \"string\", \"default\":  \"female\"}" + "  ] " + " } ";

  static final String VALUE_SCHEMA_FOR_WRITE_COMPUTE_V4 =
      " {" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"User\",     "
          + "  \"fields\": [           " + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"\"}, "
          + "       { \"name\": \"age\", \"type\": \"int\", \"default\": 30},  "
          + "       { \"name\": \"gender\", \"type\": \"string\", \"default\":  \"female\"}, "
          + "       { \"name\": \"hometown\", \"type\": \"string\"}" + "  ] " + " } ";

  static final String VALUE_SCHEMA_FOR_WRITE_COMPUTE_V5 =
      " {" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"User\",     "
          + "  \"fields\": [           " + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"\"}, "
          + "       { \"name\": \"age\", \"type\": \"int\", \"default\": 30},  "
          + "       { \"name\": \"gender\", \"type\": \"string\", \"default\":  \"female\"}, "
          + "       { \"name\": \"hometown\", \"type\": \"string\"}, "
          + "       { \"name\": \"country\", \"type\": \"string\", \"default\":  \"USA\"}" + "  ] " + " } ";
}
