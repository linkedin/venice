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

  static final String PERSON_SCHEMA_STR_V1 = "{" +
      "  \"name\": \"Person\"," +
      "  \"type\": \"record\"," +
      "  \"fields\": [" +
      "       { \"name\": \"age\", \"type\": \"int\", \"default\": -1},  " +
      "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"default_name\"},  " +
      "       { \"name\": \"intArray\", \"type\": {\"type\": \"array\", \"items\": \"int\"}, \"default\": [] }," +
      "       { \"name\": \"stringMap\", \"type\": {\"type\": \"map\", \"values\": \"string\" }, \"default\": {}}" +
      "   ]"
      + "}";

  // Person V2 schema mismatches with person V1 schema in a way that it has two additional fields "favoritePet" and "stringArray"
  // and it drops the "stringMap" field.
  static final String PERSON_SCHEMA_STR_V2 = "{" +
      "  \"name\": \"Person\"," +
      "  \"type\": \"record\"," +
      "  \"fields\": [" +
      "       { \"name\": \"age\", \"type\": \"int\", \"default\": -1},  " +
      "       { \"name\": \"favoritePet\", \"type\": \"string\", \"default\": \"Pancake!\"},  " +
      "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"default_name\"},  " +
      "       { \"name\": \"intArray\", \"type\": {\"type\": \"array\", \"items\": \"int\"}, \"default\": [] }, " +
      "       { \"name\": \"stringArray\", \"type\": {\"type\": \"array\", \"items\": \"string\"}, \"default\": [] }" +
      "   ]"
      + "}";

  // Person V3 schema should be a superset schema of Person V1 and Person V2 schemas.
  static final String PERSON_SCHEMA_STR_V3 = "{" +
      "  \"name\": \"Person\"," +
      "  \"type\": \"record\"," +
      "  \"fields\": [" +
      "       { \"name\": \"age\", \"type\": \"int\", \"default\": -1},  " +
      "       { \"name\": \"favoritePet\", \"type\": \"string\", \"default\": \"Pancake!\"},  " +
      "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"default_name\"},  " +
      "       { \"name\": \"intArray\", \"type\": {\"type\": \"array\", \"items\": \"int\"}, \"default\": [] }, " +
      "       { \"name\": \"stringArray\", \"type\": {\"type\": \"array\", \"items\": \"string\"}, \"default\": [] }, " +
      "       { \"name\": \"stringMap\", \"type\": {\"type\": \"map\", \"values\": \"string\" }, \"default\": {}}" +
      "   ]"
      + "}";
}
