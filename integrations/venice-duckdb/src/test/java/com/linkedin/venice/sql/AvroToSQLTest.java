package com.linkedin.venice.sql;

import static com.linkedin.venice.sql.AvroToSQL.UnsupportedTypeHandling.FAIL;
import static com.linkedin.venice.sql.AvroToSQL.UnsupportedTypeHandling.SKIP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class AvroToSQLTest {
  private static final String EXPECTED_CREATE_TABLE_STATEMENT_WITH_ALL_TYPES = "CREATE TABLE MyRecord(" //
      + "fixedField BINARY, " //
      + "stringField VARCHAR, " //
      + "bytesField VARBINARY, "//
      + "intField INTEGER, " //
      + "longField BIGINT, " //
      + "floatField FLOAT, " //
      + "doubleField DOUBLE, " //
      + "booleanField BOOLEAN, " //
      + "nullField NULL, " //
      + "fixedFieldUnion1 BINARY, " //
      + "fixedFieldUnion2 BINARY, " //
      + "stringFieldUnion1 VARCHAR, " //
      + "stringFieldUnion2 VARCHAR, " //
      + "bytesFieldUnion1 VARBINARY, " //
      + "bytesFieldUnion2 VARBINARY, " //
      + "intFieldUnion1 INTEGER, " //
      + "intFieldUnion2 INTEGER, " //
      + "longFieldUnion1 BIGINT, " //
      + "longFieldUnion2 BIGINT, " //
      + "floatFieldUnion1 FLOAT, " //
      + "floatFieldUnion2 FLOAT, " //
      + "doubleFieldUnion1 DOUBLE, " //
      + "doubleFieldUnion2 DOUBLE, " //
      + "booleanFieldUnion1 BOOLEAN, " //
      + "booleanFieldUnion2 BOOLEAN);";

  @Test
  public void testValidCreateTable() {
    List<Schema.Field> allFields = getAllValidFields();
    Schema schemaWithAllSupportedFieldTypes = Schema.createRecord("MyRecord", "", "", false, allFields);

    String createTableStatementForAllFields =
        AvroToSQL.createTableStatement(schemaWithAllSupportedFieldTypes, Collections.emptySet(), FAIL);
    assertEquals(createTableStatementForAllFields, EXPECTED_CREATE_TABLE_STATEMENT_WITH_ALL_TYPES);

    // Primary keys
    Set<String> primaryKeys = new HashSet<>();
    primaryKeys.add("stringField");
    String createTableWithPrimaryKey =
        AvroToSQL.createTableStatement(schemaWithAllSupportedFieldTypes, primaryKeys, FAIL);
    String expectedCreateTable = EXPECTED_CREATE_TABLE_STATEMENT_WITH_ALL_TYPES
        .replace("stringField VARCHAR", "stringField VARCHAR PRIMARY KEY");
    assertEquals(createTableWithPrimaryKey, expectedCreateTable);
  }

  @Test
  public void testInvalidCreateTable() {
    // Types that will for sure not be supported.

    assertThrows(
        IllegalArgumentException.class,
        () -> AvroToSQL.createTableStatement(Schema.create(Schema.Type.INT), Collections.emptySet(), FAIL));

    testSchemaWithInvalidType(
        new Schema.Field(
            "TripleUnionWithNull",
            Schema.createUnion(
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.INT),
                Schema.create(Schema.Type.STRING))));

    testSchemaWithInvalidType(
        new Schema.Field(
            "TripleUnionWithoutNull",
            Schema.createUnion(
                Schema.create(Schema.Type.BOOLEAN),
                Schema.create(Schema.Type.INT),
                Schema.create(Schema.Type.STRING))));

    testSchemaWithInvalidType(
        new Schema.Field(
            "DoubleUnionWithoutNull",
            Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING))));

    // Types that could eventually become supported...

    testSchemaWithInvalidType(new Schema.Field("StringArray", Schema.createArray(Schema.create(Schema.Type.STRING))));

    testSchemaWithInvalidType(new Schema.Field("StringStringMap", Schema.createMap(Schema.create(Schema.Type.STRING))));

    testSchemaWithInvalidType(
        new Schema.Field("Record", Schema.createRecord("NestedRecord", "", "", false, Collections.emptyList())));
  }

  private List<Schema.Field> getAllValidFields() {
    List<Schema.Field> allFields = new ArrayList<>();

    // Basic types
    allFields.add(new Schema.Field("fixedField", Schema.createFixed("MyFixed", "", "", 1)));
    allFields.add(new Schema.Field("stringField", Schema.create(Schema.Type.STRING)));
    allFields.add(new Schema.Field("bytesField", Schema.create(Schema.Type.BYTES)));
    allFields.add(new Schema.Field("intField", Schema.create(Schema.Type.INT)));
    allFields.add(new Schema.Field("longField", Schema.create(Schema.Type.LONG)));
    allFields.add(new Schema.Field("floatField", Schema.create(Schema.Type.FLOAT)));
    allFields.add(new Schema.Field("doubleField", Schema.create(Schema.Type.DOUBLE)));
    allFields.add(new Schema.Field("booleanField", Schema.create(Schema.Type.BOOLEAN)));
    allFields.add(new Schema.Field("nullField", Schema.create(Schema.Type.NULL)));

    // Unions with null
    List<Schema.Field> allOptionalFields = new ArrayList<>();
    for (Schema.Field field: allFields) {
      if (field.schema().getType() == Schema.Type.NULL) {
        // Madness? THIS -- IS -- SPARTAAAAAAAAAAAAAAAAAA!!!!!!!!!
        continue;
      }

      // Include both union branch orders
      allOptionalFields.add(
          new Schema.Field(
              field.name() + "Union1",
              Schema.createUnion(Schema.create(Schema.Type.NULL), field.schema())));
      allOptionalFields.add(
          new Schema.Field(
              field.name() + "Union2",
              Schema.createUnion(field.schema(), Schema.create(Schema.Type.NULL))));
    }
    allFields.addAll(allOptionalFields);

    return allFields;
  }

  private void testSchemaWithInvalidType(Schema.Field invalidField) {
    List<Schema.Field> allFields = getAllValidFields();
    allFields.add(invalidField);

    Schema schema = Schema.createRecord("MyRecord", "", "", false, allFields);

    assertThrows(
        IllegalArgumentException.class,
        () -> AvroToSQL.createTableStatement(schema, Collections.emptySet(), FAIL));

    String createTableStatement = AvroToSQL.createTableStatement(schema, Collections.emptySet(), SKIP);
    assertEquals(createTableStatement, EXPECTED_CREATE_TABLE_STATEMENT_WITH_ALL_TYPES);
  }
}
