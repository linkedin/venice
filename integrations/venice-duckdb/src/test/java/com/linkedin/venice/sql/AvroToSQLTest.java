package com.linkedin.venice.sql;

import static com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.createSchemaField;
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
        AvroToSQL.createTableStatement("MyRecord", schemaWithAllSupportedFieldTypes, Collections.emptySet(), FAIL);
    assertEquals(createTableStatementForAllFields, EXPECTED_CREATE_TABLE_STATEMENT_WITH_ALL_TYPES);

    // Primary keys
    Set<String> primaryKeys = new HashSet<>();
    primaryKeys.add("stringField");
    String createTableWithPrimaryKey =
        AvroToSQL.createTableStatement("MyRecord", schemaWithAllSupportedFieldTypes, primaryKeys, FAIL);
    String expectedCreateTable = EXPECTED_CREATE_TABLE_STATEMENT_WITH_ALL_TYPES
        .replace("stringField VARCHAR", "stringField VARCHAR PRIMARY KEY");
    assertEquals(createTableWithPrimaryKey, expectedCreateTable);
  }

  @Test
  public void testInvalidCreateTable() {
    // Types that will for sure not be supported.

    assertThrows(
        IllegalArgumentException.class,
        () -> AvroToSQL.createTableStatement("MyRecord", Schema.create(Schema.Type.INT), Collections.emptySet(), FAIL));

    testSchemaWithInvalidType(
        createSchemaField(
            "TripleUnionWithNull",
            Schema.createUnion(
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.INT),
                Schema.create(Schema.Type.STRING)),
            "",
            null));

    testSchemaWithInvalidType(
        createSchemaField(
            "TripleUnionWithoutNull",
            Schema.createUnion(
                Schema.create(Schema.Type.BOOLEAN),
                Schema.create(Schema.Type.INT),
                Schema.create(Schema.Type.STRING)),
            "",
            null));

    testSchemaWithInvalidType(
        createSchemaField(
            "DoubleUnionWithoutNull",
            Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING)),
            "",
            null));

    // Types that could eventually become supported...

    testSchemaWithInvalidType(
        createSchemaField("StringArray", Schema.createArray(Schema.create(Schema.Type.STRING)), "", null));

    testSchemaWithInvalidType(
        createSchemaField("StringStringMap", Schema.createMap(Schema.create(Schema.Type.STRING)), "", null));

    testSchemaWithInvalidType(
        createSchemaField(
            "Record",
            Schema.createRecord("NestedRecord", "", "", false, Collections.emptyList()),
            "",
            null));
  }

  private List<Schema.Field> getAllValidFields() {
    List<Schema.Field> allFields = new ArrayList<>();

    // Basic types
    allFields.add(createSchemaField("fixedField", Schema.createFixed("MyFixed", "", "", 1), "", null));
    allFields.add(createSchemaField("stringField", Schema.create(Schema.Type.STRING), "", null));
    allFields.add(createSchemaField("bytesField", Schema.create(Schema.Type.BYTES), "", null));
    allFields.add(createSchemaField("intField", Schema.create(Schema.Type.INT), "", null));
    allFields.add(createSchemaField("longField", Schema.create(Schema.Type.LONG), "", null));
    allFields.add(createSchemaField("floatField", Schema.create(Schema.Type.FLOAT), "", null));
    allFields.add(createSchemaField("doubleField", Schema.create(Schema.Type.DOUBLE), "", null));
    allFields.add(createSchemaField("booleanField", Schema.create(Schema.Type.BOOLEAN), "", null));
    allFields.add(createSchemaField("nullField", Schema.create(Schema.Type.NULL), "", null));

    // Unions with null
    List<Schema.Field> allOptionalFields = new ArrayList<>();
    for (Schema.Field field: allFields) {
      if (field.schema().getType() == Schema.Type.NULL) {
        // Madness? THIS -- IS -- SPARTAAAAAAAAAAAAAAAAAA!!!!!!!!!
        continue;
      }

      // Include both union branch orders
      allOptionalFields.add(
          createSchemaField(
              field.name() + "Union1",
              Schema.createUnion(Schema.create(Schema.Type.NULL), field.schema()),
              "",
              null));
      allOptionalFields.add(
          createSchemaField(
              field.name() + "Union2",
              Schema.createUnion(field.schema(), Schema.create(Schema.Type.NULL)),
              "",
              null));
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
        () -> AvroToSQL.createTableStatement("MyRecord", schema, Collections.emptySet(), FAIL));

    String createTableStatement = AvroToSQL.createTableStatement("MyRecord", schema, Collections.emptySet(), SKIP);
    assertEquals(createTableStatement, EXPECTED_CREATE_TABLE_STATEMENT_WITH_ALL_TYPES);
  }
}
