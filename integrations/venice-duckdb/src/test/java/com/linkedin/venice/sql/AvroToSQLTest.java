package com.linkedin.venice.sql;

import static com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.createSchemaField;
import static com.linkedin.venice.sql.AvroToSQL.UnsupportedTypeHandling.FAIL;
import static com.linkedin.venice.sql.AvroToSQL.UnsupportedTypeHandling.SKIP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class AvroToSQLTest {
  private static final String EXPECTED_CREATE_TABLE_STATEMENT_WITH_ALL_TYPES = "CREATE TABLE MyRecord(" //
      + "key1 INTEGER, " //
      + "fixedField BINARY, " //
      + "stringField VARCHAR, " //
      + "bytesField VARBINARY, "//
      + "intField INTEGER, " //
      + "longField BIGINT, " //
      + "floatField FLOAT, " //
      + "doubleField DOUBLE, " //
      + "booleanField BOOLEAN, " //
      // + "nullField NULL, " //
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

  private static final String EXPECTED_UPSERT_STATEMENT_WITH_ALL_TYPES =
      "INSERT OR REPLACE INTO MyRecord VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

  @Test
  public void testValidCreateTable() {
    List<Schema.Field> allFields = getAllValidFields();
    Schema schemaWithAllSupportedFieldTypes = Schema.createRecord("MyRecord", "", "", false, allFields);

    // Single-column key (but without a PRIMARY KEY constraint)
    Schema singleColumnKey = Schema.createRecord("MyKey", "", "", false, Collections.singletonList(getKey1()));

    TableDefinition tableDefinitionWithoutPrimaryKey = AvroToSQL.getTableDefinition(
        "MyRecord",
        singleColumnKey,
        schemaWithAllSupportedFieldTypes,
        Collections.emptySet(),
        FAIL,
        false);
    String createTableWithoutPrimaryKey = SQLUtils.createTableStatement(tableDefinitionWithoutPrimaryKey);
    assertEquals(createTableWithoutPrimaryKey, EXPECTED_CREATE_TABLE_STATEMENT_WITH_ALL_TYPES);

    // Single-column primary key
    TableDefinition tableDefinitionWithPrimaryKey = AvroToSQL.getTableDefinition(
        "MyRecord",
        singleColumnKey,
        schemaWithAllSupportedFieldTypes,
        Collections.emptySet(),
        FAIL,
        true);
    String createTableWithPrimaryKey = SQLUtils.createTableStatement(tableDefinitionWithPrimaryKey);
    String expectedCreateTable = EXPECTED_CREATE_TABLE_STATEMENT_WITH_ALL_TYPES.replace(");", ", PRIMARY KEY(key1));");
    assertEquals(createTableWithPrimaryKey, expectedCreateTable);

    // Composite primary key
    List<Schema.Field> compositeKeyFields = new ArrayList<>();
    compositeKeyFields.add(getKey1());
    compositeKeyFields.add(createSchemaField("key2", Schema.create(Schema.Type.LONG), "", null));
    Schema compositeKey = Schema.createRecord("MyKey", "", "", false, compositeKeyFields);

    TableDefinition tableDefinitionWithCompositePrimaryKey = AvroToSQL.getTableDefinition(
        "MyRecord",
        compositeKey,
        schemaWithAllSupportedFieldTypes,
        Collections.emptySet(),
        FAIL,
        true);
    String createTableWithCompositePrimaryKey = SQLUtils.createTableStatement(tableDefinitionWithCompositePrimaryKey);
    String expectedCreateTableWithCompositePK =
        EXPECTED_CREATE_TABLE_STATEMENT_WITH_ALL_TYPES.replace("key1 INTEGER", "key1 INTEGER, key2 BIGINT")
            .replace(");", ", PRIMARY KEY(key1, key2));");
    assertEquals(createTableWithCompositePrimaryKey, expectedCreateTableWithCompositePK);
  }

  @Test
  public void testUnsupportedTypesHandling() {
    // Types that will for sure not be supported.

    assertThrows(
        IllegalArgumentException.class,
        () -> SQLUtils.createTableStatement(
            AvroToSQL.getTableDefinition(
                "MyRecord",
                Schema.create(Schema.Type.INT),
                Schema.create(Schema.Type.INT),
                Collections.emptySet(),
                FAIL,
                true)));

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

  @Test
  public void testUpsertStatement() {
    Schema singleColumnKey = Schema.createRecord("MyKey", "", "", false, Collections.singletonList(getKey1()));

    List<Schema.Field> allFields = getAllValidFields();
    Schema schemaWithAllSupportedFieldTypes = Schema.createRecord("MyRecord", "", "", false, allFields);

    String upsertStatementForAllFields = AvroToSQL
        .upsertStatement("MyRecord", singleColumnKey, schemaWithAllSupportedFieldTypes, Collections.emptySet());
    assertEquals(upsertStatementForAllFields, EXPECTED_UPSERT_STATEMENT_WITH_ALL_TYPES);
  }

  private Schema.Field getKey1() {
    return createSchemaField("key1", Schema.create(Schema.Type.INT), "", null);
  }

  public static List<Schema.Field> getAllValidFields() {
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
    // allFields.add(createSchemaField("nullField", Schema.create(Schema.Type.NULL), "", null));

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

    Schema.Field keyField1 = createSchemaField("key1", Schema.create(Schema.Type.INT), "", null);
    Schema singleColumnKey = Schema.createRecord("MyKey", "", "", false, Collections.singletonList(keyField1));
    Schema valueSchema = Schema.createRecord("MyRecord", "", "", false, allFields);

    assertThrows(
        IllegalArgumentException.class,
        () -> SQLUtils.createTableStatement(
            AvroToSQL
                .getTableDefinition("MyRecord", singleColumnKey, valueSchema, Collections.emptySet(), FAIL, true)));

    String createTableStatement = SQLUtils.createTableStatement(
        AvroToSQL.getTableDefinition("MyRecord", singleColumnKey, valueSchema, Collections.emptySet(), SKIP, false));
    assertEquals(createTableStatement, EXPECTED_CREATE_TABLE_STATEMENT_WITH_ALL_TYPES);

    String upsertStatement =
        AvroToSQL.upsertStatement("MyRecord", singleColumnKey, valueSchema, Collections.emptySet());
    assertEquals(upsertStatement, EXPECTED_UPSERT_STATEMENT_WITH_ALL_TYPES);
  }
}
