package com.linkedin.venice.duckdb;

import static com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.createSchemaField;
import static com.linkedin.venice.sql.AvroToSQL.UnsupportedTypeHandling.SKIP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.sql.AvroToSQL;
import com.linkedin.venice.sql.AvroToSQLTest;
import com.linkedin.venice.sql.PreparedStatementProcessor;
import com.linkedin.venice.sql.SQLUtils;
import com.linkedin.venice.utils.ByteUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.duckdb.DuckDBResultSet;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DuckDBAvroToSQLTest {
  @DataProvider
  public Object[][] primaryKeyColumns() {
    Set<String> compositePK = new HashSet<>();
    compositePK.add("intField");
    compositePK.add("longField");
    return new Object[][] { { Collections.singleton("intField") }, { compositePK } };
  }

  @DataProvider
  public Object[][] primaryKeySchemas() {
    Schema singleColumnKey = Schema.createRecord(
        "MyKey",
        "",
        "",
        false,
        Collections.singletonList(createSchemaField("key1", Schema.create(Schema.Type.INT), "", null)));
    List<Schema.Field> compositeKeyFields = new ArrayList<>();
    compositeKeyFields.add(createSchemaField("key1", Schema.create(Schema.Type.INT), "", null));
    compositeKeyFields.add(createSchemaField("key2", Schema.create(Schema.Type.LONG), "", null));
    Schema compositeKey = Schema.createRecord("MyKey", "", "", false, compositeKeyFields);
    return new Object[][] { { singleColumnKey }, { compositeKey } };
  }

  @Test(dataProvider = "primaryKeySchemas")
  public void testUpsert(Schema keySchema) throws SQLException, IOException {
    List<Schema.Field> fields = AvroToSQLTest.getAllValidFields();
    Schema valueSchema = Schema.createRecord("MyRecord", "", "", false, fields);
    try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = connection.createStatement()) {
      // create a table
      String tableName = "MyRecord_v1";
      String createTableStatement = SQLUtils.createTableStatement(
          AvroToSQL.getTableDefinition(tableName, keySchema, valueSchema, Collections.emptySet(), SKIP, true));
      System.out.println(createTableStatement);
      stmt.execute(createTableStatement);

      String upsertStatement = AvroToSQL.upsertStatement(tableName, keySchema, valueSchema, Collections.emptySet());
      System.out.println(upsertStatement);
      PreparedStatementProcessor upsertProcessor =
          AvroToSQL.upsertProcessor(keySchema, valueSchema, Collections.emptySet());
      for (int rewriteIteration = 0; rewriteIteration < 3; rewriteIteration++) {
        List<Map.Entry<GenericRecord, GenericRecord>> records = generateRecords(keySchema, valueSchema);
        GenericRecord key, value;
        try (PreparedStatement preparedStatement = connection.prepareStatement(upsertStatement)) {
          for (int i = 0; i < records.size(); i++) {
            key = records.get(i).getKey();
            value = records.get(i).getValue();
            upsertProcessor.process(key, value, preparedStatement);
          }
        }

        int recordCount = records.size();
        try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
          for (int j = 0; j < recordCount; j++) {
            assertTrue(
                rs.next(),
                "Rewrite iteration " + rewriteIteration + ". Expected a row at index " + j
                    + " after having inserted up to row " + recordCount);

            key = records.get(j).getKey();
            value = records.get(j).getValue();
            assertRecordValidity(keySchema, key, rs, j);
            assertRecordValidity(valueSchema, value, rs, j);
            System.out.println("Rewrite iteration " + rewriteIteration + ". Successfully validated row " + j);
          }
          assertFalse(rs.next(), "Expected no more rows at index " + recordCount);
        }
        System.out
            .println("Rewrite iteration " + rewriteIteration + ". Successfully validated up to i = " + recordCount);
      }
    }
  }

  private void assertRecordValidity(Schema schema, GenericRecord record, ResultSet rs, int j)
      throws IOException, SQLException {
    for (Schema.Field field: schema.getFields()) {
      Object result = rs.getObject(field.name());
      if (result instanceof DuckDBResultSet.DuckDBBlobResult) {
        DuckDBResultSet.DuckDBBlobResult duckDBBlobResult = (DuckDBResultSet.DuckDBBlobResult) result;
        byte[] actual = IOUtils.toByteArray(duckDBBlobResult.getBinaryStream());
        byte[] expected = ((ByteBuffer) record.get(field.name())).array();
        assertEquals(
            actual,
            expected,
            ". Bytes not equals at row " + j + "! actual: " + ByteUtils.toHexString(actual) + ", wanted: "
                + ByteUtils.toHexString(expected));
        // System.out.println("Rewrite iteration " + rewriteIteration + ". Row: " + j + ", field: " +
        // field.name() + ", value: " + ByteUtils.toHexString(actual));
      } else {
        assertEquals(
            result,
            record.get(field.name()),
            ". Field '" + field.name() + "' is not correct at row " + j + "!");
        // System.out.println("Rewrite iteration " + rewriteIteration + ". Row: " + j + ", field: " +
        // field.name() + ", value: " + result);
      }
    }

  }

  private List<Map.Entry<GenericRecord, GenericRecord>> generateRecords(Schema keySchema, Schema valueSchema) {
    List<Map.Entry<GenericRecord, GenericRecord>> records = new ArrayList<>();

    GenericRecord keyRecord, valueRecord;
    Object fieldValue;
    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      keyRecord = new GenericData.Record(keySchema);
      for (Schema.Field field: keySchema.getFields()) {
        switch (field.schema().getType()) {
          case INT:
            fieldValue = i;
            break;
          case LONG:
            fieldValue = (long) i;
            break;
          default:
            throw new IllegalArgumentException("Only numeric PK columns are supported in this test.");
        }
        keyRecord.put(field.pos(), fieldValue);
      }

      valueRecord = new GenericData.Record(valueSchema);
      for (Schema.Field field: valueSchema.getFields()) {
        Schema fieldSchema = field.schema();
        if (fieldSchema.getType() == Schema.Type.UNION) {
          Schema first = field.schema().getTypes().get(0);
          Schema second = field.schema().getTypes().get(1);
          if (first.getType() == Schema.Type.NULL) {
            fieldSchema = second;
          } else if (second.getType() == Schema.Type.NULL) {
            fieldSchema = first;
          } else {
            throw new IllegalArgumentException("Unsupported union: " + field.schema());
          }
        }
        fieldValue = randomValue(fieldSchema, random);
        valueRecord.put(field.pos(), fieldValue);
      }
      records.add(new AbstractMap.SimpleEntry<>(keyRecord, valueRecord));
    }

    return records;
  }

  private Object randomValue(Schema schema, Random random) {
    switch (schema.getType()) {
      case STRING:
        return String.valueOf(random.nextLong());
      case INT:
        return random.nextInt();
      case LONG:
        return random.nextLong();
      case FLOAT:
        return random.nextFloat();
      case DOUBLE:
        return random.nextDouble();
      case BOOLEAN:
        return random.nextBoolean();
      case BYTES:
        return getBB(10, random);
      case FIXED:
        return getBB(schema.getFixedSize(), random);
      case NULL:
        return null;
      default:
        throw new IllegalArgumentException("Unsupported type: " + schema.getType());
    }
  }

  private ByteBuffer getBB(int size, Random random) {
    byte[] bytes = new byte[size];
    random.nextBytes(bytes);
    return ByteBuffer.wrap(bytes);
  }
}
