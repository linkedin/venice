package com.linkedin.venice.sql;

import static com.linkedin.venice.sql.AvroToSQL.UnsupportedTypeHandling.FAIL;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;


public class DuckDBDaVinciRecordTransformer extends DaVinciRecordTransformer<String, GenericRecord, GenericRecord> {
  private static final String duckDBFilePath = "my_database.duckdb";
  private static final String baseVersionTableName = "my_table_v";
  private final String duckDBUrl;
  private final String versionTableName;

  // Can only support on primary key atm
  private static final String primaryKey = "firstName";
  private final Set<String> primaryKeys = new HashSet<>();

  public DuckDBDaVinciRecordTransformer(int storeVersion, boolean storeRecordsInDaVinci, String baseDir) {
    super(storeVersion, storeRecordsInDaVinci);
    versionTableName = baseVersionTableName + storeVersion;
    duckDBUrl = "jdbc:duckdb:" + baseDir + "/" + duckDBFilePath;
    primaryKeys.add(primaryKey);
  }

  @Override
  public Schema getKeySchema() {
    return Schema.create(Schema.Type.STRING);
  }

  // ToDo: Put real schema
  @Override
  public Schema getOutputValueSchema() {
    Schema valueSchema = SchemaBuilder.record("nameRecord")
        .namespace("example.avro")
        .fields()
        .name("firstName")
        .type()
        .stringType()
        .stringDefault("")
        .name("lastName")
        .type()
        .stringType()
        .stringDefault("")
        .endRecord();
    return valueSchema;
  }

  @Override
  public DaVinciRecordTransformerResult<GenericRecord> transform(Lazy<String> key, Lazy<GenericRecord> value) {
    // Record transformation happens inside processPut as we need access to the connection object to create the prepared
    // statement
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.UNCHANGED);
  }

  @Override
  public void processPut(Lazy<String> key, Lazy<GenericRecord> value) {
    Schema valueSchema = value.get().getSchema();
    String upsertStatement = AvroToSQL.upsertStatement(versionTableName, valueSchema);

    try (Connection connection = DriverManager.getConnection(duckDBUrl)) {
      BiConsumer<GenericRecord, PreparedStatement> upsertProcessor = AvroToSQL.upsertProcessor(valueSchema);

      try (PreparedStatement preparedStatement = connection.prepareStatement(upsertStatement)) {
        upsertProcessor.accept(value.get(), preparedStatement);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void processDelete(Lazy<String> key) {
    try (Connection connection = DriverManager.getConnection(duckDBUrl);
        Statement stmt = connection.createStatement()) {
      String sqlDelete = String.format("DELETE FROM %s WHERE %s = '%s';", versionTableName, primaryKey, key.get());
      stmt.execute(sqlDelete);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onStartVersionIngestion(boolean isCurrentVersion) {
    try (Connection connection = DriverManager.getConnection(duckDBUrl);
        Statement stmt = connection.createStatement()) {
      String createTableStatement =
          AvroToSQL.createTableStatement(versionTableName, getOutputValueSchema(), primaryKeys, FAIL);
      stmt.execute(createTableStatement);

      if (isCurrentVersion) {
        String createViewStatement =
            String.format("CREATE OR REPLACE VIEW current_version AS SELECT * FROM %s;", versionTableName);
        stmt.execute(createViewStatement);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onEndVersionIngestion(int currentVersion) {
    try (Connection connection = DriverManager.getConnection(duckDBUrl);
        Statement stmt = connection.createStatement()) {
      // Swap to current version
      String currentVersionTableName = baseVersionTableName + currentVersion;
      String createViewStatement =
          String.format("CREATE OR REPLACE VIEW current_version AS SELECT * FROM %s;", currentVersionTableName);
      stmt.execute(createViewStatement);

      // Drop DuckDB table for storeVersion as it's retired
      String deleteTableStatement = String.format("DROP TABLE %s;", versionTableName);
      stmt.execute(deleteTableStatement);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
