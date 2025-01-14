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
import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;


public class DuckDBDaVinciRecordTransformer extends DaVinciRecordTransformer<String, GenericRecord, GenericRecord> {
  private static final String duckDBFilePath = "my_database.duckdb";
  // ToDo: Don't hardcode the table name. Get it from the storeName
  private static final String baseVersionTableName = "my_table_v";
  private static final Set<String> primaryKeys = Collections.singleton("firstName");
  private static final String deleteStatementTemplate = "DELETE FROM ? WHERE ? = ?;";
  private static final String createViewStatementTemplate =
      "CREATE OR REPLACE VIEW current_version AS SELECT * FROM ?;";
  private static final String dropTableStatementTemplate = "DROP TABLE ?;";
  private final String duckDBUrl;
  private final String versionTableName;

  public DuckDBDaVinciRecordTransformer(int storeVersion, boolean storeRecordsInDaVinci, String baseDir) {
    super(storeVersion, storeRecordsInDaVinci);
    versionTableName = baseVersionTableName + storeVersion;
    duckDBUrl = "jdbc:duckdb:" + baseDir + "/" + duckDBFilePath;
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

    // ToDo: Instead of creating a connection on every call, have a long-term connection. Maybe a connection pool?
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
    // ToDo: Instead of creating a connection on every call, have a long-term connection. Maybe a connection pool?
    try (Connection connection = DriverManager.getConnection(duckDBUrl)) {
      // ToDo make delete non-hardcoded on primaryKey
      try (PreparedStatement stmt = connection.prepareStatement(deleteStatementTemplate)) {
        stmt.setString(1, versionTableName);
        stmt.setString(2, "firstName");
        stmt.setString(1, key.get());
        stmt.execute();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onStartVersionIngestion(boolean isCurrentVersion) {
    try (Connection connection = DriverManager.getConnection(duckDBUrl);
        Statement stmt = connection.createStatement()) {
      // ToDo: Make into prepared statement
      String createTableStatement =
          AvroToSQL.createTableStatement(versionTableName, getOutputValueSchema(), primaryKeys, FAIL);
      stmt.execute(createTableStatement);

      if (isCurrentVersion) {
        try (PreparedStatement stmt1 = connection.prepareStatement(createViewStatementTemplate)) {
          stmt1.setString(1, versionTableName);
          stmt1.execute();
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onEndVersionIngestion(int currentVersion) {
    try (Connection connection = DriverManager.getConnection(duckDBUrl)) {
      // Swap to current version
      String currentVersionTableName = baseVersionTableName + currentVersion;
      try (PreparedStatement stmt = connection.prepareStatement(createViewStatementTemplate)) {
        stmt.setString(1, currentVersionTableName);
        stmt.execute();
      }

      // Drop DuckDB table for storeVersion as it's retired
      try (PreparedStatement stmt = connection.prepareStatement(dropTableStatementTemplate)) {
        stmt.setString(1, versionTableName);
        stmt.execute();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public String getDuckDBUrl() {
    return duckDBUrl;
  }
}
