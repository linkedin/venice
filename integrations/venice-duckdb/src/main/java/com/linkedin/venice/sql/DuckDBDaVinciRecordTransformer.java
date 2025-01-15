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
  private static final String deleteStatementTemplate = "DELETE FROM %s WHERE %s = ?;";
  private static final String createViewStatementTemplate =
      "CREATE OR REPLACE VIEW current_version AS SELECT * FROM %s;";
  private static final String dropTableStatementTemplate = "DROP TABLE %s;";
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

  @Override
  public Schema getOutputValueSchema() {
    return SchemaBuilder.record("nameRecord")
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
    // Unable to convert to prepared statement as table and column names can't be parameterized
    // ToDo make delete non-hardcoded on primaryKey
    String deleteStatement = String.format(deleteStatementTemplate, versionTableName, "firstName");

    // ToDo: Instead of creating a connection on every call, have a long-term connection. Maybe a connection pool?
    try (Connection connection = DriverManager.getConnection(duckDBUrl);
        PreparedStatement stmt = connection.prepareStatement(deleteStatement)) {
      stmt.setString(1, key.get());
      stmt.execute();
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
        // Unable to convert to prepared statement as table and column names can't be parameterized
        String createViewStatement = String.format(createViewStatementTemplate, versionTableName);
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
      String createViewStatement = String.format(createViewStatementTemplate, currentVersionTableName);
      stmt.execute(createViewStatement);

      // Unable to convert to prepared statement as table and column names can't be parameterized
      // Drop DuckDB table for storeVersion as it's retired
      String dropTableStatement = String.format(dropTableStatementTemplate, versionTableName);
      stmt.execute(dropTableStatement);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public String getDuckDBUrl() {
    return duckDBUrl;
  }
}
