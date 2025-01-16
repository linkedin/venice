package com.linkedin.venice.duckdb;

import static com.linkedin.venice.sql.AvroToSQL.UnsupportedTypeHandling.FAIL;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.sql.AvroToSQL;
import com.linkedin.venice.sql.InsertProcessor;
import com.linkedin.venice.utils.lazy.Lazy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class DuckDBDaVinciRecordTransformer
    extends DaVinciRecordTransformer<GenericRecord, GenericRecord, GenericRecord> {
  private static final String duckDBFilePath = "my_database.duckdb";
  private static final String deleteStatementTemplate = "DELETE FROM %s WHERE %s = ?;";
  private static final String createViewStatementTemplate =
      "CREATE OR REPLACE VIEW current_version AS SELECT * FROM %s;";
  private static final String dropTableStatementTemplate = "DROP TABLE %s;";
  private final String storeNameWithoutVersionInfo;
  private final String versionTableName;
  private final String duckDBUrl;
  private final Set<String> columnsToProject;

  public DuckDBDaVinciRecordTransformer(
      int storeVersion,
      Schema keySchema,
      Schema originalValueSchema,
      Schema outputValueSchema,
      boolean storeRecordsInDaVinci,
      String baseDir,
      String storeNameWithoutVersionInfo,
      Set<String> columnsToProject) {
    super(storeVersion, keySchema, originalValueSchema, outputValueSchema, storeRecordsInDaVinci);
    this.storeNameWithoutVersionInfo = storeNameWithoutVersionInfo;
    this.versionTableName = buildStoreNameWithVersion(storeVersion);
    this.duckDBUrl = "jdbc:duckdb:" + baseDir + "/" + duckDBFilePath;
    this.columnsToProject = columnsToProject;
  }

  @Override
  public DaVinciRecordTransformerResult<GenericRecord> transform(Lazy<GenericRecord> key, Lazy<GenericRecord> value) {
    // Record transformation happens inside processPut as we need access to the connection object to create the prepared
    // statement
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.UNCHANGED);
  }

  @Override
  public void processPut(Lazy<GenericRecord> key, Lazy<GenericRecord> value) {
    // TODO: Pre-allocate the upsert statement and everything that goes into it, as much as possible.
    Schema keySchema = key.get().getSchema();
    Schema valueSchema = value.get().getSchema();
    String upsertStatement = AvroToSQL.upsertStatement(versionTableName, keySchema, valueSchema, this.columnsToProject);

    // ToDo: Instead of creating a connection on every call, have a long-term connection. Maybe a connection pool?
    try (Connection connection = DriverManager.getConnection(duckDBUrl)) {
      // TODO: Pre-allocate the upsert processor as well
      InsertProcessor upsertProcessor = AvroToSQL.upsertProcessor(keySchema, valueSchema, this.columnsToProject);

      // TODO: Pre-allocate the prepared statement (consider thread-local if it's not thread safe)
      try (PreparedStatement preparedStatement = connection.prepareStatement(upsertStatement)) {
        upsertProcessor.process(key.get(), value.get(), preparedStatement);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void processDelete(Lazy<GenericRecord> key) {
    // Unable to convert to prepared statement as table and column names can't be parameterized
    // ToDo make delete non-hardcoded on primaryKey
    String deleteStatement = String.format(deleteStatementTemplate, versionTableName, "key");

    // ToDo: Instead of creating a connection on every call, have a long-term connection. Maybe a connection pool?
    try (Connection connection = DriverManager.getConnection(duckDBUrl);
        PreparedStatement stmt = connection.prepareStatement(deleteStatement)) {
      stmt.setString(1, key.get().get("key").toString());
      stmt.execute();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onStartVersionIngestion(boolean isCurrentVersion) {
    try (Connection connection = DriverManager.getConnection(duckDBUrl);
        Statement stmt = connection.createStatement()) {
      String createTableStatement = AvroToSQL.createTableStatement(
          versionTableName,
          getKeySchema(),
          getOutputValueSchema(),
          this.columnsToProject,
          FAIL,
          true);
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
      String currentVersionTableName = buildStoreNameWithVersion(currentVersion);
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

  public String buildStoreNameWithVersion(int version) {
    return storeNameWithoutVersionInfo + "_v" + version;
  }
}
