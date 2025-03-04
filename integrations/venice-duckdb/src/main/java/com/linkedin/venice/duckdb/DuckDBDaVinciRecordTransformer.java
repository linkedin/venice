package com.linkedin.venice.duckdb;

import static com.linkedin.venice.sql.AvroToSQL.UnsupportedTypeHandling.SKIP;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.sql.AvroToSQL;
import com.linkedin.venice.sql.PreparedStatementProcessor;
import com.linkedin.venice.sql.SQLUtils;
import com.linkedin.venice.sql.TableDefinition;
import com.linkedin.venice.utils.concurrent.CloseableThreadLocal;
import com.linkedin.venice.utils.lazy.Lazy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DuckDBDaVinciRecordTransformer
    extends DaVinciRecordTransformer<GenericRecord, GenericRecord, GenericRecord> {
  private static final Logger LOGGER = LogManager.getLogger(DuckDBDaVinciRecordTransformer.class);
  private static final String duckDBFilePath = "my_database.duckdb";
  private static final String createViewStatementTemplate = "CREATE OR REPLACE VIEW %s AS SELECT * FROM %s;";
  private static final String dropTableStatementTemplate = "DROP TABLE %s;";
  private final String storeNameWithoutVersionInfo;
  private final String versionTableName;
  private final String duckDBUrl;
  private final Set<String> columnsToProject;
  private final CloseableThreadLocal<Connection> connection;
  private final CloseableThreadLocal<PreparedStatement> deletePreparedStatement;
  private final CloseableThreadLocal<PreparedStatement> upsertPreparedStatement;
  private final PreparedStatementProcessor upsertProcessor;
  private final PreparedStatementProcessor deleteProcessor;

  public DuckDBDaVinciRecordTransformer(
      int storeVersion,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      DaVinciRecordTransformerConfig recordTransformerConfig,
      String baseDir,
      String storeNameWithoutVersionInfo,
      Set<String> columnsToProject) {
    super(storeVersion, keySchema, inputValueSchema, outputValueSchema, recordTransformerConfig);
    this.storeNameWithoutVersionInfo = storeNameWithoutVersionInfo;
    this.versionTableName = buildStoreNameWithVersion(storeVersion);
    this.duckDBUrl = "jdbc:duckdb:" + baseDir + "/" + duckDBFilePath;
    this.columnsToProject = columnsToProject;
    String deleteStatement = AvroToSQL.deleteStatement(versionTableName, keySchema);
    String upsertStatement = AvroToSQL.upsertStatement(versionTableName, keySchema, inputValueSchema, columnsToProject);
    this.connection = CloseableThreadLocal.withInitial(() -> {
      try {
        return DriverManager.getConnection(duckDBUrl);
      } catch (SQLException e) {
        throw new VeniceException("Failed to connect to DB!", e);
      }
    });
    this.deletePreparedStatement = CloseableThreadLocal.withInitial(() -> {
      try {
        return this.connection.get().prepareStatement(deleteStatement);
      } catch (SQLException e) {
        throw new VeniceException("Failed to create PreparedStatement for: " + deleteStatement, e);
      }
    });
    this.upsertPreparedStatement = CloseableThreadLocal.withInitial(() -> {
      try {
        return this.connection.get().prepareStatement(upsertStatement);
      } catch (SQLException e) {
        throw new VeniceException("Failed to create PreparedStatement for: " + upsertStatement, e);
      }
    });
    this.upsertProcessor = AvroToSQL.upsertProcessor(keySchema, inputValueSchema, columnsToProject);
    this.deleteProcessor = AvroToSQL.deleteProcessor(keySchema);
  }

  @Override
  public DaVinciRecordTransformerResult<GenericRecord> transform(
      Lazy<GenericRecord> key,
      Lazy<GenericRecord> value,
      int partitionId) {
    // Record transformation happens inside processPut as we need access to the connection object to create the prepared
    // statement
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.UNCHANGED);
  }

  @Override
  public void processPut(Lazy<GenericRecord> key, Lazy<GenericRecord> value, int partitionId) {
    this.upsertProcessor.process(key.get(), value.get(), this.upsertPreparedStatement.get());
  }

  @Override
  public void processDelete(Lazy<GenericRecord> key, int partitionId) {
    this.deleteProcessor.process(key.get(), null, this.deletePreparedStatement.get());
  }

  @Override
  public void onStartVersionIngestion(boolean isCurrentVersion) {
    try (Connection connection = DriverManager.getConnection(duckDBUrl);
        Statement stmt = connection.createStatement()) {
      TableDefinition desiredTableDefinition = AvroToSQL.getTableDefinition(
          this.versionTableName,
          getKeySchema(),
          getOutputValueSchema(),
          this.columnsToProject,
          SKIP,
          true);
      TableDefinition existingTableDefinition = SQLUtils.getTableDefinition(this.versionTableName, connection);
      if (existingTableDefinition == null) {
        LOGGER.info("Table '{}' not found on disk, will create it from scratch", this.versionTableName);
        String createTableStatement = SQLUtils.createTableStatement(desiredTableDefinition);
        stmt.execute(createTableStatement);
      } else if (existingTableDefinition.equals(desiredTableDefinition)) {
        LOGGER.info("Table '{}' found on disk and its schema is compatible. Will reuse.", this.versionTableName);
      } else {
        // TODO: Handle the wiping and re-bootstrap automatically.
        throw new VeniceException(
            "Table '" + this.versionTableName + "' found on disk, but its schema is incompatible. Please wipe.");
      }

      if (isCurrentVersion) {
        // Unable to convert to prepared statement as table and column names can't be parameterized
        String createViewStatement =
            String.format(createViewStatementTemplate, storeNameWithoutVersionInfo, versionTableName);
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
      String createViewStatement =
          String.format(createViewStatementTemplate, storeNameWithoutVersionInfo, currentVersionTableName);
      stmt.execute(createViewStatement);

      if (currentVersion != getStoreVersion()) {
        // Only drop non-current versions, e.g., the backup version getting retired.

        // Unable to convert to prepared statement as table and column names can't be parameterized
        // Drop DuckDB table for storeVersion as it's retired
        String dropTableStatement = String.format(dropTableStatementTemplate, versionTableName);
        stmt.execute(dropTableStatement);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean useUniformInputValueSchema() {
    return true;
  }

  public String getDuckDBUrl() {
    return duckDBUrl;
  }

  public String buildStoreNameWithVersion(int version) {
    return storeNameWithoutVersionInfo + "_v" + version;
  }

  @Override
  public void close() {
    this.deletePreparedStatement.close();
    this.upsertPreparedStatement.close();
    this.connection.close();
  }
}
