package com.linkedin.venice.duckdb;

import static com.linkedin.venice.sql.AvroToSQL.UnsupportedTypeHandling.SKIP;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerRecordMetadata;
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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Enables SQL querying of Venice data by integrating DuckDB into DaVinci clients.
 *
 * This transformer runs inside DaVinci clients and automatically mirrors Venice store data
 * into a local DuckDB database. This allows you to query your Venice data using standard SQL
 * instead of the Venice key-value API.
 *
 * When you configure this transformer in your DaVinci client, it will:
 * - Create SQL tables that match your Venice store structure
 * - Keep the tables updated with Venice data changes
 * - Handle new Venice store versions by managing SQL table versions
 * - Provide a SQL view that always points to the current data
 */
public class DuckDBDaVinciRecordTransformer
    extends DaVinciRecordTransformer<GenericRecord, GenericRecord, GenericRecord> {
  private static final Logger LOGGER = LogManager.getLogger(DuckDBDaVinciRecordTransformer.class);
  private static final String duckDBFilePath = "my_database.duckdb";
  private static final String createViewStatementTemplate = "CREATE OR REPLACE VIEW \"%s\" AS SELECT * FROM \"%s\";";
  private static final String dropTableStatementTemplate = "DROP TABLE \"%s\";";
  private final AtomicBoolean setUpComplete = new AtomicBoolean();
  private final String versionTableName;
  private final String duckDBUrl;
  private final Set<String> columnsToProject;
  private final CloseableThreadLocal<Connection> connection;
  private final CloseableThreadLocal<PreparedStatement> deletePreparedStatement;
  private final CloseableThreadLocal<PreparedStatement> upsertPreparedStatement;
  private final PreparedStatementProcessor upsertProcessor;
  private final PreparedStatementProcessor deleteProcessor;

  /**
   * @param baseDir directory where DuckDB files will be stored
   * @param columnsToProject specific columns to include (leave null/empty for all columns)
   * @throws VeniceException if database setup fails
   */
  public DuckDBDaVinciRecordTransformer(
      String storeName,
      int storeVersion,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      DaVinciRecordTransformerConfig recordTransformerConfig,
      String baseDir,
      Set<String> columnsToProject) {
    super(storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, recordTransformerConfig);
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

  /**
   * Note: This always returns UNCHANGED because we are not modifying the record that is persisted in DaVinci.
   */
  @Override
  public DaVinciRecordTransformerResult<GenericRecord> transform(
      Lazy<GenericRecord> key,
      Lazy<GenericRecord> value,
      int partitionId,
      DaVinciRecordTransformerRecordMetadata recordMetadata) {
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.UNCHANGED);
  }

  /**
   * Stores a new/updated record in DuckDB when Venice receives a put event.
   */
  @Override
  public void processPut(
      Lazy<GenericRecord> key,
      Lazy<GenericRecord> value,
      int partitionId,
      DaVinciRecordTransformerRecordMetadata recordMetadata) {
    this.upsertProcessor.process(key.get(), value.get(), this.upsertPreparedStatement.get());
  }

  /**
   * Deletes a record from DuckDB when Venice receives a delete event.
   */
  @Override
  public void processDelete(
      Lazy<GenericRecord> key,
      int partitionId,
      DaVinciRecordTransformerRecordMetadata recordMetadata) {
    this.deleteProcessor.process(key.get(), null, this.deletePreparedStatement.get());
  }

  /**
   * Called when DaVinci starts ingesting a Venice store version.
   *
   * Creates the SQL table for this version if it doesn't exist, or verifies
   * the existing table structure is compatible. If this is the current version,
   * it also creates a SQL view pointing to this table.
   *
   * @param partitionId what partition is being subscribed
   * @param isCurrentVersion true if this is the active store version
   * @throws VeniceException if table creation fails or structure is incompatible
   * @throws RuntimeException if SQL operations fail
   */
  @Override
  public void onStartVersionIngestion(int partitionId, boolean isCurrentVersion) {
    if (setUpComplete.get()) {
      return;
    }

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
        String createViewStatement = String.format(createViewStatementTemplate, getStoreName(), versionTableName);
        stmt.execute(createViewStatement);
      }

      setUpComplete.set(true);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Called when DaVinci finishes ingesting all data for the store.
   *
   * Updates the main SQL view to point to the current version's table and
   * removes the table for the previous version that is retired.
   *
   * @param currentVersion the version that is now active
   * @throws RuntimeException if SQL operations fail
   */
  @Override
  public void onEndVersionIngestion(int currentVersion) {
    try (Connection connection = DriverManager.getConnection(duckDBUrl);
        Statement stmt = connection.createStatement()) {
      // Swap to current version
      String currentVersionTableName = buildStoreNameWithVersion(currentVersion);
      String createViewStatement = String.format(createViewStatementTemplate, getStoreName(), currentVersionTableName);
      stmt.execute(createViewStatement);

      if (currentVersion != getStoreVersion()) {
        // Unable to convert to prepared statement as table and column names can't be parameterized
        // Drop DuckDB table for storeVersion as it's retired
        String dropTableStatement = String.format(dropTableStatementTemplate, versionTableName);
        stmt.execute(dropTableStatement);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Indicates this transformer works with consistent record schemas.
   *
   * @return true (requires all records to have the same structure)
   */
  public boolean useUniformInputValueSchema() {
    return true;
  }

  /**
   * Gets the connection URL for the DuckDB database.
   *
   * @return DuckDB JDBC connection URL
   */
  public String getDuckDBUrl() {
    return duckDBUrl;
  }

  /**
   * Creates a versioned table name by combining store name with version number.
   *
   * @param version the store version number
   * @return table name in format "storeName_v<version>"
   */
  public String buildStoreNameWithVersion(int version) {
    return getStoreName() + "_v" + version;
  }

  /**
   * Cleans up database connections and resources.
   *
   * This is called automatically when the transformer is closed.
   */
  @Override
  public void close() {
    this.deletePreparedStatement.close();
    this.upsertPreparedStatement.close();
    this.connection.close();
  }
}
