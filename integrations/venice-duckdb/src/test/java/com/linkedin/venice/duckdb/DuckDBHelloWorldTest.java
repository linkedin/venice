package com.linkedin.venice.duckdb;

import com.linkedin.venice.sql.SQLHelloWorldTest;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.testng.annotations.Test;


/**
 * The aim of this class is just to test DuckDB itself, without any Venice-ism involved.
 */
public class DuckDBHelloWorldTest extends SQLHelloWorldTest {
  public static final String DUCKDB_CONN_PREFIX = "jdbc:duckdb:";

  @Override
  protected Connection getConnection() throws SQLException {
    return DriverManager.getConnection(DUCKDB_CONN_PREFIX);
  }

  @Override
  protected String getConnectionStringToPersistentDB() {
    File tmpDir = Utils.getTempDataDirectory();
    return DUCKDB_CONN_PREFIX + tmpDir.getAbsolutePath() + "/foo.duckdb";
  }

  @Test
  public void testAppender() throws SQLException {
    try (DuckDBConnection connection = (DuckDBConnection) getConnection();
        Statement stmt = connection.createStatement()) {
      // create a table
      stmt.execute(createTableStatement("items", true));
      // insert two items into the table
      try (DuckDBAppender appender = connection.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "items")) {
        appender.beginRow();
        appender.append("jeans");
        appender.append(20.0);
        appender.append(1);
        appender.endRow();

        appender.beginRow();
        appender.append("hammer");
        appender.append(42.2);
        appender.append(2);
        appender.endRow();
      }

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM items")) {
        assertValidityOfResultSet1(rs);
      }
    }
  }
}
