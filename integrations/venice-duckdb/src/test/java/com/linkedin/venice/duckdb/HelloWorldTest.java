package com.linkedin.venice.duckdb;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.testng.annotations.Test;


/**
 * The aim of this class is just to test DuckDB itself, without any Venice-ism involved.
 */
public class HelloWorldTest {
  /**
   * Adapted from: https://duckdb.org/docs/api/java.html#querying
   */
  @Test
  public void test() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = connection.createStatement()) {
      // create a table
      stmt.execute(createTableStatement("items"));
      // insert two items into the table
      stmt.execute(insertDataset1Statement("items"));

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM items")) {
        assertValidityOfResultSet1(rs);
      }
    }
  }

  /**
   * This test verifies that DuckDB supports the same table swap technique as other RDBMS:
   *
   * BEGIN TRANSACTION;
   * UPDATE TABLE current_version RENAME TO backup_version;
   * UPDATE TABLE future_version RENAME TO current_version;
   * COMMIT;
   *
   * This can be used as the basis for Venice version swaps.
   */
  @Test
  public void testVersionSwapViaTableRename() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = connection.createStatement()) {
      // create the current_version table
      stmt.execute(createTableStatement("current_version"));
      // insert two items into the table
      stmt.execute(insertDataset1Statement("current_version"));

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM current_version")) {
        assertValidityOfResultSet1(rs);
      }

      // create the future_version table
      stmt.execute(createTableStatement("future_version"));
      // insert two items into the table
      stmt.execute(insertDataset2Statement("future_version"));

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM future_version")) {
        assertValidityOfResultSet2(rs);
      }

      stmt.execute("BEGIN TRANSACTION;");
      stmt.execute("ALTER TABLE current_version RENAME to backup_version;");
      stmt.execute("ALTER TABLE future_version RENAME to current_version;");
      stmt.execute("COMMIT;");

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM current_version")) {
        assertValidityOfResultSet2(rs);
      }
    }
  }

  /**
   * This test verifies that DuckDB supports using view alteration as the mechanism for version swaps.
   *
   * This can be used as the basis for Venice version swaps.
   */
  @Test
  public void testVersionSwapViaViewAlteration() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = connection.createStatement()) {
      // create the current_version table
      stmt.execute(createTableStatement("my_table_v1"));
      // insert two items into the table
      stmt.execute(insertDataset1Statement("my_table_v1"));
      // create current_version view
      stmt.execute(createViewStatement("my_table_v1"));

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM my_table_v1")) {
        assertValidityOfResultSet1(rs);
      }

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM my_table_current_version")) {
        assertValidityOfResultSet1(rs);
      }

      // create the future_version table
      stmt.execute(createTableStatement("my_table_v2"));
      // insert two items into the table
      stmt.execute(insertDataset2Statement("my_table_v2"));

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM my_table_v2")) {
        assertValidityOfResultSet2(rs);
      }

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM my_table_current_version")) {
        // The content of the view should remain unchanged as we have not swapped yet.
        assertValidityOfResultSet1(rs);
      }

      // SWAP!
      stmt.execute(createViewStatement("my_table_v2"));

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM my_table_current_version")) {
        assertValidityOfResultSet2(rs);
      }
    }
  }

  @Test
  public void testPrimaryKey() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = connection.createStatement()) {
      // create a table
      stmt.execute("CREATE TABLE items (item VARCHAR PRIMARY KEY, value DECIMAL(10, 2), count INTEGER)");
      // insert two items into the table
      stmt.execute(insertDataset1Statement("items"));

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM items")) {
        assertValidityOfResultSet1(rs);
      }

      assertThrows(SQLException.class, () -> stmt.execute(insertDataset2Statement("items")));
    }
  }

  private String createTableStatement(String tableName) {
    return "CREATE TABLE " + tableName + " (item VARCHAR, value DECIMAL(10, 2), count INTEGER)";
  }

  private String createViewStatement(String tableName) {
    return "CREATE OR REPLACE VIEW my_table_current_version AS SELECT * FROM " + tableName + ";";
  }

  private String insertDataset1Statement(String tableName) {
    return "INSERT INTO " + tableName + " VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)";
  }

  private String insertDataset2Statement(String tableName) {
    return "INSERT INTO " + tableName + " VALUES ('jeans', 20.0, 2), ('t-shirt', 42.2, 1)";
  }

  private void assertValidityOfResultSet1(ResultSet rs) throws SQLException {
    assertTrue(rs.next(), "There should be a first row!");
    assertEquals(rs.getString(1), "jeans");
    assertEquals(rs.getInt(3), 1);

    assertTrue(rs.next(), "There should be a second row!");
    assertEquals(rs.getString(1), "hammer");
    assertEquals(rs.getInt(3), 2);

    assertFalse(rs.next(), "There should only be two rows!");
  }

  private void assertValidityOfResultSet2(ResultSet rs) throws SQLException {
    assertTrue(rs.next(), "There should be a first row!");
    assertEquals(rs.getString(1), "jeans");
    assertEquals(rs.getInt(3), 2);

    assertTrue(rs.next(), "There should be a second row!");
    assertEquals(rs.getString(1), "t-shirt");
    assertEquals(rs.getInt(3), 1);

    assertFalse(rs.next(), "There should only be two rows!");
  }
}
