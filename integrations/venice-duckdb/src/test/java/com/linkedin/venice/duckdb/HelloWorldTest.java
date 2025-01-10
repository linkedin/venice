package com.linkedin.venice.duckdb;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
   * Lightly adapted from: https://duckdb.org/docs/api/java.html#querying
   */
  @Test
  public void test() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = connection.createStatement()) {
      // create a table
      stmt.execute("CREATE TABLE items (item VARCHAR, value DECIMAL(10, 2), count INTEGER)");
      // insert two items into the table
      stmt.execute("INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)");

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM items")) {
        assertTrue(rs.next(), "There should be a first row!");
        assertEquals(rs.getString(1), "jeans");
        assertEquals(rs.getInt(3), 1);

        assertTrue(rs.next(), "There should be a second row!");
        assertEquals(rs.getString(1), "hammer");
        assertEquals(rs.getInt(3), 2);

        assertFalse(rs.next(), "There should only be two rows!");
      }
    }
  }
}
