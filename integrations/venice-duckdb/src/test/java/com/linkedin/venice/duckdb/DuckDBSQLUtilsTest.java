package com.linkedin.venice.duckdb;

import com.linkedin.venice.sql.SQLUtilsTest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.testng.annotations.Test;


@Test
public class DuckDBSQLUtilsTest extends SQLUtilsTest {
  @Override
  protected Connection getConnection() throws SQLException {
    return DriverManager.getConnection(DuckDBHelloWorldTest.DUCKDB_CONN_PREFIX);
  }
}
