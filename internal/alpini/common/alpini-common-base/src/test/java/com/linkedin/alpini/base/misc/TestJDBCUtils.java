package com.linkedin.alpini.base.misc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class TestJDBCUtils {
  @Test(groups = "unit")
  public void testCloseQuietlyStatement() throws SQLException {

    Statement stmt = Mockito.mock(Statement.class);

    JDBCUtils.closeQuietly(stmt);

    Mockito.verify(stmt).close();
    Mockito.verifyNoMoreInteractions(stmt);

    Mockito.reset(stmt);
    Mockito.doThrow(new SQLException()).when(stmt).close();

    JDBCUtils.closeQuietly(stmt);
    Mockito.verify(stmt).close();
    Mockito.verifyNoMoreInteractions(stmt);

    stmt = null;

    JDBCUtils.closeQuietly(stmt);
  }

  @Test(groups = "unit")
  public void testCloseQuietlyResultSet() throws SQLException {

    ResultSet rs = Mockito.mock(ResultSet.class);

    JDBCUtils.closeQuietly(rs);

    Mockito.verify(rs).close();
    Mockito.verifyNoMoreInteractions(rs);

    Mockito.reset(rs);
    Mockito.doThrow(new SQLException()).when(rs).close();

    JDBCUtils.closeQuietly(rs);
    Mockito.verify(rs).close();
    Mockito.verifyNoMoreInteractions(rs);

    rs = null;

    JDBCUtils.closeQuietly(rs);
  }

  @Test(groups = "unit")
  public void testCloseQuietlyConnection() throws SQLException {

    Connection conn = Mockito.mock(Connection.class);

    JDBCUtils.closeQuietly(conn);

    Mockito.verify(conn).close();
    Mockito.verifyNoMoreInteractions(conn);

    Mockito.reset(conn);
    Mockito.doThrow(new SQLException()).when(conn).close();

    JDBCUtils.closeQuietly(conn);
    Mockito.verify(conn).close();
    Mockito.verifyNoMoreInteractions(conn);

    conn = null;

    JDBCUtils.closeQuietly(conn);
  }
}
