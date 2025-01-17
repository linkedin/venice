package com.linkedin.venice.sql;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;


public abstract class SQLUtilsTest {
  private static final String TABLE_NAME = "items";
  private static final String KEY1_NAME = "key1";
  private static final String KEY2_NAME = "key2";
  private static final String VAL1_NAME = "val1";
  private static final String VAL2_NAME = "val2";

  protected abstract Connection getConnection() throws SQLException;

  @Test
  public void pojoTesting() {
    // Basics
    TableDefinition tableDefinition = getTableDefinition();
    assertEquals(tableDefinition.getName(), TABLE_NAME);
    assertEquals(tableDefinition.getColumns().size(), 4);
    assertEquals(tableDefinition.getPrimaryKeyColumns().size(), 2);

    int expectedJdbcIndex = 1;
    ColumnDefinition key1 = tableDefinition.getColumnByJdbcIndex(expectedJdbcIndex);
    assertNotNull(key1);
    assertEquals(key1.getName(), KEY1_NAME);
    assertEquals(key1.getType(), JDBCType.INTEGER);
    assertFalse(key1.isNullable());
    assertEquals(key1.getIndexType(), IndexType.PRIMARY_KEY);
    assertNull(key1.getDefaultValue());
    assertNull(key1.getExtra());
    assertEquals(key1.getJdbcIndex(), expectedJdbcIndex++);

    ColumnDefinition key2 = tableDefinition.getColumnByJdbcIndex(expectedJdbcIndex);
    assertNotNull(key2);
    assertEquals(key2.getName(), KEY2_NAME);
    assertEquals(key2.getType(), JDBCType.BIGINT);
    assertFalse(key2.isNullable());
    assertEquals(key2.getIndexType(), IndexType.PRIMARY_KEY);
    assertNull(key2.getDefaultValue());
    assertNull(key2.getExtra());
    assertEquals(key2.getJdbcIndex(), expectedJdbcIndex++);

    ColumnDefinition val1 = tableDefinition.getColumnByJdbcIndex(expectedJdbcIndex);
    assertNotNull(val1);
    assertEquals(val1.getName(), VAL1_NAME);
    assertEquals(val1.getType(), JDBCType.VARCHAR);
    assertTrue(val1.isNullable());
    assertNull(val1.getIndexType());
    assertNull(val1.getDefaultValue());
    assertNull(val1.getExtra());
    assertEquals(val1.getJdbcIndex(), expectedJdbcIndex++);

    ColumnDefinition val2 = tableDefinition.getColumnByJdbcIndex(expectedJdbcIndex);
    assertNotNull(val2);
    assertEquals(val2.getName(), VAL2_NAME);
    assertEquals(val2.getType(), JDBCType.DOUBLE);
    assertTrue(val2.isNullable());
    assertNull(val2.getIndexType());
    assertNull(val2.getDefaultValue());
    assertNull(val2.getExtra());
    assertEquals(val2.getJdbcIndex(), expectedJdbcIndex++);

    // Equality checks
    TableDefinition otherTableDefinition = getTableDefinition();
    assertNotSame(tableDefinition, otherTableDefinition);
    assertEquals(tableDefinition, otherTableDefinition);
    assertEquals(tableDefinition, tableDefinition);

    TableDefinition differentTableDefinition = new TableDefinition(TABLE_NAME, Collections.singletonList(key1));
    assertNotEquals(tableDefinition, differentTableDefinition);

    assertNotEquals(tableDefinition, null);
    assertNotEquals(null, tableDefinition);
    assertNotEquals(tableDefinition, "");
    assertNotEquals("", tableDefinition);

    ColumnDefinition otherKey1 = otherTableDefinition.getColumnByName(KEY1_NAME);
    assertNotSame(key1, otherKey1);
    assertEquals(key1, otherKey1);
    assertEquals(key1, key1);

    ColumnDefinition otherKey2 = otherTableDefinition.getColumnByName(KEY2_NAME);
    assertNotSame(key2, otherKey2);
    assertEquals(key2, otherKey2);

    ColumnDefinition otherVal1 = otherTableDefinition.getColumnByName(VAL1_NAME);
    assertNotSame(val1, otherVal1);
    assertEquals(val1, otherVal1);

    ColumnDefinition otherVal2 = otherTableDefinition.getColumnByName(VAL2_NAME);
    assertNotSame(val2, otherVal2);
    assertEquals(val2, otherVal2);

    assertNull(otherTableDefinition.getColumnByName("bogus"));

    assertNotEquals(key1, null);
    assertNotEquals(null, key1);
    assertNotEquals(key1, "");
    assertNotEquals("", key1);

    // Invalid inputs
    assertThrows(NullPointerException.class, () -> new TableDefinition(null, Collections.emptyList()));
    assertThrows(IllegalArgumentException.class, () -> new TableDefinition(TABLE_NAME, Collections.emptyList()));

    assertThrows(IllegalArgumentException.class, () -> new ColumnDefinition(KEY1_NAME, JDBCType.INTEGER, 0));
    assertThrows(IllegalArgumentException.class, () -> new ColumnDefinition(VAL1_NAME, JDBCType.VARCHAR, 0));

    List<ColumnDefinition> outOfOrderColumns = new ArrayList<>();
    outOfOrderColumns.add(key2);
    outOfOrderColumns.add(key1);
    assertThrows(IllegalArgumentException.class, () -> new TableDefinition(TABLE_NAME, outOfOrderColumns));

    List<ColumnDefinition> sparseColumns = new ArrayList<>();
    sparseColumns.add(key1);
    sparseColumns.add(key2);
    sparseColumns.add(1, null);
    assertThrows(IllegalArgumentException.class, () -> new TableDefinition(TABLE_NAME, sparseColumns));

    assertThrows(IndexOutOfBoundsException.class, () -> tableDefinition.getColumnByJdbcIndex(0));
    assertThrows(IndexOutOfBoundsException.class, () -> tableDefinition.getColumnByJdbcIndex(5));
  }

  @Test
  public void testGetTableDefinition() throws SQLException {
    try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
      // Starting point: empty
      assertNull(SQLUtils.getTableDefinition(TABLE_NAME, connection));

      String differentTable = "not_the_table_we_want";
      statement.execute(
          "CREATE TABLE " + differentTable + " (" + KEY1_NAME + " INTEGER, " + KEY2_NAME + " BIGINT, " + VAL1_NAME
              + " VARCHAR UNIQUE, " + VAL2_NAME + " DOUBLE, PRIMARY KEY (key1, key2));");

      assertNull(SQLUtils.getTableDefinition(TABLE_NAME, connection));

      statement.execute(
          "CREATE TABLE " + TABLE_NAME + " (" + KEY1_NAME + " INTEGER, " + KEY2_NAME + " BIGINT, " + VAL1_NAME
              + " VARCHAR, " + VAL2_NAME + " DOUBLE, PRIMARY KEY (key1, key2));");

      TableDefinition tableDefinition = SQLUtils.getTableDefinition(TABLE_NAME, connection);
      assertNotNull(tableDefinition);

      TableDefinition expectedTableDefinition = getTableDefinition();
      assertEquals(tableDefinition, expectedTableDefinition);

      TableDefinition differentTableDefinition = SQLUtils.getTableDefinition(differentTable, connection);
      assertNotNull(differentTableDefinition);
      assertNotEquals(tableDefinition, differentTableDefinition);
    }
  }

  private TableDefinition getTableDefinition() {
    int jdbcIndex = 1;
    ColumnDefinition key1 =
        new ColumnDefinition(KEY1_NAME, JDBCType.INTEGER, false, IndexType.PRIMARY_KEY, jdbcIndex++);
    ColumnDefinition key2 = new ColumnDefinition(KEY2_NAME, JDBCType.BIGINT, false, IndexType.PRIMARY_KEY, jdbcIndex++);
    ColumnDefinition val1 = new ColumnDefinition(VAL1_NAME, JDBCType.VARCHAR, jdbcIndex++);
    ColumnDefinition val2 = new ColumnDefinition(VAL2_NAME, JDBCType.DOUBLE, jdbcIndex++);
    List<ColumnDefinition> columns = new ArrayList<>();
    columns.add(key1);
    columns.add(key2);
    columns.add(val1);
    columns.add(val2);
    return new TableDefinition(TABLE_NAME, columns);
  }
}
