package com.linkedin.venice.sql;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class SQLUtils {
  private static final String SHOW_TABLE_COLUMN_NAME = "column_name";
  private static final String SHOW_TABLE_TYPE = "column_type";
  private static final String SHOW_TABLE_NULLABLE = "null";
  private static final String SHOW_TABLE_INDEX_TYPE = "key";
  private static final String SHOW_TABLE_DEFAULT = "default";
  private static final String SHOW_TABLE_EXTRA = "extra";

  private SQLUtils() {
    /**
     * Static utils.
     *
     * If we wish to specialize the behavior of this class for different DB vendors, then we can change it to
     * an abstract class and have instantiable subclasses for each vendor.
     */
  }

  @Nullable
  public static TableDefinition getTableDefinition(@Nonnull String tableName, Connection connection)
      throws SQLException {
    try (PreparedStatement preparedStatement =
        connection.prepareStatement("SELECT * FROM (SHOW TABLES) WHERE name = ?;")) {
      preparedStatement.setString(1, cleanTableName(tableName));
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        if (!resultSet.next()) {
          return null;
        }
      }
    }
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW " + cleanTableName(tableName))) {
      int jdbcIndex = 1;
      List<ColumnDefinition> columns = new ArrayList<>();
      while (resultSet.next()) {
        columns.add(convertToColumnDefinition(resultSet, jdbcIndex++));
      }
      return new TableDefinition(tableName, columns);
    }
  }

  @Nonnull
  public static String createTableStatement(TableDefinition tableDefinition) {
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("CREATE TABLE " + cleanTableName(tableDefinition.getName()) + "(");
    boolean firstColumn = true;

    for (ColumnDefinition columnDefinition: tableDefinition.getColumns()) {
      if (firstColumn) {
        firstColumn = false;
      } else {
        stringBuffer.append(", ");
      }

      stringBuffer.append(cleanColumnName(columnDefinition.getName()) + " " + columnDefinition.getType().name());
    }

    firstColumn = true;
    if (!tableDefinition.getPrimaryKeyColumns().isEmpty()) {
      stringBuffer.append(", PRIMARY KEY(");
      for (ColumnDefinition columnDefinition: tableDefinition.getPrimaryKeyColumns()) {
        if (firstColumn) {
          firstColumn = false;
        } else {
          stringBuffer.append(", ");
        }
        stringBuffer.append(cleanColumnName(columnDefinition.getName()));
      }
      stringBuffer.append(")");
    }
    stringBuffer.append(");");

    return stringBuffer.toString();
  }

  /**
   * This function should encapsulate the handling of any illegal characters (by either failing or converting them).
   */
  @Nonnull
  static String cleanTableName(@Nonnull String avroRecordName) {
    return Objects.requireNonNull(avroRecordName);
  }

  /**
   * This function should encapsulate the handling of any illegal characters (by either failing or converting them).
   */
  @Nonnull
  static String cleanColumnName(@Nonnull String avroFieldName) {
    return Objects.requireNonNull(avroFieldName);
  }

  /**
   * {@link ResultSet#next()} should have already been called upstream. This function will not call it.
   */
  private static ColumnDefinition convertToColumnDefinition(ResultSet resultSet, int jdbcIndex) throws SQLException {
    return new ColumnDefinition(
        resultSet.getString(SHOW_TABLE_COLUMN_NAME),
        JDBCType.valueOf(resultSet.getString(SHOW_TABLE_TYPE)),
        resultSet.getString(SHOW_TABLE_NULLABLE).equals("YES"),
        convertIndexType(resultSet.getString(SHOW_TABLE_INDEX_TYPE)),
        resultSet.getString(SHOW_TABLE_DEFAULT),
        resultSet.getString(SHOW_TABLE_EXTRA),
        jdbcIndex);
  }

  private static IndexType convertIndexType(String indexType) {
    if (indexType == null) {
      return null;
    } else if (indexType.equals("PRI")) {
      return IndexType.PRIMARY_KEY;
    } else if (indexType.equals("UNI")) {
      return IndexType.UNIQUE;
    }
    throw new IllegalArgumentException("Unsupported index type: " + indexType);
  }
}
