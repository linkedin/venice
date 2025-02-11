package com.linkedin.venice.sql;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.Schema;


/**
 * Utility intended to convert Avro -> SQL, including DDL and DML statements.
 *
 * Initially, this implementation may have a DuckDB slant, though in the long-run it should ideally be vendor-neutral.
 */
public class AvroToSQL {
  public enum UnsupportedTypeHandling {
    FAIL, SKIP;
  }

  private static final Map<Schema.Type, JDBCType> AVRO_TO_JDBC_TYPE_MAPPING;

  static {
    Map<Schema.Type, JDBCType> avroToJdbc = new EnumMap<>(Schema.Type.class);

    // avroToJdbc.put(Schema.Type.UNION, JDBCType.?); // Unions need special handling, see below
    avroToJdbc.put(Schema.Type.FIXED, JDBCType.BINARY);
    avroToJdbc.put(Schema.Type.STRING, JDBCType.VARCHAR);
    avroToJdbc.put(Schema.Type.BYTES, JDBCType.VARBINARY);
    avroToJdbc.put(Schema.Type.INT, JDBCType.INTEGER);
    avroToJdbc.put(Schema.Type.LONG, JDBCType.BIGINT);
    avroToJdbc.put(Schema.Type.FLOAT, JDBCType.FLOAT);
    avroToJdbc.put(Schema.Type.DOUBLE, JDBCType.DOUBLE);
    avroToJdbc.put(Schema.Type.BOOLEAN, JDBCType.BOOLEAN);
    avroToJdbc.put(Schema.Type.NULL, JDBCType.NULL);

    // Unsupported for now, but eventually might be:
    // avroToJdbc.put(Schema.Type.RECORD, JDBCType.STRUCT);
    // avroToJdbc.put(Schema.Type.ENUM, JDBCType.?);
    // avroToJdbc.put(Schema.Type.ARRAY, JDBCType.ARRAY);
    // avroToJdbc.put(Schema.Type.MAP, JDBCType.?);

    AVRO_TO_JDBC_TYPE_MAPPING = Collections.unmodifiableMap(avroToJdbc);
  }

  private AvroToSQL() {
    /**
     * Static util.
     *
     * N.B.: For now, this is fine. But later on, we may want to specialize some of the behavior for different DB
     * vendors (e.g., to support both DuckDB and SQLite, or even others). At that point, we would likely want to
     * leverage subclasses, and therefore it may be cleaner to make this class abstract and instantiable. That is
     * fine, we'll cross that bridge when we get to it.
     */
  }

  @Nonnull
  public static TableDefinition getTableDefinition(
      @Nonnull String tableName,
      @Nonnull Schema keySchema,
      @Nonnull Schema valueSchema,
      @Nonnull Set<String> columnsToProject,
      @Nonnull UnsupportedTypeHandling unsupportedTypeHandling,
      boolean primaryKey) {
    List<ColumnDefinition> columnDefinitions = new ArrayList<>();
    int jdbcIndex = 1;
    for (Schema.Field field: combineColumns(keySchema, valueSchema, columnsToProject)) {
      JDBCType correspondingType = getCorrespondingType(field);
      if (correspondingType == null) {
        switch (unsupportedTypeHandling) {
          case SKIP:
            continue;
          case FAIL:
            Schema fieldSchema = field.schema();
            Schema.Type fieldType = fieldSchema.getType();
            throw new IllegalArgumentException(fieldType + " is not supported!");
          default:
            // Defensive code (unreachable)
            throw new IllegalStateException("Missing enum branch handling!");
        }
      }

      boolean isPrimaryKey = primaryKey && keySchema.getFields().contains(field);
      columnDefinitions.add(
          new ColumnDefinition(
              SQLUtils.cleanColumnName(field.name()),
              correspondingType,
              true, // TODO: plug nullability
              isPrimaryKey ? IndexType.PRIMARY_KEY : null,
              null, // TODO: plug default (if necessary)...
              null,
              jdbcIndex++));
    }

    return new TableDefinition(tableName, columnDefinitions);
  }

  @Nonnull
  public static String upsertStatement(
      @Nonnull String tableName,
      @Nonnull Schema keySchema,
      @Nonnull Schema valueSchema,
      @Nonnull Set<String> columnsToProject) {
    Set<Schema.Field> allColumns = combineColumns(keySchema, valueSchema, columnsToProject);
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("INSERT OR REPLACE INTO " + SQLUtils.cleanTableName(tableName) + " VALUES (");
    boolean firstColumn = true;

    for (Schema.Field field: allColumns) {
      JDBCType correspondingType = getCorrespondingType(field);
      if (correspondingType == null) {
        // Skipped field.
        continue;
      }

      if (firstColumn) {
        firstColumn = false;
      } else {
        stringBuffer.append(", ");
      }

      stringBuffer.append("?");
    }
    stringBuffer.append(");");

    return stringBuffer.toString();
  }

  @Nonnull
  public static PreparedStatementProcessor upsertProcessor(
      @Nonnull Schema keySchema,
      @Nonnull Schema valueSchema,
      @Nonnull Set<String> columnsToProject) {
    return new KeyValuePreparedStatementProcessor(keySchema, valueSchema, columnsToProject);
  }

  @Nonnull
  public static String deleteStatement(@Nonnull String tableName, @Nonnull Schema keySchema) {
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("DELETE FROM " + SQLUtils.cleanTableName(tableName) + " WHERE ");
    boolean firstColumn = true;

    for (Schema.Field field: keySchema.getFields()) {
      JDBCType correspondingType = getCorrespondingType(field);
      if (correspondingType == null) {
        // Skipped field.
        throw new IllegalArgumentException(
            "All types from the key schema must be supported, but field '" + field.name() + "' is of type: "
                + field.schema().getType());
      }

      if (firstColumn) {
        firstColumn = false;
      } else {
        stringBuffer.append(" AND ");
      }
      stringBuffer.append(SQLUtils.cleanColumnName(field.name()));
      stringBuffer.append(" = ?");
    }
    stringBuffer.append(";");

    return stringBuffer.toString();
  }

  @Nonnull
  public static PreparedStatementProcessor deleteProcessor(@Nonnull Schema keySchema) {
    return new KeyOnlyPreparedStatementProcessor(keySchema);
  }

  @Nullable
  static JDBCType getCorrespondingType(Schema.Field field) {
    Schema fieldSchema = field.schema();
    Schema.Type fieldType = fieldSchema.getType();

    // Unpack unions
    if (fieldType == Schema.Type.UNION) {
      List<Schema> unionBranches = fieldSchema.getTypes();
      if (unionBranches.size() == 2) {
        if (unionBranches.get(0).getType() == Schema.Type.NULL) {
          fieldType = unionBranches.get(1).getType();
        } else if (unionBranches.get(1).getType() == Schema.Type.NULL) {
          fieldType = unionBranches.get(0).getType();
        } else {
          return null;
        }
      } else {
        return null;
      }
    }

    return AVRO_TO_JDBC_TYPE_MAPPING.get(fieldType);
  }

  @Nonnull
  static Set<Schema.Field> combineColumns(
      @Nonnull Schema keySchema,
      @Nonnull Schema valueSchema,
      @Nonnull Set<String> columnsToProject) {
    Objects.requireNonNull(keySchema);
    Objects.requireNonNull(valueSchema);
    Objects.requireNonNull(columnsToProject);
    if (keySchema.getType() != Schema.Type.RECORD || valueSchema.getType() != Schema.Type.RECORD) {
      // TODO: We can improve this to handle primitive types which aren't wrapped inside records.
      throw new IllegalArgumentException("Only Avro records can have a corresponding CREATE TABLE statement.");
    }
    Set<Schema.Field> allColumns = new LinkedHashSet<>(keySchema.getFields().size() + valueSchema.getFields().size());
    allColumns.addAll(keySchema.getFields());
    for (Schema.Field field: valueSchema.getFields()) {
      if (columnsToProject.isEmpty() || columnsToProject.contains(field.name())) {
        if (!allColumns.add(field)) {
          throw new IllegalArgumentException(
              "The value field '" + field.name() + "' is also present in the key schema! "
                  + "Field names must not conflict across both key and value. "
                  + "This can be side-stepped by populating the columnsToProject param to include only unique fields.");
        }
      }
    }
    return allColumns;
  }
}
