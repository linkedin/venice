package com.linkedin.venice.sql;

import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


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
    Map<Schema.Type, JDBCType> avroToJdbc = new EnumMap(Schema.Type.class);

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
  public static String createTableStatement(
      @Nonnull String tableName,
      @Nonnull Schema avroSchema,
      @Nonnull Set<String> primaryKeyColumns,
      @Nonnull UnsupportedTypeHandling unsupportedTypeHandling) {
    Objects.requireNonNull(avroSchema);
    Objects.requireNonNull(primaryKeyColumns);
    Objects.requireNonNull(unsupportedTypeHandling);
    if (avroSchema.getType() != Schema.Type.RECORD) {
      throw new IllegalArgumentException("Only Avro records can have a corresponding CREATE TABLE statement.");
    }
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("CREATE TABLE " + cleanTableName(tableName) + "(");
    boolean firstColumn = true;

    for (Schema.Field field: avroSchema.getFields()) {
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

      if (firstColumn) {
        firstColumn = false;
      } else {
        stringBuffer.append(", ");
      }

      stringBuffer.append(cleanColumnName(field.name()) + " " + correspondingType.name());
    }

    firstColumn = true;
    if (!primaryKeyColumns.isEmpty()) {
      stringBuffer.append(", PRIMARY KEY(");
      for (String pkColumn: primaryKeyColumns) {
        if (firstColumn) {
          firstColumn = false;
        } else {
          stringBuffer.append(", ");
        }
        stringBuffer.append(cleanColumnName(pkColumn));
      }
      stringBuffer.append(")");
    }
    stringBuffer.append(");");

    return stringBuffer.toString();
  }

  @Nonnull
  public static String upsertStatement(@Nonnull String tableName, @Nonnull Schema recordSchema) {
    Objects.requireNonNull(recordSchema);
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("INSERT OR REPLACE INTO " + cleanTableName(tableName) + " VALUES (");
    boolean firstColumn = true;

    for (Schema.Field field: recordSchema.getFields()) {
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
  public static BiConsumer<GenericRecord, PreparedStatement> upsertProcessor(@Nonnull Schema recordSchema) {
    Objects.requireNonNull(recordSchema);
    // N.B.: JDBC indices start at 1, not at 0;
    int index = 1;
    int[] avroFieldIndexToJdbcIndexMapping = new int[recordSchema.getFields().size()];
    int[] avroFieldIndexToUnionBranchIndex = new int[recordSchema.getFields().size()];
    JDBCType[] avroFieldIndexToCorrespondingType = new JDBCType[recordSchema.getFields().size()];
    for (Schema.Field field: recordSchema.getFields()) {
      JDBCType correspondingType = getCorrespondingType(field);
      if (correspondingType == null) {
        // Skipped field.
        continue;
      }
      avroFieldIndexToJdbcIndexMapping[field.pos()] = index++;
      avroFieldIndexToCorrespondingType[field.pos()] = correspondingType;
      if (field.schema().getType() == Schema.Type.UNION) {
        Schema fieldSchema = field.schema();
        List<Schema> unionBranches = fieldSchema.getTypes();
        if (unionBranches.get(0).getType() == Schema.Type.NULL) {
          avroFieldIndexToUnionBranchIndex[field.pos()] = 1;
        } else if (unionBranches.get(1).getType() == Schema.Type.NULL) {
          avroFieldIndexToUnionBranchIndex[field.pos()] = 0;
        } else {
          throw new IllegalStateException("Should have skipped unsupported union: " + fieldSchema);
        }
      }
    }
    return (record, preparedStatement) -> {
      try {
        int jdbcIndex;
        JDBCType jdbcType;
        Object fieldValue;
        Schema.Type fieldType;
        for (Schema.Field field: record.getSchema().getFields()) {
          jdbcIndex = avroFieldIndexToJdbcIndexMapping[field.pos()];
          if (jdbcIndex == 0) {
            // Skipped field.
            continue;
          }
          fieldValue = record.get(field.pos());
          if (fieldValue == null) {
            jdbcType = avroFieldIndexToCorrespondingType[field.pos()];
            preparedStatement.setNull(jdbcIndex, jdbcType.getVendorTypeNumber());
            continue;
          }
          fieldType = field.schema().getType();
          if (fieldType == Schema.Type.UNION) {
            // Unions are handled via unpacking
            fieldType = field.schema().getTypes().get(avroFieldIndexToUnionBranchIndex[field.pos()]).getType();
          }

          processField(jdbcIndex, fieldType, fieldValue, preparedStatement, field.name());
        }
        preparedStatement.execute();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    };
  }

  private static void processField(
      int jdbcIndex,
      @Nonnull Schema.Type fieldType,
      @Nonnull Object fieldValue,
      @Nonnull PreparedStatement preparedStatement,
      @Nonnull String fieldName) throws SQLException {
    switch (fieldType) {
      case FIXED:
      case BYTES:
        preparedStatement.setBytes(jdbcIndex, ByteUtils.extractByteArray((ByteBuffer) fieldValue));
        break;
      case STRING:
        preparedStatement.setString(jdbcIndex, fieldValue.toString());
        break;
      case INT:
        preparedStatement.setInt(jdbcIndex, (int) fieldValue);
        break;
      case LONG:
        preparedStatement.setLong(jdbcIndex, (long) fieldValue);
        break;
      case FLOAT:
        preparedStatement.setFloat(jdbcIndex, (float) fieldValue);
        break;
      case DOUBLE:
        preparedStatement.setDouble(jdbcIndex, (double) fieldValue);
        break;
      case BOOLEAN:
        preparedStatement.setBoolean(jdbcIndex, (boolean) fieldValue);
        break;
      case NULL:
        // Weird case... probably never comes into play?
        preparedStatement.setNull(jdbcIndex, JDBCType.NULL.getVendorTypeNumber());
        break;

      case UNION:
        // Defensive code. Unreachable.
        throw new IllegalArgumentException(
            "Unions should be unpacked by the calling function, but union field '" + fieldName + "' was passed in!");

      // These types could be supported eventually, but for now aren't.
      case RECORD:
      case ENUM:
      case ARRAY:
      case MAP:
      default:
        throw new IllegalStateException("Should have skipped field '" + fieldName + "' but somehow didn't!");
    }
  }

  @Nullable
  private static JDBCType getCorrespondingType(Schema.Field field) {
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

  /**
   * This function should encapsulate the handling of any illegal characters (by either failing or converting them).
   */
  @Nonnull
  private static String cleanTableName(@Nonnull String avroRecordName) {
    return Objects.requireNonNull(avroRecordName);
  }

  /**
   * This function should encapsulate the handling of any illegal characters (by either failing or converting them).
   */
  @Nonnull
  private static String cleanColumnName(@Nonnull String avroFieldName) {
    return Objects.requireNonNull(avroFieldName);
  }
}
