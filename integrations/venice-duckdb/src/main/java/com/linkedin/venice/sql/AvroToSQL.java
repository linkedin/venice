package com.linkedin.venice.sql;

import java.sql.JDBCType;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  /** Not sure if the reverse mapping will be needed. TODO: Decide whether to keep or remove. */
  private static final Map<JDBCType, Schema.Type> JDBC_TO_AVRO_TYPE_MAPPING;

  static {
    Map<Schema.Type, JDBCType> avroToJdbc = new EnumMap(Schema.Type.class);
    Map<JDBCType, Schema.Type> jdbcToAvro = new EnumMap(JDBCType.class);

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

    for (Map.Entry<Schema.Type, JDBCType> entry: avroToJdbc.entrySet()) {
      if (jdbcToAvro.put(entry.getValue(), entry.getKey()) != null) {
        // There is already a mapping!
        throw new IllegalStateException("There cannot be two mappings for: " + entry.getValue());
      }
    }

    AVRO_TO_JDBC_TYPE_MAPPING = Collections.unmodifiableMap(avroToJdbc);
    JDBC_TO_AVRO_TYPE_MAPPING = Collections.unmodifiableMap(jdbcToAvro);
  }

  private AvroToSQL() {
    // Static util
  }

  public static String createTableStatement(
      Schema avroSchema,
      Set<String> primaryKeyColumns,
      UnsupportedTypeHandling unsupportedTypeHandling) {
    if (avroSchema.getType() != Schema.Type.RECORD) {
      throw new IllegalArgumentException("Only Avro records can have a corresponding CREATE TABLE statement.");
    }
    String createTable = "CREATE TABLE " + cleanTableName(avroSchema.getName()) + "(";
    boolean firstColumn = true;

    for (Schema.Field field: avroSchema.getFields()) {
      Schema fieldSchema = field.schema();
      Schema.Type fieldType = fieldSchema.getType();

      // Unpack unions
      if (fieldType == Schema.Type.UNION) {
        List<Schema> unionBranches = fieldSchema.getTypes();
        boolean unsupported = false;
        if (unionBranches.size() != 2) {
          unsupported = true;
        } else if (unionBranches.get(0).getType() == Schema.Type.NULL) {
          fieldType = unionBranches.get(1).getType();
        } else if (unionBranches.get(1).getType() == Schema.Type.NULL) {
          fieldType = unionBranches.get(0).getType();
        } else {
          unsupported = true;
        }
        if (unsupported) {
          switch (unsupportedTypeHandling) {
            case SKIP:
              continue;
            case FAIL:
              throw new IllegalArgumentException(
                  "Avro unions are only supported if they have two branches and one of them is null. Provided union: "
                      + field.schema());
          }
        }
      }

      JDBCType correspondingType = AVRO_TO_JDBC_TYPE_MAPPING.get(fieldType);
      if (correspondingType == null) {
        switch (unsupportedTypeHandling) {
          case SKIP:
            continue;
          case FAIL:
            throw new IllegalArgumentException(fieldType + " is not supported!");
        }
      }

      if (firstColumn) {
        firstColumn = false;
      } else {
        createTable += ", ";
      }

      createTable += cleanColumnName(field.name()) + " " + correspondingType.name();

      if (primaryKeyColumns.contains(field.name())) {
        createTable += " PRIMARY KEY";
      }
    }
    createTable += ");";

    return createTable;
  }

  /**
   * This function should encapsulate the handling of any illegal characters (by either failing or converting them).
   */
  private static String cleanTableName(String avroRecordName) {
    return avroRecordName;
  }

  private static String cleanColumnName(String avroFieldName) {
    return avroFieldName;
  }
}
