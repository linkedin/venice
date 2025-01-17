package com.linkedin.venice.sql;

import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class InsertProcessor {
  private final int[] keyFieldIndexToJdbcIndexMapping;
  private final int[] valueFieldIndexToJdbcIndexMapping;
  private final int[] keyFieldIndexToUnionBranchIndex;
  private final int[] valueFieldIndexToUnionBranchIndex;
  private final JDBCType[] keyFieldIndexToCorrespondingType;
  private final JDBCType[] valueFieldIndexToCorrespondingType;

  InsertProcessor(@Nonnull Schema keySchema, @Nonnull Schema valueSchema, @Nonnull Set<String> columnsToProject) {
    Objects.requireNonNull(keySchema);
    Objects.requireNonNull(valueSchema);
    Objects.requireNonNull(columnsToProject);

    int keyFieldCount = keySchema.getFields().size();
    this.keyFieldIndexToJdbcIndexMapping = new int[keyFieldCount];
    this.keyFieldIndexToUnionBranchIndex = new int[keyFieldCount];
    this.keyFieldIndexToCorrespondingType = new JDBCType[keyFieldCount];

    int valueFieldCount = valueSchema.getFields().size();
    this.valueFieldIndexToJdbcIndexMapping = new int[valueFieldCount];
    this.valueFieldIndexToUnionBranchIndex = new int[valueFieldCount];
    this.valueFieldIndexToCorrespondingType = new JDBCType[valueFieldCount];

    // N.B.: JDBC indices start at 1, not at 0.
    int index = 1;
    index = populateArrays(
        index,
        keySchema,
        this.keyFieldIndexToJdbcIndexMapping,
        this.keyFieldIndexToUnionBranchIndex,
        this.keyFieldIndexToCorrespondingType,
        Collections.emptySet()); // N.B.: All key columns must be projected.
    populateArrays(
        index, // N.B.: The same index value needs to carry over from key to value columns.
        valueSchema,
        this.valueFieldIndexToJdbcIndexMapping,
        this.valueFieldIndexToUnionBranchIndex,
        this.valueFieldIndexToCorrespondingType,
        columnsToProject);
  }

  public void process(GenericRecord key, GenericRecord value, PreparedStatement preparedStatement) {
    try {
      processRecord(
          key,
          preparedStatement,
          this.keyFieldIndexToJdbcIndexMapping,
          this.keyFieldIndexToUnionBranchIndex,
          this.keyFieldIndexToCorrespondingType);

      processRecord(
          value,
          preparedStatement,
          this.valueFieldIndexToJdbcIndexMapping,
          this.valueFieldIndexToUnionBranchIndex,
          this.valueFieldIndexToCorrespondingType);

      preparedStatement.execute();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private int populateArrays(
      int index,
      @Nonnull Schema schema,
      @Nonnull int[] avroFieldIndexToJdbcIndexMapping,
      @Nonnull int[] avroFieldIndexToUnionBranchIndex,
      @Nonnull JDBCType[] avroFieldIndexToCorrespondingType,
      @Nonnull Set<String> columnsToProject) {
    for (Schema.Field field: schema.getFields()) {
      JDBCType correspondingType = AvroToSQL.getCorrespondingType(field);
      if (correspondingType == null) {
        // Skipped field.
        continue;
      }
      if (!columnsToProject.isEmpty() && !columnsToProject.contains(field.name())) {
        // Column is not projected.
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
    return index;
  }

  private void processRecord(
      GenericRecord record,
      PreparedStatement preparedStatement,
      int[] avroFieldIndexToJdbcIndexMapping,
      int[] avroFieldIndexToUnionBranchIndex,
      JDBCType[] avroFieldIndexToCorrespondingType) throws SQLException {
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
      try {
        processField(jdbcIndex, fieldType, fieldValue, preparedStatement, field.name());
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to process field. Name: '" + field.name() + "; jdbcIndex: " + jdbcIndex + "; type: " + fieldType
                + "; value: " + fieldValue,
            e);
      }
    }
  }

  private void processField(
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
}
