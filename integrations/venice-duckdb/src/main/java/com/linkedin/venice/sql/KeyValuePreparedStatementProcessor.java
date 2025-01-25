package com.linkedin.venice.sql;

import com.linkedin.venice.exceptions.VeniceException;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class KeyValuePreparedStatementProcessor extends KeyOnlyPreparedStatementProcessor {
  private final int[] valueFieldIndexToJdbcIndexMapping;
  private final int[] valueFieldIndexToUnionBranchIndex;
  private final JDBCType[] valueFieldIndexToCorrespondingType;

  KeyValuePreparedStatementProcessor(
      @Nonnull Schema keySchema,
      @Nonnull Schema valueSchema,
      @Nonnull Set<String> columnsToProject) {
    super(keySchema);
    Objects.requireNonNull(columnsToProject);
    Objects.requireNonNull(valueSchema);

    int valueFieldCount = valueSchema.getFields().size();
    this.valueFieldIndexToJdbcIndexMapping = new int[valueFieldCount];
    this.valueFieldIndexToUnionBranchIndex = new int[valueFieldCount];
    this.valueFieldIndexToCorrespondingType = new JDBCType[valueFieldCount];

    int valueStartingIndex = getLastKeyJdbcIndex() + 1;
    populateArrays(
        valueStartingIndex,
        valueSchema,
        this.valueFieldIndexToJdbcIndexMapping,
        this.valueFieldIndexToUnionBranchIndex,
        this.valueFieldIndexToCorrespondingType,
        columnsToProject);
  }

  @Override
  public void process(GenericRecord key, GenericRecord value, PreparedStatement preparedStatement) {
    try {
      processKey(key, preparedStatement);

      processRecord(
          value,
          preparedStatement,
          this.valueFieldIndexToJdbcIndexMapping,
          this.valueFieldIndexToUnionBranchIndex,
          this.valueFieldIndexToCorrespondingType);

      preparedStatement.execute();
    } catch (SQLException e) {
      throw new VeniceException("Failed to execute prepared statement!", e);
    }
  }
}
