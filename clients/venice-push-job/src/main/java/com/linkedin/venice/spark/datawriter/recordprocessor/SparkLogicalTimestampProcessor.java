package com.linkedin.venice.spark.datawriter.recordprocessor;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.spark.SparkConstants;
import org.apache.avro.Schema;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;


/**
 * A Spark {@link MapFunction} that processes input {@link Row} and converts the logical timestamp in the RMD field
 * to the actual RMD byte array if necessary. It acts as pass through for non-long RMD types.
 */
public class SparkLogicalTimestampProcessor implements MapFunction<Row, Row> {
  private final boolean containsLogicalTimestamp;
  private final Schema rmdSchema;

  public SparkLogicalTimestampProcessor(boolean containsLogicalTimestamp, String rmdSchemaString) {
    if (containsLogicalTimestamp && (rmdSchemaString == null || rmdSchemaString.isEmpty())) {
      throw new IllegalArgumentException("RMD schema must be provided if the RMD field contains logical timestamps.");
    }

    this.containsLogicalTimestamp = containsLogicalTimestamp;
    this.rmdSchema =
        containsLogicalTimestamp ? AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(rmdSchemaString) : null;
  }

  @Override
  public Row call(Row record) throws Exception {
    byte[] key = record.getAs(SparkConstants.KEY_COLUMN_NAME);
    byte[] value = record.getAs(SparkConstants.VALUE_COLUMN_NAME);
    byte[] rmd;

    if (containsLogicalTimestamp) {
      Long timestamp = record.getAs(SparkConstants.RMD_COLUMN_NAME);
      rmd = convertLogicalTimestampToRmd(rmdSchema, timestamp);
    } else {
      rmd = record.getAs(SparkConstants.RMD_COLUMN_NAME);
    }

    return new GenericRowWithSchema(new Object[] { key, value, rmd }, SparkConstants.DEFAULT_SCHEMA);
  }

  @VisibleForTesting
  static byte[] convertLogicalTimestampToRmd(Schema rmdSchema, Long logicalTimestamp) {
    if (logicalTimestamp == -1L) {
      return null;
    }
    return RmdSchemaGenerator.generateRecordLevelTimestampMetadata(rmdSchema, logicalTimestamp);
  }
}
