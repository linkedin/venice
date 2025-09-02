package com.linkedin.venice.spark.datawriter.recordprocessor;

import static com.linkedin.venice.spark.utils.RmdPushUtils.getDeserializerForLogicalTimestamp;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.spark.SparkConstants;
import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;


/**
 * A Spark {@link MapFunction} that processes input {@link Row} and converts the logical timestamp in the RMD field
 * to the actual RMD byte array if necessary. It acts as pass through for non-long RMD types.
 */
public class SparkLogicalTimestampProcessor implements MapFunction<Row, Row> {
  private static final Logger LOGGER = LogManager.getLogger(SparkLogicalTimestampProcessor.class);
  private static final long serialVersionUID = 1L;
  private final boolean containsLogicalTimestamp;
  private final Schema rmdSchema;

  private transient RecordDeserializer<Long> logicalTimestampDeserializer;

  public SparkLogicalTimestampProcessor(boolean containsLogicalTimestamp, String rmdSchemaString) {
    if (containsLogicalTimestamp && (rmdSchemaString == null || rmdSchemaString.isEmpty())) {
      throw new IllegalArgumentException("RMD schema must be provided if the RMD field contains logical timestamps.");
    }

    LOGGER.info("RMD field contains logical timestamp: " + containsLogicalTimestamp);
    this.containsLogicalTimestamp = containsLogicalTimestamp;
    this.rmdSchema =
        containsLogicalTimestamp ? AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(rmdSchemaString) : null;
  }

  @Override
  public Row call(Row record) throws Exception {
    byte[] key = record.getAs(SparkConstants.KEY_COLUMN_NAME);
    byte[] value = record.getAs(SparkConstants.VALUE_COLUMN_NAME);
    byte[] rmd = record.getAs(SparkConstants.RMD_COLUMN_NAME);

    if (containsLogicalTimestamp) {
      /*
       * Lazily initialize the deserializer since the field is transient as RecordSerializer is not serializable.
       */
      if (logicalTimestampDeserializer == null) {
        logicalTimestampDeserializer = getDeserializerForLogicalTimestamp();
      }

      try {
        /*
         * We perform an additional serialization of the logical timestamp in the prior contract to keep the
         * implementation complexity low. The alternative is to introduce contracts with upstream stages to allow
         * both Long and Bytes for RMD.
         */
        Long timestamp = logicalTimestampDeserializer.deserialize(rmd);
        rmd = convertLogicalTimestampToRmd(rmdSchema, timestamp);
      } catch (Exception e) {
        // TODO: Should we fail the job instead of returning null RMD?
        LOGGER.error("Encountered error while deserializing logical timestamp due to", e);
        rmd = null;
      }
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
