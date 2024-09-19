package com.linkedin.venice.hadoop.input.recordreader.avro;

import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ETL_VALUE_SCHEMA_TRANSFORMATION;
import static com.linkedin.venice.vpj.VenicePushJobConstants.EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.GENERATE_PARTIAL_UPDATE_RECORD_FROM_INPUT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SCHEMA_STRING_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.UPDATE_SCHEMA_STRING_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;

import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;


/**
 * A record reader that reads records from Avro file input into Avro-serialized keys and values.
 */
public class VeniceAvroRecordReader extends AbstractAvroRecordReader<AvroWrapper<IndexedRecord>, NullWritable> {
  /**
   * This constructor is used when data is read from HDFS.
   * @param dataSchema Schema of the avro file
   * @param keyFieldStr Field name of the key field
   * @param valueFieldStr Field name of the value field
   * @param etlValueSchemaTransformation The type of transformation that was applied to this schema during ETL. When source data set is not an ETL job, use NONE.
   */
  public VeniceAvroRecordReader(
      Schema dataSchema,
      String keyFieldStr,
      String valueFieldStr,
      ETLValueSchemaTransformation etlValueSchemaTransformation,
      Schema updateSchema) {
    super(dataSchema, keyFieldStr, valueFieldStr, etlValueSchemaTransformation, updateSchema);
  }

  public static VeniceAvroRecordReader fromProps(VeniceProperties props) {
    Schema dataSchema = AvroSchemaParseUtils.parseSchemaFromJSON(
        props.getString(SCHEMA_STRING_PROP),
        props.getBoolean(EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED, DEFAULT_EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED));

    String keyFieldStr = props.getString(KEY_FIELD_PROP);
    String valueFieldStr = props.getString(VALUE_FIELD_PROP);

    ETLValueSchemaTransformation etlValueSchemaTransformation = ETLValueSchemaTransformation
        .valueOf(props.getString(ETL_VALUE_SCHEMA_TRANSFORMATION, ETLValueSchemaTransformation.NONE.name()));

    boolean generatePartialUpdateRecordFromInput = props.getBoolean(GENERATE_PARTIAL_UPDATE_RECORD_FROM_INPUT, false);
    Schema updateSchema = null;
    if (generatePartialUpdateRecordFromInput) {
      updateSchema =
          AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(props.getString(UPDATE_SCHEMA_STRING_PROP));
    }

    return new VeniceAvroRecordReader(
        dataSchema,
        keyFieldStr,
        valueFieldStr,
        etlValueSchemaTransformation,
        updateSchema);
  }

  @Override
  protected IndexedRecord getRecordDatum(AvroWrapper<IndexedRecord> record, NullWritable nullValue) {
    return record.datum();
  }
}
