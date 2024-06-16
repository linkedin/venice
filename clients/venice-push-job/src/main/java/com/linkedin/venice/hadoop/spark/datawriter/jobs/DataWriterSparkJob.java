package com.linkedin.venice.hadoop.spark.datawriter.jobs;

import static com.linkedin.venice.hadoop.VenicePushJobConstants.ETL_VALUE_SCHEMA_TRANSFORMATION;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.FILE_KEY_SCHEMA;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.FILE_VALUE_SCHEMA;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.GENERATE_PARTIAL_UPDATE_RECORD_FROM_INPUT;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.GLOB_FILTER_PATTERN;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.SCHEMA_STRING_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.UPDATE_SCHEMA_STRING_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.VSON_PUSH;
import static com.linkedin.venice.hadoop.spark.SparkConstants.DEFAULT_SCHEMA;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.hadoop.input.recordreader.avro.VeniceAvroRecordReader;
import com.linkedin.venice.hadoop.input.recordreader.vson.VeniceVsonRecordReader;
import com.linkedin.venice.hadoop.spark.utils.RowToAvroConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;


/**
 * The default implementation of {@link AbstractDataWriterSparkJob} for Avro and Vson file input formats.
 */
public class DataWriterSparkJob extends AbstractDataWriterSparkJob {
  @Override
  protected Dataset<Row> getUserInputDataFrame() {
    SparkSession sparkSession = getSparkSession();
    PushJobSetting pushJobSetting = getPushJobSetting();

    if (pushJobSetting.isAvro) {
      return getAvroDataFrame(sparkSession, pushJobSetting);
    } else {
      return getVsonDataFrame(sparkSession, pushJobSetting);
    }
  }

  private Dataset<Row> getAvroDataFrame(SparkSession sparkSession, PushJobSetting pushJobSetting) {
    RuntimeConfig sparkConf = sparkSession.conf();
    sparkConf.set(KEY_FIELD_PROP, pushJobSetting.keyField);
    sparkConf.set(VALUE_FIELD_PROP, pushJobSetting.valueField);
    sparkConf.set(ETL_VALUE_SCHEMA_TRANSFORMATION, pushJobSetting.etlValueSchemaTransformation.name());
    sparkConf.set(SCHEMA_STRING_PROP, pushJobSetting.inputDataSchemaString);
    if (pushJobSetting.generatePartialUpdateRecordFromInput) {
      sparkConf.set(GENERATE_PARTIAL_UPDATE_RECORD_FROM_INPUT, String.valueOf(true));
      sparkConf.set(UPDATE_SCHEMA_STRING_PROP, pushJobSetting.valueSchemaString);
    }
    sparkConf.set(VSON_PUSH, String.valueOf(false));

    Dataset<Row> df =
        sparkSession.read().format("avro").option("pathGlobFilter", GLOB_FILTER_PATTERN).load(pushJobSetting.inputURI);

    // Transforming the input data format
    df = df.map((MapFunction<Row, Row>) (Row record) -> {
      Schema updateSchema = null;
      if (pushJobSetting.generatePartialUpdateRecordFromInput) {
        updateSchema = AvroCompatibilityHelper.parse(pushJobSetting.valueSchemaString);
      }

      GenericRecord rowRecord = RowToAvroConverter.convert(record, pushJobSetting.inputDataSchema);
      VeniceAvroRecordReader recordReader = new VeniceAvroRecordReader(
          pushJobSetting.inputDataSchema,
          pushJobSetting.keyField,
          pushJobSetting.valueField,
          pushJobSetting.etlValueSchemaTransformation,
          updateSchema);

      AvroWrapper<IndexedRecord> recordAvroWrapper = new AvroWrapper<>(rowRecord);
      final byte[] inputKeyBytes = recordReader.getKeyBytes(recordAvroWrapper, null);
      final byte[] inputValueBytes = recordReader.getValueBytes(recordAvroWrapper, null);

      return new GenericRowWithSchema(new Object[] { inputKeyBytes, inputValueBytes }, DEFAULT_SCHEMA);
    }, RowEncoder.apply(DEFAULT_SCHEMA));

    return df;
  }

  private Dataset<Row> getVsonDataFrame(SparkSession sparkSession, PushJobSetting pushJobSetting) {
    RuntimeConfig sparkConf = sparkSession.conf();
    sparkConf.set(VSON_PUSH, String.valueOf(true));
    sparkConf.set(FILE_KEY_SCHEMA, pushJobSetting.vsonInputKeySchemaString);
    sparkConf.set(FILE_VALUE_SCHEMA, pushJobSetting.vsonInputValueSchemaString);

    JavaRDD<Row> rdd = sparkSession.sparkContext()
        .sequenceFile(pushJobSetting.inputURI, BytesWritable.class, BytesWritable.class)
        .toJavaRDD()
        .map(record -> {
          VeniceVsonRecordReader recordReader = new VeniceVsonRecordReader(
              pushJobSetting.vsonInputKeySchemaString,
              pushJobSetting.vsonInputValueSchemaString,
              pushJobSetting.keyField,
              pushJobSetting.valueField);

          final byte[] inputKeyBytes = recordReader.getKeyBytes(record._1, record._2);
          final byte[] inputValueBytes = recordReader.getValueBytes(record._1, record._2);

          return new GenericRowWithSchema(new Object[] { inputKeyBytes, inputValueBytes }, DEFAULT_SCHEMA);
        });
    return sparkSession.createDataFrame(rdd, DEFAULT_SCHEMA);
  }
}
