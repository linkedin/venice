package com.linkedin.venice.spark.datawriter.jobs;

import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ETL_VALUE_SCHEMA_TRANSFORMATION;
import static com.linkedin.venice.vpj.VenicePushJobConstants.FILE_KEY_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.FILE_VALUE_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.GENERATE_PARTIAL_UPDATE_RECORD_FROM_INPUT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.GLOB_FILTER_PATTERN;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INPUT_PATH_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SCHEMA_STRING_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SPARK_NATIVE_INPUT_FORMAT_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.UPDATE_SCHEMA_STRING_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VSON_PUSH;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.hadoop.input.recordreader.avro.VeniceAvroRecordReader;
import com.linkedin.venice.hadoop.input.recordreader.vson.VeniceVsonRecordReader;
import com.linkedin.venice.spark.input.hdfs.VeniceHdfsSource;
import com.linkedin.venice.spark.utils.RowToAvroConverter;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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

    VeniceProperties jobProps = getJobProperties();
    boolean useNativeInputFormat = jobProps.getBoolean(SPARK_NATIVE_INPUT_FORMAT_ENABLED, false);

    if (!useNativeInputFormat) {
      return getDataFrameFromCustomInputFormat(sparkSession, pushJobSetting);
    } else if (pushJobSetting.isAvro) {
      return getAvroDataFrame(sparkSession, pushJobSetting);
    } else {
      return getVsonDataFrame(sparkSession, pushJobSetting);
    }
  }

  private Dataset<Row> getDataFrameFromCustomInputFormat(SparkSession sparkSession, PushJobSetting pushJobSetting) {
    DataFrameReader dataFrameReader = sparkSession.read();
    dataFrameReader.format(VeniceHdfsSource.class.getCanonicalName());
    setInputConf(sparkSession, dataFrameReader, INPUT_PATH_PROP, new Path(pushJobSetting.inputURI).toString());
    setInputConf(sparkSession, dataFrameReader, KEY_FIELD_PROP, pushJobSetting.keyField);
    setInputConf(sparkSession, dataFrameReader, VALUE_FIELD_PROP, pushJobSetting.valueField);
    if (pushJobSetting.etlValueSchemaTransformation != null) {
      setInputConf(
          sparkSession,
          dataFrameReader,
          ETL_VALUE_SCHEMA_TRANSFORMATION,
          pushJobSetting.etlValueSchemaTransformation.name());
    }
    if (pushJobSetting.isAvro) {
      setInputConf(sparkSession, dataFrameReader, SCHEMA_STRING_PROP, pushJobSetting.inputDataSchemaString);
      if (pushJobSetting.generatePartialUpdateRecordFromInput) {
        setInputConf(sparkSession, dataFrameReader, GENERATE_PARTIAL_UPDATE_RECORD_FROM_INPUT, String.valueOf(true));
        setInputConf(sparkSession, dataFrameReader, UPDATE_SCHEMA_STRING_PROP, pushJobSetting.valueSchemaString);
      }
      setInputConf(sparkSession, dataFrameReader, VSON_PUSH, String.valueOf(false));
    } else {
      setInputConf(sparkSession, dataFrameReader, VSON_PUSH, String.valueOf(true));
      setInputConf(sparkSession, dataFrameReader, FILE_KEY_SCHEMA, pushJobSetting.vsonInputKeySchemaString);
      setInputConf(sparkSession, dataFrameReader, FILE_VALUE_SCHEMA, pushJobSetting.vsonInputValueSchemaString);
    }
    return dataFrameReader.load();
  }

  private Dataset<Row> getAvroDataFrame(SparkSession sparkSession, PushJobSetting pushJobSetting) {
    Dataset<Row> df =
        sparkSession.read().format("avro").option("pathGlobFilter", GLOB_FILTER_PATTERN).load(pushJobSetting.inputURI);

    // Transforming the input data format
    df = df.map((MapFunction<Row, Row>) (record) -> {
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

  @Deprecated
  private Dataset<Row> getVsonDataFrame(SparkSession sparkSession, PushJobSetting pushJobSetting) {
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
