package com.linkedin.venice.spark.datawriter.jobs;

import static com.linkedin.venice.vpj.VenicePushJobConstants.ETL_VALUE_SCHEMA_TRANSFORMATION;
import static com.linkedin.venice.vpj.VenicePushJobConstants.FILE_KEY_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.FILE_VALUE_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.GENERATE_PARTIAL_UPDATE_RECORD_FROM_INPUT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INPUT_PATH_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SCHEMA_STRING_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.UPDATE_SCHEMA_STRING_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VSON_PUSH;

import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.spark.input.hdfs.VeniceHdfsSource;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * The default implementation of {@link AbstractDataWriterSparkJob} for Avro and Vson file input formats.
 */
public class DataWriterSparkJob extends AbstractDataWriterSparkJob {
  @Override
  protected Dataset<Row> getUserInputDataFrame() {
    SparkSession sparkSession = getSparkSession();
    PushJobSetting pushJobSetting = getPushJobSetting();

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
}
