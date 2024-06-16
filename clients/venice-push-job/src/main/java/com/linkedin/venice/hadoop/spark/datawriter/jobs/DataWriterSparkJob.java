package com.linkedin.venice.hadoop.spark.datawriter.jobs;

import static com.linkedin.venice.hadoop.VenicePushJobConstants.GLOB_FILTER_PATTERN;
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
