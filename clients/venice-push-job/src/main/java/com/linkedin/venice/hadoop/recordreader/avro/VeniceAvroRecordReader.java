package com.linkedin.venice.hadoop.recordreader.avro;

import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;


/**
 * A record reader that reads records from Avro file input into Avro-serialized keys and values.
 */
public class VeniceAvroRecordReader extends AbstractAvroRecordReader<AvroWrapper<IndexedRecord>, NullWritable> {
  /**
   * This constructor is used when data is read from HDFS.
   * @param topicName Topic which is to be published to
   * @param keyFieldStr Field name of the key field
   * @param valueFieldStr Field name of the value field
   * @param fs File system where the source data exists
   * @param hdfsPath Path of the avro file in the File system
   * @param etlValueSchemaTransformation The type of transformation that was applied to this schema during ETL. When source data set is not an ETL job, use NONE.
   */
  public VeniceAvroRecordReader(
      String topicName,
      String keyFieldStr,
      String valueFieldStr,
      FileSystem fs,
      Path hdfsPath,
      ETLValueSchemaTransformation etlValueSchemaTransformation) {
    super(topicName, keyFieldStr, valueFieldStr, fs, hdfsPath, etlValueSchemaTransformation);
  }

  /**
   * This constructor is used in the Dali, please consider evolving it gracefully otherwise it will break downstream dependency.
   * @param topicName Topic which is to be published to
   * @param fileSchema Schema of the source files
   * @param keyFieldStr Field name of the key field
   * @param valueFieldStr Field name of the value field
   * @param etlValueSchemaTransformation The type of transformation that was applied to this schema during ETL. When source data set is not an ETL job, use NONE.
   */
  public VeniceAvroRecordReader(
      String topicName,
      Schema fileSchema,
      String keyFieldStr,
      String valueFieldStr,
      ETLValueSchemaTransformation etlValueSchemaTransformation) {
    super(topicName, fileSchema, keyFieldStr, valueFieldStr, etlValueSchemaTransformation);
  }

  /**
   * This constructor is used in the Mapper.
   */
  public VeniceAvroRecordReader(VeniceProperties props) {
    super(props);
  }

  @Override
  protected IndexedRecord getRecordDatum(AvroWrapper<IndexedRecord> record, NullWritable nullValue) {
    return record.datum();
  }
}
