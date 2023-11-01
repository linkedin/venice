package com.linkedin.venice.hadoop.spark.input.hdfs;

import static com.linkedin.venice.hadoop.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.ETL_VALUE_SCHEMA_TRANSFORMATION;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.FILE_KEY_SCHEMA;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.FILE_VALUE_SCHEMA;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.SCHEMA_STRING_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.VSON_PUSH;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.input.recordreader.VeniceRecordIterator;
import com.linkedin.venice.hadoop.input.recordreader.avro.VeniceAvroFileIterator;
import com.linkedin.venice.hadoop.input.recordreader.avro.VeniceAvroRecordReader;
import com.linkedin.venice.hadoop.input.recordreader.vson.VeniceVsonFileIterator;
import com.linkedin.venice.hadoop.input.recordreader.vson.VeniceVsonRecordReader;
import com.linkedin.venice.hadoop.spark.input.VeniceAbstractPartitionReader;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.connector.read.InputPartition;


public class VeniceHdfsInputPartitionReader extends VeniceAbstractPartitionReader {
  public VeniceHdfsInputPartitionReader(VeniceProperties jobConfig, VeniceHdfsInputPartition partition) {
    super(jobConfig, partition);
  }

  @Override
  protected VeniceRecordIterator createRecordIterator(VeniceProperties jobConfig, InputPartition partition) {
    if (!(partition instanceof VeniceHdfsInputPartition)) {
      throw new VeniceException("Expected VeniceHdfsInputPartition");
    }
    VeniceHdfsInputPartition inputPartition = (VeniceHdfsInputPartition) partition;

    Configuration configuration = new Configuration();
    FileSystem fs;
    try {
      fs = FileSystem.get(configuration);
    } catch (IOException e) {
      throw new VeniceException("Unable to get a FileSystem", e);
    }

    String keyFieldStr = jobConfig.getString(KEY_FIELD_PROP, DEFAULT_KEY_FIELD_PROP);
    String valueFieldStr = jobConfig.getString(VALUE_FIELD_PROP, DEFAULT_VALUE_FIELD_PROP);
    ETLValueSchemaTransformation etlValueSchemaTransformation = ETLValueSchemaTransformation
        .valueOf(jobConfig.getString(ETL_VALUE_SCHEMA_TRANSFORMATION, ETLValueSchemaTransformation.NONE.name()));
    boolean vsonPush = jobConfig.getBoolean(VSON_PUSH, false);
    Path filePath = inputPartition.getFilePath();

    if (vsonPush) {
      String fileKeySchema = jobConfig.getString(FILE_KEY_SCHEMA);
      String fileValueSchema = jobConfig.getString(FILE_VALUE_SCHEMA);
      VeniceVsonRecordReader recordReader =
          new VeniceVsonRecordReader(fileKeySchema, fileValueSchema, keyFieldStr, valueFieldStr);
      return new VeniceVsonFileIterator(fs, filePath, recordReader);
    } else {
      Schema fileSchema = AvroCompatibilityHelper.parse(jobConfig.getString(SCHEMA_STRING_PROP));
      VeniceAvroRecordReader recordReader =
          new VeniceAvroRecordReader(fileSchema, keyFieldStr, valueFieldStr, etlValueSchemaTransformation, null);
      return new VeniceAvroFileIterator(fs, filePath, recordReader);
    }
  }
}
