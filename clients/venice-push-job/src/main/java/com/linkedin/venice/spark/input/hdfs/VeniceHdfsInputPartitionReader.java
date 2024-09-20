package com.linkedin.venice.spark.input.hdfs;

import static com.linkedin.venice.vpj.VenicePushJobConstants.VSON_PUSH;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.input.recordreader.VeniceRecordIterator;
import com.linkedin.venice.hadoop.input.recordreader.avro.VeniceAvroFileIterator;
import com.linkedin.venice.hadoop.input.recordreader.avro.VeniceAvroRecordReader;
import com.linkedin.venice.hadoop.input.recordreader.vson.VeniceVsonFileIterator;
import com.linkedin.venice.hadoop.input.recordreader.vson.VeniceVsonRecordReader;
import com.linkedin.venice.spark.input.VeniceAbstractPartitionReader;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
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
    FileSystem fs;
    try {
      fs = inputPartition.getFilePath().getFileSystem(new Configuration());
    } catch (IOException e) {
      throw new VeniceException("Unable to get a FileSystem", e);
    }

    boolean vsonPush = jobConfig.getBoolean(VSON_PUSH, false);
    Path filePath = inputPartition.getFilePath();

    if (vsonPush) {
      VeniceVsonRecordReader recordReader = new VeniceVsonRecordReader(jobConfig);
      return new VeniceVsonFileIterator(fs, filePath, recordReader);
    }

    VeniceAvroRecordReader recordReader = VeniceAvroRecordReader.fromProps(jobConfig);
    return new VeniceAvroFileIterator(fs, filePath, recordReader);
  }
}
