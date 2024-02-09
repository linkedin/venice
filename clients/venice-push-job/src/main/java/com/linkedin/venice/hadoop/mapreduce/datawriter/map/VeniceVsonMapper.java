package com.linkedin.venice.hadoop.mapreduce.datawriter.map;

import com.linkedin.venice.hadoop.input.recordreader.AbstractVeniceRecordReader;
import com.linkedin.venice.hadoop.input.recordreader.vson.VeniceVsonRecordReader;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.hadoop.io.BytesWritable;


/**
 * Mapper that reads Vson input and deserializes it as Avro object and then Avro binary
 */
public class VeniceVsonMapper extends AbstractVeniceMapper<BytesWritable, BytesWritable> {
  @Override
  public AbstractVeniceRecordReader<BytesWritable, BytesWritable> getRecordReader(VeniceProperties props) {
    return new VeniceVsonRecordReader(props);
  }
}
