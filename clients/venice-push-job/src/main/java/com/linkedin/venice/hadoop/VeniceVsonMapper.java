package com.linkedin.venice.hadoop;

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

  @Override
  protected FilterChain<BytesWritable> getFilterChain(final VeniceProperties props) {
    throw new UnsupportedOperationException("VeniceVsonMapper hasn't implemented the filter yet");
  }
}
