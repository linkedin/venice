package com.linkedin.venice.hadoop;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
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
  protected Optional<AbstractVeniceFilter<BytesWritable>> getFilter(final VeniceProperties props) {
    return Optional.empty();
  }
}
