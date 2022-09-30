package com.linkedin.venice.hadoop;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;


public class VeniceAvroMapper extends AbstractVeniceMapper<AvroWrapper<IndexedRecord>, NullWritable> {
  @Override
  public AbstractVeniceRecordReader<AvroWrapper<IndexedRecord>, NullWritable> getRecordReader(VeniceProperties props) {
    return new VeniceAvroRecordReader(props);
  }

  @Override
  protected Optional<AbstractVeniceFilter<NullWritable>> getFilter(final VeniceProperties props) {
    return Optional.empty();
  }
}
