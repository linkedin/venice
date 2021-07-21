package com.linkedin.venice.replication;

import java.util.List;
import org.apache.avro.generic.GenericRecord;


class MergeList implements Merge<List<Object>> {
  @Override
  public ValueAndTimestampMetadata<List<Object>> put(ValueAndTimestampMetadata<List<Object>> oldValueAndTimestampMetadata, List<Object> newValue,
      long writeOperationTimestamp) {
    return null;
  }

  @Override
  public ValueAndTimestampMetadata<List<Object>> delete(ValueAndTimestampMetadata<List<Object>> oldValueAndTimestampMetadata,
      long writeOperationTimestamp) {
    return null;
  }

  @Override
  public ValueAndTimestampMetadata<List<Object>> update(ValueAndTimestampMetadata<List<Object>> oldValueAndTimestampMetadata,
      GenericRecord writeOperation, long writeOperationTimestamp) {
    return null;
  }
}
