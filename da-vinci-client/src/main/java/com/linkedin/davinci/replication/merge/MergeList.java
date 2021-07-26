package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.utils.Lazy;
import java.util.List;
import org.apache.avro.generic.GenericRecord;


class MergeList implements Merge<List<Object>> {
  private static final MergeList INSTANCE = new MergeList();

  private MergeList() {}

  static MergeList getMergeList() {
    return INSTANCE;
  }

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
      Lazy<GenericRecord> writeOperation, long writeOperationTimestamp) {
    return null;
  }
}
