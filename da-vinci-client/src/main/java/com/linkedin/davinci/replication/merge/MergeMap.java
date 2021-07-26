package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.utils.Lazy;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;


class MergeMap implements Merge<Map<CharSequence, Object>> {
  private static final MergeMap INSTANCE = new MergeMap();

  private MergeMap() {}

  static MergeMap getMergeMap() {
    return INSTANCE;
  }

  @Override
  public ValueAndTimestampMetadata<Map<CharSequence, Object>> put(ValueAndTimestampMetadata<Map<CharSequence, Object>> oldValueAndTimestampMetadata,
      Map<CharSequence, Object> newValue, long writeOperationTimestamp) {
    return null;
  }

  @Override
  public ValueAndTimestampMetadata<Map<CharSequence, Object>> delete(ValueAndTimestampMetadata<Map<CharSequence, Object>> oldValueAndTimestampMetadata,
      long writeOperationTimestamp) {
    return null;
  }

  @Override
  public ValueAndTimestampMetadata<Map<CharSequence, Object>> update(ValueAndTimestampMetadata<Map<CharSequence, Object>> oldValueAndTimestampMetadata,
      Lazy<GenericRecord> writeOperation, long writeOperationTimestamp) {
    return null;
  }
}
