package com.linkedin.venice.replication;

import java.util.Map;
import org.apache.avro.generic.GenericRecord;


class MergeMap implements Merge<Map<CharSequence, Object>> {
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
      GenericRecord writeOperation, long writeOperationTimestamp) {
    return null;
  }
}
