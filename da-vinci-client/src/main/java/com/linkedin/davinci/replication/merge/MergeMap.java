package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.utils.Lazy;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


class MergeMap implements Merge<Map<CharSequence, Object>> {
  private static final MergeMap INSTANCE = new MergeMap();

  private MergeMap() {}

  static MergeMap getInstance() {
    return INSTANCE;
  }

  @Override
  public ValueAndReplicationMetadata<Map<CharSequence, Object>> put(
      ValueAndReplicationMetadata<Map<CharSequence, Object>> oldValueAndReplicationMetadata,
      Map<CharSequence, Object> newValue, long writeOperationTimestamp, long sourceOffsetOfNewValue, int sourceBrokerIDOfNewValue) {
    return null;
  }

  @Override
  public ValueAndReplicationMetadata<Map<CharSequence, Object>> delete(
      ValueAndReplicationMetadata<Map<CharSequence, Object>> oldValueAndReplicationMetadata,
      long writeOperationTimestamp, long sourceOffsetOfNewValue, int sourceBrokerIDOfNewValue) {
    return null;
  }

  @Override
  public ValueAndReplicationMetadata<Map<CharSequence, Object>> update(
      ValueAndReplicationMetadata<Map<CharSequence, Object>> oldValueAndReplicationMetadata,
      Lazy<GenericRecord> writeOperation,
      Schema valueSchema,
      Schema writeComputeSchema,
      long writeOperationTimestamp,
      long sourceOffsetOfNewValue,
      int sourceBrokerIDOfNewValue
  ) {
    return null;
  }
}
