package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.utils.Lazy;
import java.util.List;
import org.apache.avro.generic.GenericRecord;


class MergeList implements Merge<List<Object>> {
  private static final MergeList INSTANCE = new MergeList();

  private MergeList() {}

  static MergeList getInstance() {
    return INSTANCE;
  }

  @Override
  public ValueAndReplicationMetadata<List<Object>> put(
      ValueAndReplicationMetadata<List<Object>> oldValueAndReplicationMetadata, List<Object> newValue,
      long writeOperationTimestamp, long sourceOffsetOfNewValue, int sourceBrokerIDOfNewValue) {
    return null;
  }

  @Override
  public ValueAndReplicationMetadata<List<Object>> delete(
      ValueAndReplicationMetadata<List<Object>> oldValueAndReplicationMetadata,
      long writeOperationTimestamp, long sourceOffsetOfNewValue, int sourceBrokerIDOfNewValue) {
    return null;
  }

  @Override
  public ValueAndReplicationMetadata<List<Object>> update(
      ValueAndReplicationMetadata<List<Object>> oldValueAndReplicationMetadata,
      Lazy<GenericRecord> writeOperation, long writeOperationTimestamp, long sourceOffsetOfNewValue, int sourceBrokerIDOfNewValue) {
    return null;
  }
}
