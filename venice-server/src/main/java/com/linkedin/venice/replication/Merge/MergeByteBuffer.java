package com.linkedin.venice.replication.merge;

import com.linkedin.venice.replication.Merge;
import com.linkedin.venice.replication.ValueAndTimestampMetadata;
import com.linkedin.venice.utils.Lazy;
import java.nio.ByteBuffer;
import org.apache.avro.generic.GenericRecord;

/**
 * The workflow is
 *
 * Query old TSMD. If it's null (and running in first batch push merge policy), then write the new value directly.
 * If the old TSMD exists, then deserialize it and run Merge<BB>.
 * If the incoming TS is higher than the entirety of the old TSMD, then write the new value directly.
 * If the incoming TS is lower than the entirety of the old TSMD, then drop the new value.
 * If the incoming TS is partially higher, partially lower, than the old TSMD, then query the old value, deserialize it, and pass it to Merge<GR>, Merge<Map> or Merge<List> .
 */
public class MergeByteBuffer implements Merge<Lazy<ByteBuffer>> {
  @Override
  public ValueAndTimestampMetadata<Lazy<ByteBuffer>> put(ValueAndTimestampMetadata<Lazy<ByteBuffer>> oldValueAndTimestampMetadata,
      Lazy<ByteBuffer> newValue, long writeOperationTimestamp) {
    return null;
  }

  @Override
  public ValueAndTimestampMetadata<Lazy<ByteBuffer>> delete(ValueAndTimestampMetadata<Lazy<ByteBuffer>> oldValueAndTimestampMetadata,
      long writeOperationTimestamp) {
    return null;
  }

  @Override
  public ValueAndTimestampMetadata<Lazy<ByteBuffer>> update(ValueAndTimestampMetadata<Lazy<ByteBuffer>> oldValueAndTimestampMetadata,
      GenericRecord writeOperation, long writeOperationTimestamp) {
    return null;
  }
}
