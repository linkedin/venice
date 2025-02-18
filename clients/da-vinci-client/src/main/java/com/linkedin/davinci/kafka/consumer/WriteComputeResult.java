package com.linkedin.davinci.kafka.consumer;

import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;


/**
 * Write compute result wrapper class holding the deserialized updated value and the serialized and potentially
 * compressed updated value bytes.
 */
public class WriteComputeResult {
  private final byte[] updatedValueBytes;
  private final GenericRecord updatedValue;

  public WriteComputeResult(byte[] updatedValueBytes, GenericRecord updatedValue) {
    this.updatedValueBytes = updatedValueBytes;
    this.updatedValue = updatedValue;
  }

  @Nullable
  public byte[] getUpdatedValueBytes() {
    return updatedValueBytes;
  }

  @Nullable
  public GenericRecord getUpdatedValue() {
    return updatedValue;
  }
}
