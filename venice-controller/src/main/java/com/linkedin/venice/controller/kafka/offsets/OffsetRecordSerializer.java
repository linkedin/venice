package com.linkedin.venice.controller.kafka.offsets;

import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.offsets.OffsetRecord;

import java.io.IOException;

public class OffsetRecordSerializer implements VeniceSerializer<OffsetRecord> {
  @Override
  public byte[] serialize(OffsetRecord object, String path) throws IOException {
    return object.toBytes();
  }

  @Override
  public OffsetRecord deserialize(byte[] bytes, String path) throws IOException {
    return new OffsetRecord(bytes);
  }
}
