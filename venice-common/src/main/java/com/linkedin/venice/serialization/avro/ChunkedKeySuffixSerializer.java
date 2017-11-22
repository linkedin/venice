package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;

public class ChunkedKeySuffixSerializer extends UnversionedAvroSpecificSerializer<ChunkedKeySuffix> {
  public ChunkedKeySuffixSerializer() {
    super(ChunkedKeySuffix.class);
  }
}
