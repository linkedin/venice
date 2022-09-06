package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;


public class ChunkedKeySuffixSerializer extends InternalAvroSpecificSerializer<ChunkedKeySuffix> {
  public ChunkedKeySuffixSerializer() {
    super(AvroProtocolDefinition.CHUNKED_KEY_SUFFIX);
  }
}
