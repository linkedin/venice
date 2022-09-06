package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;


public class ChunkedValueManifestSerializer extends InternalAvroSpecificSerializer<ChunkedValueManifest> {
  /**
   * @param withoutHeader true: this will encode only the raw data, with no header
   *                      false: this will deserialize data assuming the presence of a 4 bytes header and serialize it with 4 bytes of padding
   */
  public ChunkedValueManifestSerializer(boolean withoutHeader) {
    super(AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST, withoutHeader ? null : ByteUtils.SIZE_OF_INT);
  }
}
