package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;


public class ChunkedValueManifestSerializer extends InternalAvroSpecificSerializer<ChunkedValueManifest> {
  /**
   * @param writePath When used from the write path (true), this will encode only the raw data, with no header
   *                  When used from the read path (false), this will deserialize data assuming the presence of a four bytes header
   */
  public ChunkedValueManifestSerializer(boolean writePath) {
    super(AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST, writePath ? null : ByteUtils.SIZE_OF_INT);
  }
}
