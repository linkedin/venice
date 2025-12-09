package com.linkedin.venice.hadoop.input.kafka.ttl;

import com.linkedin.venice.common.VeniceRmdTTLFilter;
import com.linkedin.venice.hadoop.input.kafka.chunk.ChunkAssembler;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * This class is responsible to filter records based on the RMD information and the ttl config by taking chunked records {@link ChunkAssembler.ValueBytesAndSchemaId}.
 */
public class VeniceChunkedPayloadTTLFilter extends VeniceRmdTTLFilter<ChunkAssembler.ValueBytesAndSchemaId> {
  public VeniceChunkedPayloadTTLFilter(VeniceProperties props) throws IOException {
    super(props);
  }

  @Override
  protected int getSchemaId(ChunkAssembler.ValueBytesAndSchemaId valueBytesAndSchemaId) {
    return valueBytesAndSchemaId.getSchemaID();
  }

  @Override
  protected int getRmdProtocolId(ChunkAssembler.ValueBytesAndSchemaId valueBytesAndSchemaId) {
    return valueBytesAndSchemaId.getReplicationMetadataVersionId();
  }

  @Override
  protected ByteBuffer getRmdPayload(ChunkAssembler.ValueBytesAndSchemaId valueBytesAndSchemaId) {
    return valueBytesAndSchemaId.getReplicationMetadataPayload();
  }

  @Override
  protected ByteBuffer getValuePayload(ChunkAssembler.ValueBytesAndSchemaId valueBytesAndSchemaId) {
    byte[] valueBytes = valueBytesAndSchemaId.getBytes();
    return valueBytes == null ? null : ByteBuffer.wrap(valueBytes);
  }

  @Override
  protected void updateRmdPayload(ChunkAssembler.ValueBytesAndSchemaId valueBytesAndSchemaId, ByteBuffer payload) {
    valueBytesAndSchemaId.setReplicationMetadataPayload(payload);
  }

  @Override
  protected void updateValuePayload(ChunkAssembler.ValueBytesAndSchemaId valueBytesAndSchemaId, byte[] payload) {
    valueBytesAndSchemaId.setBytes(payload);
  }
}
