package com.linkedin.venice.hadoop.input.kafka.ttl;

import com.linkedin.venice.hadoop.input.kafka.chunk.ChunkAssembler;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * This class is responsible to filter records based on the RMD information and the ttl config by taking chunked records {@link ChunkAssembler.ChunkedValueWithMetadata}.
 */
public class VeniceChunkedPayloadTTLFilter extends VeniceRmdTTLFilter<ChunkAssembler.ChunkedValueWithMetadata> {
  public VeniceChunkedPayloadTTLFilter(VeniceProperties props) throws IOException {
    super(props);
  }

  @Override
  protected int getSchemaId(ChunkAssembler.ChunkedValueWithMetadata chunkedValueWithMetadata) {
    return chunkedValueWithMetadata.getSchemaID();
  }

  @Override
  protected int getRmdProtocolId(ChunkAssembler.ChunkedValueWithMetadata chunkedValueWithMetadata) {
    return chunkedValueWithMetadata.getReplicationMetadataVersionId();
  }

  @Override
  protected ByteBuffer getRmdPayload(ChunkAssembler.ChunkedValueWithMetadata chunkedValueWithMetadata) {
    return chunkedValueWithMetadata.getReplicationMetadataPayload();
  }

  @Override
  protected ByteBuffer getValuePayload(ChunkAssembler.ChunkedValueWithMetadata chunkedValueWithMetadata) {
    byte[] valueBytes = chunkedValueWithMetadata.getBytes();
    return valueBytes == null ? null : ByteBuffer.wrap(valueBytes);
  }

  @Override
  protected void updateRmdPayload(
      ChunkAssembler.ChunkedValueWithMetadata chunkedValueWithMetadata,
      ByteBuffer payload) {
    chunkedValueWithMetadata.setReplicationMetadataPayload(payload);
  }

  @Override
  protected void updateValuePayload(ChunkAssembler.ChunkedValueWithMetadata chunkedValueWithMetadata, byte[] payload) {
    chunkedValueWithMetadata.setBytes(payload);
  }

  @Override
  protected long getLogicalTimestamp(ChunkAssembler.ChunkedValueWithMetadata chunkedValueWithMetadata) {
    return chunkedValueWithMetadata.getLogicalTimestamp();
  }
}
