package com.linkedin.venice.hadoop.input.kafka.ttl;

import com.linkedin.venice.hadoop.input.kafka.chunk.ChunkAssembler;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.nio.ByteBuffer;


public class VeniceChunkedPayloadTTLFilter extends VeniceRmdTTLFilter<ChunkAssembler.ValueBytesAndSchemaId> {
  public VeniceChunkedPayloadTTLFilter(VeniceProperties props) throws IOException {
    super(props);
  }

  @Override
  protected int getSchemaId(ChunkAssembler.ValueBytesAndSchemaId valueBytesAndSchemaId) {
    return valueBytesAndSchemaId.getSchemaID();
  }

  @Override
  protected int getRmdId(ChunkAssembler.ValueBytesAndSchemaId valueBytesAndSchemaId) {
    return valueBytesAndSchemaId.getReplicationMetadataVersionId();
  }

  @Override
  protected ByteBuffer getRmdPayload(ChunkAssembler.ValueBytesAndSchemaId valueBytesAndSchemaId) {
    return valueBytesAndSchemaId.getReplicationMetadataPayload();
  }
}
