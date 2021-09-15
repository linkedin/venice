package com.linkedin.venice.writer;

import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.kafka.clients.producer.Callback;


/**
 * This is a simple container class to hold replication metadata related fields together to be passed on to the Delete api in VeniceWriter
 * {@link VeniceWriter#delete(Object, Callback, LeaderMetadataWrapper, long, Optional)}. Caller should construct an instance of this object by properly
 * filling up all the fields of this object.
 */
public class DeleteMetadata {
  private final int valueSchemaId;
  private final int replicationMetadataVersionId;
  private final ByteBuffer replicationMetadataPayload;

  public DeleteMetadata(int valueSchemaId, int replicationMetadataVersionId, ByteBuffer replicationMetadataPayload) {
    this.valueSchemaId = valueSchemaId;
    this.replicationMetadataVersionId = replicationMetadataVersionId;
    this.replicationMetadataPayload = replicationMetadataPayload;
  }

  public int getValueSchemaId() {
    return valueSchemaId;
  }

  public int getReplicationMetadataVersionId() {
    return replicationMetadataVersionId;
  }

  public ByteBuffer getReplicationMetadataPayload() {
    return replicationMetadataPayload;
  }

  public int getSerializedSize() {
    return 2 * ByteUtils.SIZE_OF_INT + replicationMetadataPayload.remaining();
  }
}
