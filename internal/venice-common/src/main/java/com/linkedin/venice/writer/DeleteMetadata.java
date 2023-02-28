package com.linkedin.venice.writer;

import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.Optional;


/**
 * This is a simple container class to hold replication metadata related fields together to be passed on to the Delete api in VeniceWriter
 * {@link VeniceWriter#delete(Object, Callback, LeaderMetadataWrapper, long, Optional)}. Caller should construct an instance of this object by properly
 * filling up all the fields of this object.
 */
public class DeleteMetadata {
  private final int valueSchemaId;
  private final int rmdVersionId;
  private final ByteBuffer rmdPayload;

  public DeleteMetadata(int valueSchemaId, int rmdVersionId, ByteBuffer rmdPayload) {
    this.valueSchemaId = valueSchemaId;
    this.rmdVersionId = rmdVersionId;
    this.rmdPayload = rmdPayload;
  }

  public int getValueSchemaId() {
    return valueSchemaId;
  }

  public int getRmdVersionId() {
    return rmdVersionId;
  }

  public ByteBuffer getRmdPayload() {
    return rmdPayload;
  }

  public int getSerializedSize() {
    return 2 * ByteUtils.SIZE_OF_INT + rmdPayload.remaining();
  }
}
