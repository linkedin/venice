package com.linkedin.venice.writer;

import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.kafka.clients.producer.Callback;


/**
 * This is a simple container class to hold replication metadata related fields together to be passed on to the Put api in VeniceWriter
 * {@link VeniceWriter#put(Object, Object, int, Callback, LeaderMetadataWrapper, long, Optional)}. Caller should construct an instance of this object by properly
 * filling up all the fields of this object.
 */
public class PutMetadata {
  private final int rmdVersionId;
  private final ByteBuffer rmdPayload;

  public PutMetadata(int rmdVersionId, ByteBuffer rmdPayload) {
    this.rmdVersionId = rmdVersionId;
    this.rmdPayload = rmdPayload;
  }

  public int getRmdVersionId() {
    return rmdVersionId;
  }

  public ByteBuffer getRmdPayload() {
    return rmdPayload;
  }

  public int getSerializedSize() {
    return ByteUtils.SIZE_OF_INT + rmdPayload.remaining();
  }
}
