package com.linkedin.venice.writer;

import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.kafka.clients.producer.Callback;


/**
 * This is a simple container class to hold timestamp metadata related fields together to be passed on to the Put api in VeniceWriter
 * {@link VeniceWriter#put(Object, Object, int, Callback, LeaderMetadataWrapper, long, Optional)}. Caller should construct an instance of this object by properly
 * filling up all the fields of this object.
 */
public class PutMetadata {
  private final int timestampMetadataVersionId;
  private final ByteBuffer timestampMetadataPayload;

  public PutMetadata(int timestampMetadataVersionId, ByteBuffer timestampMetadataPayload) {
    this.timestampMetadataVersionId = timestampMetadataVersionId;
    this.timestampMetadataPayload = timestampMetadataPayload;
  }

  public int getTimestampMetadataVersionId() {
    return timestampMetadataVersionId;
  }

  public ByteBuffer getTimestampMetadataPayload() {
    return timestampMetadataPayload;
  }

  public int getSerializedSize() {
    return ByteUtils.SIZE_OF_INT + timestampMetadataPayload.remaining();
  }
}
