package com.linkedin.venice.writer;

import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.kafka.clients.producer.Callback;


/**
 * This is a simple container class to hold timestamp metadata related fields together to be passed on to the Delete api in VeniceWriter
 * {@link VeniceWriter#delete(Object, Callback, long, long, Optional)}. Caller should construct an instance of this object by properly
 * filling up all the fields of this object.
 */
public class DeleteMetadata {
  private final int valueSchemaId;
  private final int timestampMetadataVersionId;
  private final ByteBuffer timestampMetadataPayload;

  public DeleteMetadata(int valueSchemaId, int timestampMetadataVersionId, ByteBuffer timestampMetadataPayload) {
    this.valueSchemaId = valueSchemaId;
    this.timestampMetadataVersionId = timestampMetadataVersionId;
    this.timestampMetadataPayload = timestampMetadataPayload;
  }

  public int getValueSchemaId() {
    return valueSchemaId;
  }

  public int getTimestampMetadataVersionId() {
    return timestampMetadataVersionId;
  }

  public ByteBuffer getTimestampMetadataPayload() {
    return timestampMetadataPayload;
  }
}
