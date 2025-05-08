package com.linkedin.venice.pubsub.adapter.kafka.common;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubPositionFactory;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * A {@link PubSubPositionFactory} for creating {@link ApacheKafkaOffsetPosition} instances.
 * <p>
 * This factory handles deserialization of positions backed by Kafka offsets.
 */
public class ApacheKafkaOffsetPositionFactory extends PubSubPositionFactory {
  public ApacheKafkaOffsetPositionFactory(int positionTypeId) {
    super(positionTypeId);
  }

  @Override
  public PubSubPosition createFromByteBuffer(ByteBuffer buffer) {
    try {
      return ApacheKafkaOffsetPosition.of(buffer);
    } catch (IOException e) {
      throw new VeniceException("Failed to deserialize Apache Kafka offset position", e);
    }
  }

  @Override
  public String getPubSubPositionClassName() {
    return ApacheKafkaOffsetPosition.class.getName();
  }
}
