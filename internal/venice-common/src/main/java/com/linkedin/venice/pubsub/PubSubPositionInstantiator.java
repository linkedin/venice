package com.linkedin.venice.pubsub;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import java.io.IOException;


/**
 * A class for creating PubSubPosition objects from wire format
 */
public class PubSubPositionInstantiator {
  public static final PubSubPositionInstantiator INSTANCE = new PubSubPositionInstantiator();

  /**
   * Converts a wire format position to a PubSubPosition
   * @param positionWireFormat the wire format position
   * @return concrete position object represented by the wire format
   */
  PubSubPosition convertToPosition(PubSubPositionWireFormat positionWireFormat) {
    if (positionWireFormat == null) {
      throw new IllegalArgumentException("Cannot deserialize null wire format position");
    }

    switch (positionWireFormat.type) {
      case PubSubPositionTypeRegistry.APACHE_KAFKA_OFFSET_POSITION_TYPE_ID:
        try {
          return new ApacheKafkaOffsetPosition(positionWireFormat.rawBytes);
        } catch (IOException e) {
          throw new VeniceException("Failed to deserialize Apache Kafka offset position", e);
        }
      case PubSubPositionTypeRegistry.EARLIEST_POSITION_RESERVED_TYPE_ID:
        return PubSubSymbolicPosition.EARLIEST;
      case PubSubPositionTypeRegistry.LATEST_POSITION_RESERVED_TYPE_ID:
        return PubSubSymbolicPosition.LATEST;
      default:
        throw new IllegalArgumentException(
            "Cannot convert to position. Unknown position type: " + positionWireFormat.type);
    }
  }

  PubSubPosition convertToPosition(byte[] positionWireFormatBytes) {
    if (positionWireFormatBytes == null) {
      throw new IllegalArgumentException("Cannot deserialize null wire format position");
    }
    InternalAvroSpecificSerializer<PubSubPositionWireFormat> wireFormatSerializer =
        AvroProtocolDefinition.PUBSUB_POSITION_WIRE_FORMAT.getSerializer();
    PubSubPositionWireFormat wireFormat = wireFormatSerializer.deserialize(positionWireFormatBytes, null);
    return convertToPosition(wireFormat);
  }

  public static PubSubPosition getPositionFromWireFormat(byte[] positionWireFormatBytes) {
    return INSTANCE.convertToPosition(positionWireFormatBytes);
  }

  public static PubSubPosition getPositionFromWireFormat(PubSubPositionWireFormat positionWireFormat) {
    return INSTANCE.convertToPosition(positionWireFormat);
  }
}
