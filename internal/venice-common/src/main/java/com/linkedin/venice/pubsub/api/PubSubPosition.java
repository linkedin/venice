package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.annotation.RestrictedApi;
import com.linkedin.venice.memory.Measurable;
import com.linkedin.venice.pubsub.PubSubPositionFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import java.nio.ByteBuffer;


/**
 * Represents a position of a message in a partition of a topic.
 */
public interface PubSubPosition extends Measurable {
  InternalAvroSpecificSerializer<PubSubPositionWireFormat> PUBSUB_POSITION_WIRE_FORMAT_SERIALIZER =
      AvroProtocolDefinition.PUBSUB_POSITION_WIRE_FORMAT.getSerializer();

  @RestrictedApi("This API facilitates the transition from numeric offsets to PubSubPosition. "
      + "It should be removed once the codebase fully adopts PubSubPosition.")
  long getNumericOffset();

  boolean equals(Object obj);

  int hashCode();

  default boolean isSymbolic() {
    return false;
  }

  /**
   * Position wrapper is used to wrap the position type and the position value.
   * This is used to serialize and deserialize the position object when sending and receiving it over the wire.
   * @return the position wrapper
   */
  PubSubPositionWireFormat getPositionWireFormat();

  /**
   * Returns the serialized wire format bytes of this position.
   * @return byte array representing the position in wire format
   */
  default ByteBuffer toWireFormatBuffer() {
    return PUBSUB_POSITION_WIRE_FORMAT_SERIALIZER.serialize(getPositionWireFormat());
  }

  /**
   * Serializes this position to a wire format and returns the result as a byte array.
   * This method bypasses ByteBuffer and directly returns the backing array.
   *
   * @return a byte array representing the serialized wire format of this position
   */
  default byte[] toWireFormatBytes() {
    return PUBSUB_POSITION_WIRE_FORMAT_SERIALIZER.serialize(null, getPositionWireFormat());
  }

  /**
   * Returns the factory class responsible for creating this position type.
   *
   * This enables strong type checking and reflection-based instantiation without
   * requiring the actual factory instance to be returned.
   *
   * @return the class object of the factory that can construct this position
   */
  Class<? extends PubSubPositionFactory> getFactoryClass();

  default String getFactoryClassName() {
    return getFactoryClass().getName();
  }
}
