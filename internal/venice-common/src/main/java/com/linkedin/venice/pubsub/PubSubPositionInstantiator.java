package com.linkedin.venice.pubsub;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A utility for converting serialized {@link PubSubPositionWireFormat} objects into concrete {@link PubSubPosition}
 * instances using the configured {@link PubSubPositionTypeRegistry}.
 *
 * <p>This class provides static convenience methods for deserializing positions in scenarios
 * where direct injection of the registry is not available. For production use, callers should prefer
 * explicitly passing their own {@link PubSubPositionTypeRegistry} instance.</p>
 *
 * <p>The deserialization process involves looking up the type ID from the wire format using the registry
 * and invoking the corresponding {@link PubSubPositionFactory} to construct the position instance.</p>
 */
public class PubSubPositionInstantiator {
  private static final Logger LOGGER = LogManager.getLogger(PubSubPositionInstantiator.class);
  /**
   * Note: The following default instance is only for convenience purposes until we've updated all the code to use
   * pass the registry and resolver explicitly.
   */
  public static final PubSubPositionInstantiator INSTANCE =
      new PubSubPositionInstantiator(PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY);

  private final PubSubPositionTypeRegistry pubSubPositionTypeRegistry;

  private PubSubPositionInstantiator(PubSubPositionTypeRegistry pubSubPositionTypeRegistry) {
    this.pubSubPositionTypeRegistry = pubSubPositionTypeRegistry;
  }

  /**
   * Converts a wire format position to a PubSubPosition
   * @param positionWireFormat the wire format position
   * @return concrete position object represented by the wire format
   */
  PubSubPosition convertToPosition(PubSubPositionWireFormat positionWireFormat) {
    if (positionWireFormat == null) {
      throw new IllegalArgumentException("Cannot deserialize null wire format position");
    }
    // look up the type ID in the registry
    int typeId = positionWireFormat.type;
    PubSubPositionFactory factory = pubSubPositionTypeRegistry.getFactoryByTypeId(typeId);
    if (factory == null) {
      LOGGER.error(
          "Cannot convert to position. Unknown position type: {}. Available types: {}",
          typeId,
          pubSubPositionTypeRegistry.getAllTypeIdToFactoryClassNameMappings());
      throw new VeniceException("Cannot convert to position. Unknown position type: " + typeId);
    }
    // deserialize the position using the factory
    return factory.createFromWireFormat(positionWireFormat);
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
