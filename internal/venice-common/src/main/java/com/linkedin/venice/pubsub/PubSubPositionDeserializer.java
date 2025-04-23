package com.linkedin.venice.pubsub;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Utility for converting serialized {@link PubSubPositionWireFormat} data into concrete {@link PubSubPosition}
 * instances using a configured {@link PubSubPositionTypeRegistry}.
 *
 * <p>This class offers static access to a default deserializer instance backed by the reserved registry.
 * In most production cases, callers are encouraged to instantiate their own deserializer with a custom
 * registry instead of relying on the default static entry point.</p>
 *
 * <p>Deserialization involves reading the type ID from the wire format and delegating to the corresponding
 * {@link PubSubPositionFactory} to produce the appropriate position implementation.</p>
 */
public class PubSubPositionDeserializer {
  private static final Logger LOGGER = LogManager.getLogger(PubSubPositionDeserializer.class);
  /**
   * Note: The following default instance is only for convenience purposes until we've updated all the code to use
   * pass the registry and resolver explicitly.
   */
  public static final PubSubPositionDeserializer DEFAULT_DESERIALIZER =
      new PubSubPositionDeserializer(PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY);

  private final PubSubPositionTypeRegistry pubSubPositionTypeRegistry;

  public PubSubPositionDeserializer(PubSubPositionTypeRegistry registry) {
    this.pubSubPositionTypeRegistry = registry;
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
    return DEFAULT_DESERIALIZER.convertToPosition(positionWireFormatBytes);
  }

  public static PubSubPosition getPositionFromWireFormat(PubSubPositionWireFormat positionWireFormat) {
    return DEFAULT_DESERIALIZER.convertToPosition(positionWireFormat);
  }
}
