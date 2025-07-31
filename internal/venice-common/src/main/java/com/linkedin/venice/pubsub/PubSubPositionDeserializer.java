package com.linkedin.venice.pubsub;

import static com.linkedin.venice.pubsub.api.PubSubPosition.PUBSUB_POSITION_WIRE_FORMAT_SERIALIZER;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
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
  public PubSubPosition toPosition(PubSubPositionWireFormat positionWireFormat) {
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

  public PubSubPosition toPosition(byte[] positionWireFormatBytes) {
    if (positionWireFormatBytes == null) {
      throw new IllegalArgumentException("Cannot deserialize null wire format position");
    }
    PubSubPositionWireFormat wireFormat =
        PUBSUB_POSITION_WIRE_FORMAT_SERIALIZER.deserialize(positionWireFormatBytes, null);
    return toPosition(wireFormat);
  }

  public PubSubPosition toPosition(ByteBuffer positionWireFormatBytes) {
    if (positionWireFormatBytes == null) {
      throw new IllegalArgumentException("Cannot deserialize null wire format position");
    }
    PubSubPositionWireFormat wireFormat =
        PUBSUB_POSITION_WIRE_FORMAT_SERIALIZER.deserialize(ByteUtils.extractByteArray(positionWireFormatBytes), null);
    return toPosition(wireFormat);
  }

  /**
   * Convenience method for converting a serialized byte array representing a
   * {@link PubSubPositionWireFormat} into a concrete {@link PubSubPosition} instance.
   *
   * <p>This uses the {@link #DEFAULT_DESERIALIZER} with the reserved position type registry.
   * Recommended only for use in non-critical paths or tests where custom registries are not required.</p>
   *
   * @param positionWireFormatBytes the serialized bytes of {@link PubSubPositionWireFormat}
   * @return deserialized {@link PubSubPosition} object
   * @throws VeniceException if deserialization fails or type ID is unrecognized
   */
  public static PubSubPosition getPositionFromWireFormat(byte[] positionWireFormatBytes) {
    return DEFAULT_DESERIALIZER.toPosition(positionWireFormatBytes);
  }

  public static PubSubPosition getPositionFromWireFormat(ByteBuffer positionWireFormatBuffer) {
    return DEFAULT_DESERIALIZER.toPosition(positionWireFormatBuffer);
  }

  /**
   * Convenience method for converting a {@link PubSubPositionWireFormat} record into a concrete {@link PubSubPosition}.
   *
   * <p>This uses the {@link #DEFAULT_DESERIALIZER} with the reserved position type registry.
   * Prefer constructing your own {@link PubSubPositionDeserializer} with a custom registry if needed.</p>
   *
   * @param positionWireFormat the wire format record to convert
   * @return deserialized {@link PubSubPosition} object
   * @throws VeniceException if the type ID in the wire format is unrecognized
   */
  public static PubSubPosition getPositionFromWireFormat(PubSubPositionWireFormat positionWireFormat) {
    return DEFAULT_DESERIALIZER.toPosition(positionWireFormat);
  }
}
