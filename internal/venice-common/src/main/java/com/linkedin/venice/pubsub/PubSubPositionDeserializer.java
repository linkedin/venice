package com.linkedin.venice.pubsub;

import static com.linkedin.venice.pubsub.api.PubSubPosition.PUBSUB_POSITION_WIRE_FORMAT_SERIALIZER;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.ReflectUtils;
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
    return factory.fromWireFormat(positionWireFormat);
  }

  public PubSubPosition toPosition(byte[] positionWireFormatBytes) {
    if (positionWireFormatBytes == null) {
      throw new IllegalArgumentException("Cannot deserialize null wire format position");
    }
    return toPosition(deserializeWireFormat(positionWireFormatBytes));
  }

  public PubSubPosition toPosition(ByteBuffer positionWireFormatBytes) {
    if (positionWireFormatBytes == null) {
      throw new IllegalArgumentException("Cannot deserialize null wire format position");
    }
    return toPosition(deserializeWireFormat(positionWireFormatBytes));
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

  /**
   * Deserializes a {@link PubSubPositionWireFormat} into a {@link PubSubPosition} using the
   * provided fully-qualified {@link PubSubPositionFactory} class name.
   *
   * <p>This method is intended for use cases where the factory class name is explicitly stored
   * alongside the wire format, such as in repush checkpointing or change capture consumers.
   * It bypasses the usual deserialization path involving {@link PubSubPositionDeserializer}
   * and {@link PubSubPositionTypeRegistry}.
   *
   * <p><b>Note:</b> This method should NOT be used within Venice servers or controllers, where factory
   * mappings are dynamically registered and accessed via type ID. Use this only when the factory class name
   * is stored and reflection is the only viable deserialization strategy.
   *
   * <p>The factory class must implement {@link PubSubPositionFactory} and provide a constructor
   * with a single {@code int} argument representing the position type ID.
   *
   * @param wireFormat the wire format of the position
   * @param factoryClassName the fully-qualified class name of the factory to use for deserialization
   * @return a {@link PubSubPosition} instance deserialized from the wire format
   * @throws VeniceException if instantiation or deserialization fails
   */
  public static PubSubPosition deserializePositionUsingFactoryClassName(
      PubSubPositionWireFormat wireFormat,
      String factoryClassName) {

    try {
      Class<?> rawClass = Class.forName(factoryClassName);
      if (!PubSubPositionFactory.class.isAssignableFrom(rawClass)) {
        throw new IllegalArgumentException("Class " + factoryClassName + " is not a subtype of PubSubPositionFactory");
      }

      @SuppressWarnings("unchecked")
      Class<? extends PubSubPositionFactory> factoryClass = (Class<? extends PubSubPositionFactory>) rawClass;

      PubSubPositionFactory factory =
          ReflectUtils.callConstructor(factoryClass, new Class<?>[] { int.class }, new Object[] { wireFormat.type });

      return factory.fromWireFormat(wireFormat);
    } catch (Exception e) {
      throw new VeniceException("Failed to deserialize PubSubPosition using factory class: " + factoryClassName, e);
    }
  }

  /**
   * Deserializes a {@link PubSubPosition} using the provided wire format bytes and factory class name.
   *
   * <p>This helper is intended for use cases where the factory class name is explicitly stored alongside
   * the position wire format bytes, such as in repush checkpoints or change capture consumers.</p>
   *
   * <p>It should not be used in Venice servers or controllers where position deserialization is handled
   * via {@link PubSubPositionDeserializer} and
   * {@link PubSubPositionTypeRegistry} based on type IDs.</p>
   *
   * @param positionWireBytes the serialized wire format bytes in a {@link ByteBuffer}
   * @param factoryClassName the fully-qualified class name of the factory used to deserialize the position
   * @return the deserialized {@link PubSubPosition}
   * @throws RuntimeException if deserialization or factory instantiation fails
   */
  public static PubSubPosition deserializePubSubPosition(ByteBuffer positionWireBytes, String factoryClassName) {
    PubSubPositionWireFormat wireFormat = PubSubPositionDeserializer.deserializeWireFormat(positionWireBytes);
    return PubSubPositionDeserializer.deserializePositionUsingFactoryClassName(wireFormat, factoryClassName);
  }

  public static PubSubPosition deserializePubSubPosition(byte[] positionWireBytes, String factoryClassName) {
    PubSubPositionWireFormat wireFormat = PubSubPositionDeserializer.deserializeWireFormat(positionWireBytes);
    return PubSubPositionDeserializer.deserializePositionUsingFactoryClassName(wireFormat, factoryClassName);
  }

  public static PubSubPosition deserializePubSubPosition(PubSubPositionWireFormat wireFormat, String factoryClassName) {
    return PubSubPositionDeserializer.deserializePositionUsingFactoryClassName(wireFormat, factoryClassName);
  }

  /**
   * Deserializes a {@link PubSubPositionWireFormat} from a {@link ByteBuffer}.
   * <p>
   * This method extracts a byte array from the given {@code ByteBuffer} and uses
   * {@link PubSubPosition#PUBSUB_POSITION_WIRE_FORMAT_SERIALIZER}} to decode it into a {@code PubSubPositionWireFormat}
   * instance.
   * </p>
   *
   * <p>This is typically used when reconstructing position metadata stored in serialized wire format,
   * such as in checkpointing, audit logs, or administrative tooling.</p>
   *
   * @param positionWireFormatBytes the wire format bytes wrapped in a {@link ByteBuffer}
   * @return the deserialized {@link PubSubPositionWireFormat}
   * @throws RuntimeException if deserialization fails
   */
  public static PubSubPositionWireFormat deserializeWireFormat(ByteBuffer positionWireFormatBytes) {
    return deserializeWireFormat(ByteUtils.extractByteArray(positionWireFormatBytes));
  }

  public static PubSubPositionWireFormat deserializeWireFormat(byte[] positionWireFormatBytes) {
    return PUBSUB_POSITION_WIRE_FORMAT_SERIALIZER.deserialize(positionWireFormatBytes, null);
  }
}
