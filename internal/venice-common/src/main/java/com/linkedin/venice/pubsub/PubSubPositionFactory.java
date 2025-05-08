package com.linkedin.venice.pubsub;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import java.nio.ByteBuffer;


/**
 * A factory interface for creating {@link PubSubPosition} instances and resolving their associated class names.
 * <p>
 * Each implementation is tied to a unique position type ID and is responsible for:
 * <ul>
 *   <li>Deserializing a {@link PubSubPosition} from its {@link PubSubPositionWireFormat}</li>
 *   <li>Returning the class name of the position type it handles</li>
 * </ul>
 * <p>
 * Factories are used during deserialization to map a type ID to the appropriate {@link PubSubPosition} implementation.
 */
public abstract class PubSubPositionFactory {
  private final int positionTypeId;

  /**
   * Constructs a factory with the given position type ID.
   *
   * @param positionTypeId the unique integer identifier for the position type
   */
  public PubSubPositionFactory(int positionTypeId) {
    this.positionTypeId = positionTypeId;
  }

  /**
   * Creates a {@link PubSubPosition} instance by deserializing the provided {@link PubSubPositionWireFormat}.
   * <p>
   * This method validates that the type ID in the wire format matches the expected type ID for this factory.
   * If the type ID does not match, an exception is thrown to prevent incorrect deserialization.
   * <p>
   * Internally, this delegates to {@link #createFromByteBuffer(ByteBuffer)} to perform the actual decoding.
   *
   * @param positionWireFormat the wire format containing the type ID and raw encoded bytes
   * @return a new {@link PubSubPosition} instance reconstructed from the wire format
   * @throws VeniceException if the type ID does not match the factory's expected type
   */
  public PubSubPosition createFromWireFormat(PubSubPositionWireFormat positionWireFormat) {
    if (positionWireFormat.getType() != positionTypeId) {
      throw new VeniceException(
          "Position type ID mismatch: expected " + positionTypeId + ", but got " + positionWireFormat.getType());
    }
    return createFromByteBuffer(positionWireFormat.getRawBytes());
  }

  /**
   * Deserializes a {@link PubSubPosition} from the given byte buffer.
   *
   * @param buffer the byte buffer containing the serialized position
   * @return a new {@link PubSubPosition} instance
   */
  public abstract PubSubPosition createFromByteBuffer(ByteBuffer buffer);

  /**
   * Returns the fully qualified class name of the {@link PubSubPosition} implementation handled by this factory.
   *
   * @return the fully qualified class name of the associated position class
   */
  public abstract String getPubSubPositionClassName();

  /**
   * Returns the unique position type ID associated with this factory.
   *
   * @return the integer type ID
   */
  public int getPositionTypeId() {
    return positionTypeId;
  }
}
