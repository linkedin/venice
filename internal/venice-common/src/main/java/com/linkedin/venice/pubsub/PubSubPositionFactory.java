package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;


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
   * Deserializes a {@link PubSubPosition} from the given wire format.
   *
   * @param positionWireFormat the serialized wire format representation
   * @return a new {@link PubSubPosition} instance
   */
  public abstract PubSubPosition fromWireFormat(PubSubPositionWireFormat positionWireFormat);

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
