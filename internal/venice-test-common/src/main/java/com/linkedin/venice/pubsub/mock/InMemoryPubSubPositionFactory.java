package com.linkedin.venice.pubsub.mock;

import static com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition.*;

import com.linkedin.venice.pubsub.PubSubPositionFactory;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.nio.ByteBuffer;


public class InMemoryPubSubPositionFactory extends PubSubPositionFactory {
  /**
   * Constructs a factory with the given position type ID.
   *
   * @param positionTypeId the unique integer identifier for the position type
   */
  public InMemoryPubSubPositionFactory(int positionTypeId) {
    super(positionTypeId);
  }

  @Override
  public PubSubPosition fromPositionRawBytes(ByteBuffer buffer) {
    return InMemoryPubSubPosition.of(buffer);
  }

  @Override
  public String getPubSubPositionClassName() {
    return InMemoryPubSubPosition.class.getName();
  }

  public static PubSubPositionTypeRegistry getPositionTypeRegistryWithInMemoryPosition() {
    Int2ObjectMap<String> typeIdToFactory = new Int2ObjectOpenHashMap<>(1);
    typeIdToFactory.put(INMEMORY_PUBSUB_POSITION_TYPE_ID, InMemoryPubSubPositionFactory.class.getName());
    return new PubSubPositionTypeRegistry(typeIdToFactory);
  }
}
