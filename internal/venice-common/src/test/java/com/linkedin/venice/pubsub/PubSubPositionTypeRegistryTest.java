package com.linkedin.venice.pubsub;

import static com.linkedin.venice.pubsub.PubSubPositionTypeRegistry.APACHE_KAFKA_OFFSET_POSITION_TYPE_ID;
import static com.linkedin.venice.pubsub.PubSubPositionTypeRegistry.EARLIEST_POSITION_RESERVED_TYPE_ID;
import static com.linkedin.venice.pubsub.PubSubPositionTypeRegistry.LATEST_POSITION_RESERVED_TYPE_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.utils.VeniceProperties;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.Properties;
import org.testng.annotations.Test;


public class PubSubPositionTypeRegistryTest {
  @Test
  public void testRegistryWithDefaultReservedTypes() {
    PubSubPositionTypeRegistry registry = PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY;

    assertEquals(
        registry.getClassName(EARLIEST_POSITION_RESERVED_TYPE_ID),
        PubSubSymbolicPosition.EARLIEST.getClass().getName());
    assertEquals(
        registry.getClassName(LATEST_POSITION_RESERVED_TYPE_ID),
        PubSubSymbolicPosition.LATEST.getClass().getName());
    assertEquals(
        registry.getClassName(APACHE_KAFKA_OFFSET_POSITION_TYPE_ID),
        ApacheKafkaOffsetPosition.class.getName());

    assertEquals(registry.getTypeId(PubSubSymbolicPosition.EARLIEST), EARLIEST_POSITION_RESERVED_TYPE_ID);
    assertEquals(registry.getTypeId(PubSubSymbolicPosition.LATEST), LATEST_POSITION_RESERVED_TYPE_ID);
    assertTrue(registry.hasType(ApacheKafkaOffsetPosition.class.getName()));
    PubSubPosition kafkaOffsetPosition = new ApacheKafkaOffsetPosition(0);
    assertEquals(registry.getTypeId(kafkaOffsetPosition), APACHE_KAFKA_OFFSET_POSITION_TYPE_ID);
  }

  @Test
  public void testRegistryMergesUserTypesWithReservedTypes() {
    Int2ObjectMap<String> userMap = new Int2ObjectOpenHashMap<>();
    userMap.put(99, DummyPubSubPosition.class.getName());

    PubSubPositionTypeRegistry registry = new PubSubPositionTypeRegistry(userMap);

    assertEquals(registry.getClassName(99), DummyPubSubPosition.class.getName());
    assertEquals(registry.getTypeId(DummyPubSubPosition.class.getName()), 99);

    // check each entry in reserved map is in the merged map
    Int2ObjectMap<String> typeIdToClassNameMap = registry.getTypeIdToClassNameMap();
    PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY.getTypeIdToClassNameMap()
        .forEach((typeId, className) -> {
          if (typeId != 99) {
            assertEquals(
                typeIdToClassNameMap.get((int) typeId),
                className,
                "Type ID: " + typeId + " not found in merged map");
          }
        });
  }

  @Test
  public void testRegistryRejectsConflictingReservedOverride() {
    Int2ObjectMap<String> userMap = new Int2ObjectOpenHashMap<>();
    userMap.put(EARLIEST_POSITION_RESERVED_TYPE_ID, DummyPubSubPosition.class.getName());

    Exception exception = expectThrows(IllegalArgumentException.class, () -> new PubSubPositionTypeRegistry(userMap));
    assertTrue(exception.getMessage().contains("Conflicting entry for reserved position type ID"));
  }

  @Test
  public void testRegistryRejectsUnknownClassName() {
    Int2ObjectMap<String> userMap = new Int2ObjectOpenHashMap<>();
    userMap.put(99, "com.example.NonExistentPositionClass");

    Exception exception = expectThrows(IllegalArgumentException.class, () -> new PubSubPositionTypeRegistry(userMap));
    assertTrue(exception.getMessage().contains("Class not found for fully qualified name"));
  }

  @Test
  public void testRegistryRejectsNullClassName() {
    Int2ObjectMap<String> userMap = new Int2ObjectOpenHashMap<>();
    userMap.put(100, null);

    Exception exception = expectThrows(IllegalArgumentException.class, () -> new PubSubPositionTypeRegistry(userMap));
    assertTrue(exception.getMessage().contains("null or empty"));
  }

  @Test
  public void testGetRegistryFromProperties() {
    Properties props = new Properties();
    props.put(ConfigKeys.PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP, "101:" + DummyPubSubPosition.class.getName());
    VeniceProperties properties = new VeniceProperties(props);

    PubSubPositionTypeRegistry registry = PubSubPositionTypeRegistry.getRegistryFromPropertiesOrDefault(properties);

    assertEquals(registry.getClassName(101), DummyPubSubPosition.class.getName());
    assertEquals(registry.getTypeId(DummyPubSubPosition.class.getName()), 101);

    // Case: No user-defined types
    assertEquals(
        PubSubPositionTypeRegistry.getRegistryFromPropertiesOrDefault(new VeniceProperties(new Properties())),
        PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY);
  }

  // Dummy implementation for test use only
  public static class DummyPubSubPosition implements PubSubPosition {
    @Override
    public int getHeapSize() {
      return 0;
    }

    @Override
    public int comparePosition(PubSubPosition other) {
      return 0;
    }

    @Override
    public long diff(PubSubPosition other) {
      return 0;
    }

    @Override
    public PubSubPositionWireFormat getPositionWireFormat() {
      return null;
    }

    @Override
    public long getNumericOffset() {
      return 0;
    }
  }

  // tests two uniq ids for the same user provided types are not allowed
  @Test
  public void testRegistryRejectsDuplicateUserTypes() {
    Int2ObjectMap<String> userMap = new Int2ObjectOpenHashMap<>();
    userMap.put(99, DummyPubSubPosition.class.getName());
    userMap.put(100, DummyPubSubPosition.class.getName());

    Exception exception = expectThrows(IllegalArgumentException.class, () -> new PubSubPositionTypeRegistry(userMap));
    assertTrue(
        exception.getMessage().contains(" is already mapped to a different type ID."),
        "Got: " + exception.getMessage());
  }

  @Test
  public void testGetTypeIdFailsForUnknownClass() {
    PubSubPositionTypeRegistry registry = PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY;

    Exception exception =
        expectThrows(IllegalArgumentException.class, () -> registry.getTypeId("com.unknown.ClassName"));
    assertTrue(exception.getMessage().contains("class name not found"));
  }

  @Test
  public void testGetTypeIdFailsForNullPosition() {
    PubSubPositionTypeRegistry registry = PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY;

    Exception exception = expectThrows(IllegalArgumentException.class, () -> registry.getTypeId((PubSubPosition) null));
    assertTrue(exception.getMessage().contains("position is null"));
  }

  @Test
  public void testGetClassNameFailsForInvalidTypeId() {
    PubSubPositionTypeRegistry registry = PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY;

    Exception exception = expectThrows(IllegalArgumentException.class, () -> registry.getClassName(12345));
    assertTrue(exception.getMessage().contains("type ID not found"));
  }
}
