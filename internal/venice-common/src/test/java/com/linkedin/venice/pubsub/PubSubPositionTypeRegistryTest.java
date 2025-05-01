package com.linkedin.venice.pubsub;

import static com.linkedin.venice.pubsub.PubSubPositionTypeRegistry.APACHE_KAFKA_OFFSET_POSITION_TYPE_ID;
import static com.linkedin.venice.pubsub.PubSubPositionTypeRegistry.EARLIEST_POSITION_RESERVED_TYPE_ID;
import static com.linkedin.venice.pubsub.PubSubPositionTypeRegistry.LATEST_POSITION_RESERVED_TYPE_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPositionFactory;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.VeniceProperties;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.nio.ByteBuffer;
import java.util.Properties;
import org.testng.annotations.Test;


public class PubSubPositionTypeRegistryTest {
  @Test
  public void testRegistryWithDefaultReservedTypes() {
    PubSubPositionTypeRegistry registry = PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY;

    assertEquals(
        registry.getFactoryByTypeId(EARLIEST_POSITION_RESERVED_TYPE_ID).getClass(),
        EarliestPositionFactory.class);
    assertEquals(registry.getFactoryByTypeId(LATEST_POSITION_RESERVED_TYPE_ID).getClass(), LatestPositionFactory.class);
    assertEquals(
        registry.getFactoryByTypeId(APACHE_KAFKA_OFFSET_POSITION_TYPE_ID).getClass(),
        ApacheKafkaOffsetPositionFactory.class);

    assertEquals(
        registry.getTypeIdForFactoryClass(EarliestPositionFactory.class.getName()),
        EARLIEST_POSITION_RESERVED_TYPE_ID);
    assertEquals(
        registry.getTypeIdForFactoryClass(LatestPositionFactory.class.getName()),
        LATEST_POSITION_RESERVED_TYPE_ID);
    assertTrue(registry.containsFactoryClass(ApacheKafkaOffsetPositionFactory.class.getName()));
    assertEquals(
        registry
            .getTypeIdForFactoryInstance(new ApacheKafkaOffsetPositionFactory(APACHE_KAFKA_OFFSET_POSITION_TYPE_ID)),
        APACHE_KAFKA_OFFSET_POSITION_TYPE_ID);
  }

  @Test
  public void testRegistryMergesUserTypesWithReservedTypes() {
    Int2ObjectMap<String> userMap = new Int2ObjectOpenHashMap<>();
    userMap.put(99, DummyPubSubPositionFactory.class.getName());

    PubSubPositionTypeRegistry registry = new PubSubPositionTypeRegistry(userMap);

    assertEquals(registry.getFactoryByTypeId(99).getClass(), DummyPubSubPositionFactory.class);
    assertEquals(registry.getTypeIdForFactoryClass(DummyPubSubPositionFactory.class.getName()), 99);

    // check each entry in reserved map is in the merged map
    Int2ObjectMap<String> typeIdToClassNameMap = registry.getAllTypeIdToFactoryClassNameMappings();
    PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY.getAllTypeIdToFactoryClassNameMappings()
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
    userMap.put(EARLIEST_POSITION_RESERVED_TYPE_ID, DummyPubSubPositionFactory.class.getName());

    Exception exception = expectThrows(VeniceException.class, () -> new PubSubPositionTypeRegistry(userMap));
    assertTrue(
        exception.getMessage().contains("Conflicting entry for reserved position type ID"),
        "Got: " + exception.getMessage());
  }

  @Test
  public void testRegistryRejectsUnknownClassName() {
    Int2ObjectMap<String> userMap = new Int2ObjectOpenHashMap<>();
    userMap.put(99, "com.example.NonExistentPositionClass");
    Exception exception = expectThrows(VeniceException.class, () -> new PubSubPositionTypeRegistry(userMap));
    assertTrue(exception.getMessage().contains("Failed to create factory for"), "Got: " + exception.getMessage());
    assertTrue(ExceptionUtils.recursiveClassEquals(exception.getCause(), ClassNotFoundException.class));
  }

  @Test
  public void testRegistryRejectsNullClassName() {
    Int2ObjectMap<String> userMap = new Int2ObjectOpenHashMap<>();
    userMap.put(100, null);

    Exception exception = expectThrows(VeniceException.class, () -> new PubSubPositionTypeRegistry(userMap));
    assertTrue(exception.getMessage().contains("Blank class name for type ID"), "Got: " + exception.getMessage());
  }

  @Test
  public void testGetRegistryFromProperties() {
    Properties props = new Properties();
    props
        .put(ConfigKeys.PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP, "101:" + DummyPubSubPositionFactory.class.getName());
    VeniceProperties properties = new VeniceProperties(props);

    PubSubPositionTypeRegistry registry = PubSubPositionTypeRegistry.fromPropertiesOrDefault(properties);

    // Compare factory class, not instance
    assertEquals(registry.getFactoryByTypeId(101).getClass(), DummyPubSubPositionFactory.class);
    assertEquals(registry.getTypeIdForFactoryClass(DummyPubSubPositionFactory.class.getName()), 101);

    // Case: No user-defined types
    assertEquals(
        PubSubPositionTypeRegistry.fromPropertiesOrDefault(new VeniceProperties(new Properties())),
        PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY);
  }

  // Dummy implementation for test use only
  public static class DummyPubSubPositionFactory extends PubSubPositionFactory {
    public DummyPubSubPositionFactory(int positionTypeId) {
      super(positionTypeId);
    }

    @Override
    public PubSubPosition createFromWireFormat(PubSubPositionWireFormat positionWireFormat) {
      return null;
    }

    @Override
    public PubSubPosition createFromByteBuffer(ByteBuffer buffer) {
      return null;
    }

    @Override
    public String getPubSubPositionClassName() {
      return "";
    }
  }

  // tests two uniq ids for the same user provided types are not allowed
  @Test
  public void testRegistryRejectsDuplicateUserTypes() {
    Int2ObjectMap<String> userMap = new Int2ObjectOpenHashMap<>();
    userMap.put(99, DummyPubSubPositionFactory.class.getName());
    userMap.put(100, DummyPubSubPositionFactory.class.getName());

    Exception exception = expectThrows(VeniceException.class, () -> new PubSubPositionTypeRegistry(userMap));
    assertTrue(exception.getMessage().contains("Duplicate mapping for class"), "Got: " + exception.getMessage());
  }

  @Test
  public void testGetTypeIdForFactoryClassFailsForUnknownInstance() {
    PubSubPositionTypeRegistry registry = PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY;

    Exception exception =
        expectThrows(VeniceException.class, () -> registry.getTypeIdForFactoryClass("com.unknown.ClassName"));
    assertTrue(exception.getMessage().contains("class name not found"), "Got: " + exception.getMessage());
  }

  @Test
  public void testGetTypeIdForFactoryClassFailsForNullPosition() {
    PubSubPositionTypeRegistry registry = PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY;

    Exception exception =
        expectThrows(VeniceException.class, () -> registry.getTypeIdForFactoryInstance((PubSubPositionFactory) null));
    assertTrue(exception.getMessage().contains("position factory is null"), "Got: " + exception.getMessage());
  }

  @Test
  public void testGetFactoryByTypeIdFailsForInvalidTypeId() {
    PubSubPositionTypeRegistry registry = PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY;

    Exception exception = expectThrows(VeniceException.class, () -> registry.getFactoryByTypeId(12345));
    assertTrue(exception.getMessage().contains("type ID not found"), "Got: " + exception.getMessage());
  }
}
