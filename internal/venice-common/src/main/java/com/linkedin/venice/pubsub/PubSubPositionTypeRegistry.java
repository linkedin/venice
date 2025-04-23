package com.linkedin.venice.pubsub;

import static com.linkedin.venice.ConfigKeys.PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPositionFactory;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.VeniceProperties;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A configurable registry that maintains a bidirectional mapping between
 * {@link PubSubPositionFactory} implementation class names (fully qualified) and their corresponding integer type IDs.
 * <p>
 * This registry enables compact serialization and deserialization of {@link PubSubPosition} instances
 * by representing them with integer type IDs instead of full class names.
 * <p>
 * During construction, the registry validates that all class names are well-formed and resolvable at runtime.
 * It also enforces consistency with reserved type IDs for symbolic positions such as
 * {@link PubSubSymbolicPosition#EARLIEST} and, {@link PubSubSymbolicPosition#LATEST}, and
 * {@link ApacheKafkaOffsetPosition}, rejecting any attempts to override them with mismatched types.
 * <p>
 * <b>This registry is immutable after construction</b>. The internal mappings cannot be modified once built,
 * ensuring thread safety and predictable behavior throughout the lifetime of the instance.
 * <p>
 * This class is intended for use in components that need to serialize, deserialize, or resolve
 * {@link PubSubPosition} types based on their type IDs or class names.
 */
public class PubSubPositionTypeRegistry {
  public static final Logger LOGGER = LogManager.getLogger(PubSubPositionTypeRegistry.class);

  /**
   * Reserved type IDs for internal standard {@link PubSubPosition} implementations.
   * <p>
   * These constants define special type IDs used to identify symbolic or built-in PubSub positions.
   * They are reserved for internal use and must not be reused, overridden, or assigned to custom
   * {@link PubSubPosition} implementations by client code.
   * <p>
   * Reserved IDs:
   * <ul>
   *   <li>{@code -2} —> {@link PubSubSymbolicPosition#EARLIEST}: Marker for the earliest retrievable position in a partition</li>
   *   <li>{@code -1} —> {@link PubSubSymbolicPosition#LATEST}: Marker for the latest retrievable position in a partition</li>
   *   <li>{@code  0} —> {@link ApacheKafkaOffsetPosition}: Standard Kafka offset-based position</li>
   * </ul>
   * <p>
   * {@link #POSITION_TYPE_INVALID_MAGIC_VALUE} is used as a sentinel value to indicate that a position
   * type ID has not yet been assigned or initialized. It must never be used as a valid position type.
   */
  public static final int EARLIEST_POSITION_RESERVED_TYPE_ID = -2;
  public static final int LATEST_POSITION_RESERVED_TYPE_ID = -1;
  public static final int APACHE_KAFKA_OFFSET_POSITION_TYPE_ID = 0;
  public static final int POSITION_TYPE_INVALID_MAGIC_VALUE = Integer.MIN_VALUE;

  /**
   * A predefined map of reserved PubSub position type IDs to their corresponding fully qualified factory class names.
   * <p>
   * This map is intended for internal use to bootstrap or validate known position implementations.
   * It includes all reserved positions that are part of the standard Venice PubSub position model.
   * <p>
   */
  public static final Int2ObjectMap<String> RESERVED_POSITION_TYPE_ID_TO_CLASS_NAME_MAP;

  static {
    Int2ObjectMap<String> tempMap = new Int2ObjectOpenHashMap<>(3);
    tempMap.put(EARLIEST_POSITION_RESERVED_TYPE_ID, EarliestPositionFactory.class.getName());
    tempMap.put(LATEST_POSITION_RESERVED_TYPE_ID, LatestPositionFactory.class.getName());
    tempMap.put(APACHE_KAFKA_OFFSET_POSITION_TYPE_ID, ApacheKafkaOffsetPositionFactory.class.getName());
    RESERVED_POSITION_TYPE_ID_TO_CLASS_NAME_MAP = Int2ObjectMaps.unmodifiable(tempMap);
  }

  /**
   * A default, pre-configured {@link PubSubPositionTypeRegistry} instance that contains all reserved
   * {@link PubSubPosition} type IDs and their associated factory class names.
   * <p>
   * This instance should be used wherever a standard, system-defined position registry is sufficient.
   * It includes entries for known symbolic positions like {@link PubSubSymbolicPosition#EARLIEST},
   * {@link PubSubSymbolicPosition#LATEST}, and standard implementations such as {@link ApacheKafkaOffsetPosition}.
   * <p>
   * Note: This instance is read-only and should not be modified or extended at runtime.
   */
  public static final PubSubPositionTypeRegistry RESERVED_POSITION_TYPE_REGISTRY =
      new PubSubPositionTypeRegistry(RESERVED_POSITION_TYPE_ID_TO_CLASS_NAME_MAP);

  private final Object2IntMap<String> factoryClassNameToTypeIdMap;
  private final Int2ObjectMap<String> typeIdToFactoryClassNameMap;
  private final Int2ObjectMap<PubSubPositionFactory> typeIdToFactoryMap;

  /**
   * Constructs a {@link PubSubPositionTypeRegistry} by merging the provided type ID to class name map
   * with the system-defined reserved position type mappings.
   * <p>
   * Reserved position type IDs (such as those for {@link PubSubSymbolicPosition#EARLIEST}, {@link PubSubSymbolicPosition#LATEST}, and
   * {@link ApacheKafkaOffsetPosition}) are validated to ensure they are not overridden with a conflicting class name.
   * If a reserved ID is present in the provided map, it must map to the exact same class name as the reserved definition;
   * otherwise, an {@link VeniceException} is thrown.
   * <p>
   * Any reserved IDs not present in the input map are automatically included.
   * The final registry is unmodifiable after construction.
   *
   * @param userProvidedMap a map from position type IDs to fully qualified class names
   * @throws VeniceException if a reserved type ID is overridden with a different class
   * @throws ClassNotFoundException if any of the provided class names cannot be loaded
   */
  public PubSubPositionTypeRegistry(Int2ObjectMap<String> userProvidedMap) {
    Int2ObjectMap<String> mergedMap = mergeAndValidateTypes(userProvidedMap);
    this.typeIdToFactoryClassNameMap = Int2ObjectMaps.unmodifiable(mergedMap);
    this.factoryClassNameToTypeIdMap = Object2IntMaps.unmodifiable(buildClassNameToTypeIdMap(mergedMap));
    this.typeIdToFactoryMap = Int2ObjectMaps.unmodifiable(instantiateFactories(mergedMap));
    LOGGER.info(
        "PubSub position type registry initialized with {} entries: {}",
        factoryClassNameToTypeIdMap.size(),
        factoryClassNameToTypeIdMap);
  }

  private static Int2ObjectMap<String> mergeAndValidateTypes(Int2ObjectMap<String> userMap) {
    Int2ObjectMap<String> merged = new Int2ObjectOpenHashMap<>(userMap);
    for (Int2ObjectMap.Entry<String> reservedEntry: RESERVED_POSITION_TYPE_ID_TO_CLASS_NAME_MAP.int2ObjectEntrySet()) {
      int reservedId = reservedEntry.getIntKey();
      String reservedClass = reservedEntry.getValue();
      if (merged.containsKey(reservedId)) {
        String userClass = merged.get(reservedId);
        if (!reservedClass.equals(userClass)) {
          LOGGER.error(
              "Conflicting entry for reserved position type ID: {}. Expected class name: [{}], but got: [{}].",
              reservedId,
              reservedClass,
              userClass);
          throw new VeniceException(
              "Conflicting entry for reserved position type ID: " + reservedId + ". Expected class name: ["
                  + reservedClass + "], but got: [" + userClass + "]");
        }
      } else {
        merged.put(reservedId, reservedClass);
      }
    }
    return merged;
  }

  private static Object2IntMap<String> buildClassNameToTypeIdMap(Int2ObjectMap<String> typeIdToClassMap) {
    Object2IntMap<String> classNameToTypeId = new Object2IntOpenHashMap<>(typeIdToClassMap.size());
    for (Map.Entry<Integer, String> entry: typeIdToClassMap.int2ObjectEntrySet()) {
      int typeId = entry.getKey();
      String className = entry.getValue();
      if (StringUtils.isBlank(className)) {
        LOGGER.error("Blank class name for type ID: {}. Type ID mapping: {} cannot be used.", typeId, typeIdToClassMap);
        throw new VeniceException("Blank class name for type ID: " + typeId);
      }
      if (classNameToTypeId.containsKey(className) && classNameToTypeId.getInt(className) != typeId) {
        LOGGER.error(
            "Duplicate mapping for class name {}. Type ID: {} conflicts with existing mapping: {}.",
            className,
            typeId,
            classNameToTypeId.getInt(className));
        throw new VeniceException("Duplicate mapping for class " + className);
      }
      classNameToTypeId.put(className, typeId);
    }
    return classNameToTypeId;
  }

  private static Int2ObjectMap<PubSubPositionFactory> instantiateFactories(Int2ObjectMap<String> typeIdToClassMap) {
    Int2ObjectMap<PubSubPositionFactory> factories = new Int2ObjectOpenHashMap<>(typeIdToClassMap.size());
    for (Map.Entry<Integer, String> entry: typeIdToClassMap.int2ObjectEntrySet()) {
      int typeId = entry.getKey();
      String className = entry.getValue();
      try {
        PubSubPositionFactory factory = ReflectUtils
            .callConstructor(ReflectUtils.loadClass(className), new Class<?>[] { int.class }, new Object[] { typeId });
        factories.put(typeId, factory);
      } catch (Exception e) {
        LOGGER.error(
            "Failed to create factory for class name: {} (type ID: {}). Mapping: {} cannot be used.",
            className,
            typeId,
            typeIdToClassMap,
            e);
        throw new VeniceException("Failed to create factory for " + className + " (ID " + typeId + ")", e);
      }
    }
    return factories;
  }

  /**
   * Returns the integer type ID for the given pubsub position factory class name.
   *
   * @param factoryClassName the fully qualified class name of a {@link PubSubPositionFactory} implementation
   * @return the integer type ID or throws an exception if the class name is not found
   */
  public int getTypeIdForFactoryClass(String factoryClassName) {
    if (!factoryClassNameToTypeIdMap.containsKey(factoryClassName)) {
      LOGGER.error(
          "PubSub position factory class name not found: {}. Valid class names: {}",
          factoryClassName,
          factoryClassNameToTypeIdMap.keySet());
      throw new VeniceException("PubSub position factory class name not found: " + factoryClassName);
    }
    return factoryClassNameToTypeIdMap.getInt(factoryClassName);
  }

  /**
   * Returns the integer type ID for the given {@link PubSubPositionFactory} implementation.
   *
   * @param pubSubPositionFactory the {@link PubSubPositionFactory} implementation
   * @return the integer type ID, or throws an exception if the factory is null
   */
  public int getTypeIdForFactoryInstance(PubSubPositionFactory pubSubPositionFactory) {
    if (pubSubPositionFactory == null) {
      LOGGER.error("PubSub position factory is null. Cannot get type ID.");
      throw new VeniceException("PubSub position factory is null.");
    }
    return getTypeIdForFactoryClass(pubSubPositionFactory.getClass().getName());
  }

  /**
   * Checks if the given pubsub position factory class name is present in the registry.
   *
   * @param positionFactoryClassName the fully qualified class name of a {@link PubSubPositionFactory} implementation
   * @return true if the class name is present, false otherwise
   */
  public boolean containsFactoryClass(String positionFactoryClassName) {
    return factoryClassNameToTypeIdMap.containsKey(positionFactoryClassName);
  }

  /**
   * Returns the fully qualified class name of the {@link PubSubPositionFactory} implementation
   *
   * @param typeId the integer type ID of a {@code PubSubPosition} implementation
   * @return the fully qualified class name, or throws an exception if the type ID is not found
   */
  public PubSubPositionFactory getFactoryByTypeId(int typeId) {
    if (!typeIdToFactoryClassNameMap.containsKey(typeId)) {
      LOGGER.error(
          "PubSub position type ID not found: {}. Valid type IDs: {}",
          typeId,
          typeIdToFactoryClassNameMap.keySet());
      throw new VeniceException("PubSub position type ID not found: " + typeId);
    }
    return typeIdToFactoryMap.get(typeId);
  }

  /**
   * Returns the mapping of integer type IDs to fully qualified factory class names.
   * @return a map from integer type IDs to the fully qualified class factory names of {@code PubSubPosition} implementations
   */
  @VisibleForTesting
  Int2ObjectMap<String> getAllTypeIdToFactoryClassNameMappings() {
    return typeIdToFactoryClassNameMap;
  }

  @Override
  public String toString() {
    return "PositionTypeRegistry(" + factoryClassNameToTypeIdMap + ")";
  }

  /**
   * Creates a {@link PubSubPositionTypeRegistry} from the given {@link VeniceProperties} by reading the config key
   * {@link ConfigKeys#PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP}.
   * <p>
   * If the key is present, it loads the type-to-class-name mapping from the config and merges it with the reserved
   * system-defined types. If the key is not present, it returns the default registry containing only the reserved types.
   *
   * @param properties the {@link VeniceProperties} containing the configuration
   * @return a {@link PubSubPositionTypeRegistry} constructed from the provided configuration, or the default registry if not configured
   * @throws VeniceException if the provided configuration attempts to override a reserved type with a different class
   */
  public static PubSubPositionTypeRegistry fromPropertiesOrDefault(VeniceProperties properties) {
    if (properties.containsKey(PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP)) {
      return new PubSubPositionTypeRegistry(properties.getIntKeyedMap(PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP));
    }
    return PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY;
  }
}
