package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.exceptions.VeniceException;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;


public class VeniceProperties implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final VeniceProperties EMPTY = new VeniceProperties();

  private final Map<String, String> props;

  /**
   * @deprecated Use {@link VeniceProperties#empty}
   */
  @Deprecated
  public VeniceProperties() {
    props = Collections.emptyMap();
  }

  public VeniceProperties(Properties properties) {
    Map<String, String> tmpProps = new HashMap<>(properties.size());
    for (Map.Entry<Object, Object> e: properties.entrySet()) {
      tmpProps.put(e.getKey().toString(), e.getValue() == null ? null : e.getValue().toString());
    }
    props = Collections.unmodifiableMap(tmpProps);
  }

  public VeniceProperties(Map<CharSequence, CharSequence> properties) {
    Map<String, String> tmpProps = new HashMap<>(properties.size());
    for (Map.Entry<CharSequence, CharSequence> e: properties.entrySet()) {
      tmpProps.put(e.getKey().toString(), e.getValue() == null ? null : e.getValue().toString());
    }
    props = Collections.unmodifiableMap(tmpProps);
  }

  public static VeniceProperties empty() {
    return EMPTY;
  }

  public Properties getPropertiesCopy() {
    Properties propertiesCopy = new Properties();
    props.forEach((key, value) -> {
      propertiesCopy.put(key, value);
    });
    return propertiesCopy;
  }

  public Set<String> keySet() {
    return props.keySet();
  }

  public boolean containsKey(String k) {
    return props.containsKey(k);
  }

  private String get(String key) {
    if (props.containsKey(key)) {
      return props.get(key);
    } else {
      throw new UndefinedPropertyException(
          "Property " + key + " is not defined, some codePath is calling without calling containsKeys");
    }
  }

  /**
   * Extracts properties that begin with the specified namespace.
   * <p>
   * This method identifies all properties that start with the given namespace, removes the namespace
   * prefix, and returns the filtered properties.
   * </p>
   * <p>
   * This enables support for dynamic configurations (e.g., required by Pub/Sub clients).
   * All properties following a namespace-based convention can be extracted and used
   * by various adapters, such as Pub/Sub Producers or Consumers.
   * </p>
   *
   * @param nameSpace The namespace to filter properties by.
   * @return A {@link VeniceProperties} instance containing properties with the namespace removed.
   */
  public VeniceProperties clipAndFilterNamespace(String nameSpace) {
    return clipAndFilterNamespace(Collections.singleton(nameSpace));
  }

  /**
   * Extracts properties that begin with any of the specified namespaces.
   * <p>
   * This method identifies all properties that start with one of the provided namespaces, removes
   * the matching namespace prefix, and returns the filtered properties.
   * </p>
   * <p>
   * This supports dynamic configurations for various use cases, including Pub/Sub clients,
   * messaging systems, and other configurable components. By following a namespace-based convention,
   * different configurations can be extracted and supplied to respective adapters.
   * </p>
   * <p>
   * Example structure:
   * </p>
   * <pre>{@code
   * "pubsub.kafka.bootstrap.servers"
   * "pubsub.pulsar.service.url"
   * "pubsub.warpstream.bucket.url"
   * }</pre>
   * <p>
   * Using this method with namespaces {"pubsub.kafka.", "pubsub.pulsar."} would result in:
   * </p>
   * <pre>{@code
   * "bootstrap.servers"
   * "service.url"
   * }</pre>
   *
   * @param namespaces A set of namespaces to filter properties by.
   * @return A {@link VeniceProperties} instance containing properties with the matching namespaces removed.
   */
  public VeniceProperties clipAndFilterNamespace(Set<String> namespaces) {
    PropertyBuilder builder = new PropertyBuilder();

    // Ensure all namespaces end with "."
    Set<String> formattedNamespaces =
        namespaces.stream().map(ns -> ns.endsWith(".") ? ns : ns + ".").collect(Collectors.toSet());

    for (Map.Entry<String, String> entry: this.props.entrySet()) {
      String key = entry.getKey();

      for (String namespace: formattedNamespaces) {
        if (key.startsWith(namespace)) {
          String extractedKey = key.substring(namespace.length());
          builder.put(extractedKey, entry.getValue());
          break; // Avoid unnecessary checks once a match is found
        }
      }
    }

    return builder.build();
  }

  private static final String STORE_PREFIX = "store-";

  /**
   * Get store related properties. properties that do not begin
   * with store- are considered common to all stores. out of
   * properties that begins with store- , only properties
   * that matches the store name are related to current
   * store. These matching properties override the base properties
   * if they are present.
   *
   * @param storeName name of the store
   * @return Properties for the current store.
   */

  public VeniceProperties getStoreProperties(String storeName) {
    String nameSpace = STORE_PREFIX + storeName + ".";

    PropertyBuilder builder = new PropertyBuilder();
    Properties storeOverrideProperties = new Properties();

    for (Map.Entry<String, String> entry: this.props.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(STORE_PREFIX)) { // Filter all store related overrides
        if (key.startsWith(nameSpace)) { // Save only current store related properties.
          String extractedKey = key.substring(nameSpace.length());
          storeOverrideProperties.setProperty(extractedKey, entry.getValue());
        }
      } else {
        // Non store related properties.
        builder.put(key, entry.getValue());
      }
    }

    // Apply the store related overrides on top of the current properties.
    builder.put(storeOverrideProperties);
    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    VeniceProperties that = (VeniceProperties) o;

    return props.equals(that.props);
  }

  @Override
  public int hashCode() {
    return this.props.hashCode();
  }

  @Override
  public String toString() {
    return toString(false);
  }

  public String toString(boolean prettyPrint) {
    StringBuilder builder = new StringBuilder("{");
    final AtomicBoolean first = new AtomicBoolean(true); // ah... the things we do for closures...
    this.props.entrySet().stream().sorted((o1, o2) -> o1.getKey().compareTo(o2.getKey())).forEach(entry -> {
      if (first.get()) {
        first.set(false);
      } else {
        builder.append(", ");
      }
      if (prettyPrint) {
        builder.append("\n\t");
      }
      builder.append(entry.getKey());
      builder.append(": ");
      builder.append(entry.getValue());
    });
    if (prettyPrint && !props.isEmpty()) {
      builder.append("\n");
    }
    builder.append("}");
    return builder.toString();
  }

  /**
   * Store all properties in a file
   *
   * @param file to store into
   * @throws IOException If there is an error writing
   */
  public void storeFlattened(File file) throws IOException {
    try (FileOutputStream fos = new FileOutputStream(file); BufferedOutputStream out = new BufferedOutputStream(fos);) {
      storeFlattened(out);
    }
  }

  /**
   * Store all properties to an {@link java.io.OutputStream}
   *
   * @param out The stream to write to
   * @throws IOException If there is an error writing
   */
  public void storeFlattened(OutputStream out) throws IOException {
    Properties p = new Properties();
    for (String key: this.props.keySet()) {
      if (!p.containsKey(key)) {
        p.setProperty(key, get(key));
      }
    }

    p.store(out, null);
  }

  public String getString(String key, Supplier<String> defaultValue) {
    if (containsKey(key)) {
      return get(key);
    } else {
      return defaultValue.get();
    }
  }

  public String getString(String key, String defaultValue) {
    if (containsKey(key)) {
      return get(key);
    } else {
      return defaultValue;
    }
  }

  public String getStringWithAlternative(String preferredKey, String altKey, String defaultValue) {
    if (containsKey(preferredKey)) {
      return get(preferredKey);
    } else if (containsKey(altKey)) {
      return get(altKey);
    } else {
      return defaultValue;
    }
  }

  public String getString(String key) {
    if (containsKey(key)) {
      return get(key);
    } else {
      throw new UndefinedPropertyException(key);
    }
  }

  public String getStringWithAlternative(String preferredKey, String altKey) {
    if (containsKey(preferredKey)) {
      return get(preferredKey);
    } else if (containsKey(altKey)) {
      return get(altKey);
    } else {
      throw new UndefinedPropertyException(preferredKey);
    }
  }

  public boolean getBoolean(String key, boolean defaultValue) {
    if (containsKey(key)) {
      return "true".equalsIgnoreCase(get(key));
    } else {
      return defaultValue;
    }
  }

  public boolean getBooleanWithAlternative(String preferredKey, String altKey, boolean defaultValue) {
    if (containsKey(preferredKey)) {
      return "true".equalsIgnoreCase(get(preferredKey));
    } else if (containsKey(altKey)) {
      return "true".equalsIgnoreCase(get(altKey));
    } else {
      return defaultValue;
    }
  }

  public boolean getBoolean(String key) {
    if (containsKey(key)) {
      return "true".equalsIgnoreCase(get(key));
    } else {
      throw new UndefinedPropertyException(key);
    }
  }

  public long getLong(String name, long defaultValue) {
    if (containsKey(name)) {
      return Long.parseLong(get(name));
    } else {
      return defaultValue;
    }
  }

  public long getLong(String name) {
    if (containsKey(name)) {
      return Long.parseLong(get(name));
    } else {
      throw new UndefinedPropertyException(name);
    }
  }

  public int getInt(String name, int defaultValue) {
    if (containsKey(name)) {
      return Integer.parseInt(get(name));
    } else {
      return defaultValue;
    }
  }

  public int getInt(String name) {
    if (containsKey(name)) {
      return Integer.parseInt(get(name));
    } else {
      throw new UndefinedPropertyException(name);
    }
  }

  public Optional<Integer> getOptionalInt(String name) {
    if (containsKey(name)) {
      return Optional.of(Integer.parseInt(get(name)));
    } else {
      return Optional.empty();
    }
  }

  public double getDouble(String name, double defaultValue) {
    if (containsKey(name)) {
      return Double.parseDouble(get(name));
    } else {
      return defaultValue;
    }
  }

  public double getDouble(String name) {
    if (containsKey(name)) {
      return Double.parseDouble(get(name));
    } else {
      throw new UndefinedPropertyException(name);
    }
  }

  public long getSizeInBytes(String name, long defaultValue) {
    if (containsKey(name)) {
      return getSizeInBytes(name);
    } else {
      return defaultValue;
    }
  }

  public long getSizeInBytes(String name) {
    if (!containsKey(name)) {
      throw new UndefinedPropertyException(name);
    }

    String bytes = get(name);
    return convertSizeFromLiteral(bytes);
  }

  public static long convertSizeFromLiteral(String size) {
    String sizeLc = size.toLowerCase().trim();
    if (sizeLc.endsWith("kb")) {
      return Long.parseLong(size.substring(0, size.length() - 2)) * 1024;
    } else if (sizeLc.endsWith("k")) {
      return Long.parseLong(size.substring(0, size.length() - 1)) * 1024;
    } else if (sizeLc.endsWith("mb")) {
      return Long.parseLong(size.substring(0, size.length() - 2)) * 1024 * 1024;
    } else if (sizeLc.endsWith("m")) {
      return Long.parseLong(size.substring(0, size.length() - 1)) * 1024 * 1024;
    } else if (sizeLc.endsWith("gb")) {
      return Long.parseLong(size.substring(0, size.length() - 2)) * 1024 * 1024 * 1024;
    } else if (sizeLc.endsWith("g")) {
      return Long.parseLong(size.substring(0, size.length() - 1)) * 1024 * 1024 * 1024;
    } else {
      return Long.parseLong(size);
    }
  }

  public List<String> getList(String key, List<String> defaultValue) {
    if (!containsKey(key)) {
      return defaultValue;
    }

    String value = get(key);

    String[] pieces = value.split("\\s*,\\s*");
    return Arrays.asList(pieces);
  }

  public List<String> getListWithAlternative(String preferredKey, String altKey, List<String> defaultValue) {
    if (!containsKey(preferredKey) && !containsKey(altKey)) {
      return defaultValue;
    }
    String value = "";
    if (containsKey(preferredKey)) {
      value = get(preferredKey);
    } else if (containsKey(altKey)) {
      value = get(altKey);
    }

    String[] pieces = value.split("\\s*,\\s*");
    return Arrays.asList(pieces);
  }

  public List<String> getList(String key) {
    if (!containsKey(key)) {
      throw new UndefinedPropertyException(key);
    }
    return getList(key, null);
  }

  public Map<String, String> getMap(String key, Map<String, String> defaultValue) {
    if (!containsKey(key) || getString(key).trim().isEmpty()) {
      return defaultValue;
    }
    return getMap(key);
  }

  /**
   * Retrieves a map from the configuration using the given key,
   * where each entry is a colon-separated string (e.g., "key:value").
   *
   * @param key the configuration key
   * @return an unmodifiable {@code Map<String, String>}
   * @throws UndefinedPropertyException if the key does not exist
   * @throws VeniceException if any entry is malformed
   */
  public Map<String, String> getMap(String key) {
    return Collections.unmodifiableMap(
        parseKeyValueList(
            key,
            k -> k, // identity function for string keys
            HashMap::new));
  }

  /**
   * Retrieves a map from the configuration using the given key,
   * where keys are expected to be integers and values are strings.
   *
   * @param key the configuration key
   * @return an {@code Int2ObjectMap<String>} of parsed entries
   * @throws UndefinedPropertyException if the key does not exist
   * @throws VeniceException if any entry is malformed or key is not a valid integer
   */
  public Int2ObjectMap<String> getIntKeyedMap(String key) {
    Int2ObjectMap<String> parsedMap = parseKeyValueList(key, strKey -> {
      try {
        return Integer.parseInt(strKey.trim());
      } catch (NumberFormatException e) {
        throw new VeniceException("Invalid integer key: " + strKey, e);
      }
    }, Int2ObjectOpenHashMap::new);
    return Int2ObjectMaps.unmodifiable(parsedMap);
  }

  /**
   * Retrieves a map from the configuration using the given key.
   * <p>
   * The expected format of the value is a list of strings, where each entry is a colon-separated key-value pair.
   * Only the first colon (`:`) is used to split the key from the value, allowing the value itself to contain colons.
   * <p>
   * Example input for the key:
   * <pre>
   *   config.key = ["1:broker1.kafka.com:9092", "2:broker2.kafka.com:9093"]
   * </pre>
   * This would return a map:
   * <pre>
   *   {
   *     "1" -> "broker1.kafka.com:9092",
   *     "2" -> "broker2.kafka.com:9093"
   *   }
   * </pre>
   * @param key         the configuration key
   * @param keyParser   a function to convert the string key into the desired key type
   * @param mapSupplier a supplier that provides a mutable map instance
   * @param <K>         the key type
   * @param <M>         the map type
   * @return a populated map from the config
   * @throws UndefinedPropertyException if the key is missing
   * @throws VeniceException if any entry is malformed
   */
  private <K, M extends Map<K, String>> M parseKeyValueList(
      String key,
      Function<String, K> keyParser,
      Supplier<M> mapSupplier) {
    if (!containsKey(key)) {
      throw new UndefinedPropertyException(key);
    }

    List<String> keyValuePairs = getList(key);
    M map = mapSupplier.get();

    for (String keyValuePair: keyValuePairs) {
      // One entry could have multiple ":". For example, "<ID>:<Kafka URL>:<port>". In this case, we split the String by
      // its first ":" so that we get key=<ID> and value=<Kafka URL>:<port>
      int indexOfFirstColon = keyValuePair.indexOf(':');
      if (indexOfFirstColon == -1) {
        throw new VeniceException(
            "Invalid config. Expect each entry to contain at least one \":\". Got: " + keyValuePair);
      }
      String mapKey = keyValuePair.substring(0, indexOfFirstColon);
      String mapValue = keyValuePair.substring(indexOfFirstColon + 1);
      map.put(keyParser.apply(mapKey), mapValue);
    }

    return map;
  }

  /**
   * Helper function to convert a map to a string representation.
   * @param map The map to convert
   * @return
   */
  public static String mapToString(Map<?, ?> map) {
    if (map == null || map.isEmpty()) {
      return "";
    }

    StringBuilder builder = new StringBuilder();
    for (Map.Entry<?, ?> entry: map.entrySet()) {
      if (builder.length() > 0) {
        builder.append(',');
      }
      builder.append(entry.getKey()).append(':').append(entry.getValue());
    }
    return builder.toString();
  }

  public Properties toProperties() {
    Properties properties = new Properties();
    properties.putAll(this.props);
    return properties;
  }

  public boolean isEmpty() {
    return this.props.isEmpty();
  }

  public Map<String, String> getAsMap() {
    return props;
  }
}
