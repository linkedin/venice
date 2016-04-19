package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.UndefinedPropertyException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


public class VeniceProperties {

  private final Map<String, String> props;

  public VeniceProperties(Properties properties) {
    this.props = new HashMap<String, String>();
    for (Map.Entry<Object, Object> e : properties.entrySet()) {
      this.props.put((String) e.getKey(), (String) e.getValue());
    }
  }

  public boolean containsKey(String k) {
    return props.containsKey(k);
  }

  private String get(String key) {
    if(props.containsKey(key)) {
      return props.get(key);
    } else {
      throw new UndefinedPropertyException("Property " + key +
              " is not defined, some codePath is calling without calling containsKeys");
    }
  }

  public VeniceProperties extractProperties(String nameSpace) {
    PropertyBuilder builder = new PropertyBuilder();
    if(!nameSpace.endsWith(".")) {
      nameSpace = nameSpace + ".";
    }

    for (Map.Entry<String,String> entry: this.props.entrySet()) {
      String key = entry.getKey();
      if(key.startsWith(nameSpace)) {
         String extractedKey = key.substring(nameSpace.length());
         builder.put(extractedKey , this.props.get(key));
      }
    }
    return builder.build();
  }

  private static final String STORE_PREFIX = "store-";
  public VeniceProperties getStoreProperties(String storeName) {
    String nameSpace = STORE_PREFIX + storeName + ".";

    PropertyBuilder builder = new PropertyBuilder();

    for (Map.Entry<String,String> entry: this.props.entrySet()) {
      String key = entry.getKey();
      if(key.startsWith(STORE_PREFIX)) {
        if(key.startsWith(nameSpace)) {
          String extractedKey = key.substring(nameSpace.length());
          builder.put(extractedKey, entry.getValue());
        }
      } else {
        builder.put(key, this.props.get(key));
      }
    }
    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    return this.props.equals(o);
  }

  @Override
  public int hashCode() {
    return this.props.hashCode();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("{");
    for (Map.Entry<String, String> entry : this.props.entrySet()) {
      builder.append(entry.getKey());
      builder.append(": ");
      builder.append(entry.getValue());
      builder.append(", ");
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
    try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
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
    for (String key : this.props.keySet()) {
      if (!p.containsKey(key)) {
        p.setProperty(key, get(key));
      }
    }

    p.store(out, null);
  }

  public String getString(String key, String defaultValue) {
    if (containsKey(key)) {
      return get(key);
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

  public boolean getBoolean(String key, boolean defaultValue) {
    if (containsKey(key)) {
      return "true".equalsIgnoreCase(get(key));
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

  public long getBytes(String name, long defaultValue) {
    if (containsKey(name)) {
      return getBytes(name);
    } else {
      return defaultValue;
    }
  }
  public long getBytes(String name) {
    if (!containsKey(name)) {
      throw new UndefinedPropertyException(name);
    }

    String bytes = get(name);
    String bytesLc = bytes.toLowerCase().trim();
    if (bytesLc.endsWith("kb")) {
      return Long.parseLong(bytes.substring(0, bytes.length() - 2)) * 1024;
    } else if (bytesLc.endsWith("k")) {
      return Long.parseLong(bytes.substring(0, bytes.length() - 1)) * 1024;
    } else if (bytesLc.endsWith("mb")) {
      return Long.parseLong(bytes.substring(0, bytes.length() - 2)) * 1024 * 1024;
    } else if (bytesLc.endsWith("m")) {
      return Long.parseLong(bytes.substring(0, bytes.length() - 1)) * 1024 * 1024;
    } else if (bytesLc.endsWith("gb")) {
      return Long.parseLong(bytes.substring(0, bytes.length() - 2)) * 1024 * 1024 * 1024;
    } else if (bytesLc.endsWith("g")) {
      return Long.parseLong(bytes.substring(0, bytes.length() - 1)) * 1024 * 1024 * 1024;
    } else {
      return Long.parseLong(bytes);
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

  public List<String> getList(String key) {
    if (!containsKey(key)) {
      throw new UndefinedPropertyException(key);
    }
    return getList(key, null);
  }

  public Properties toProperties() {
    Properties properties = new Properties();
    properties.putAll(this.props);
    return properties;
  }

}
