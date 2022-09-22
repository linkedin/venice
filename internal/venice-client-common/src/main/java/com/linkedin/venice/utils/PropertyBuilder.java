package com.linkedin.venice.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;


/**
 * PropertyBuilder is the suggested Way to construct VeniceProperties.
 *
 * Ideally you use the PropertyBuilder APIs to add properties
 * and once all the properties are added, call the build method
 * to get the VeniceProperties object.
 *
 * All the put methods overwrite the property if it already exists.
 *
 */
public class PropertyBuilder {
  private final Properties props = new Properties();

  public PropertyBuilder() {

  }

  public PropertyBuilder put(String key, Object value) {
    props.put(key, value.toString());
    return this;
  }

  public PropertyBuilder putIfAbsent(String key, Object value) {
    if (props.get(key) == null) {
      props.put(key, value.toString());
    }
    return this;
  }

  public PropertyBuilder put(File file) throws IOException {
    try (InputStream input = new BufferedInputStream(new FileInputStream(file))) {
      props.load(input);
    }
    return this;
  }

  public PropertyBuilder put(Properties properties) {
    for (Map.Entry<Object, Object> e: properties.entrySet()) {
      props.put(e.getKey(), e.getValue());
    }
    return this;
  }

  public VeniceProperties build() {
    return new VeniceProperties(this.props);
  }
}
