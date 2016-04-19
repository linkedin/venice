package com.linkedin.venice.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import javax.swing.*;


/**
 * Created by athirupa on 4/20/16.
 */
public class PropertyBuilder {

  private final Properties props = new Properties();

  public PropertyBuilder() {

  }
  public PropertyBuilder put(String key, String value) {
    props.put(key, value);
    return this;
  }

  public PropertyBuilder put(String key, Integer value) {
    props.put(key, value.toString());
    return this;
  }

  public PropertyBuilder put(String key, Long value) {
    props.put(key, value.toString());
    return this;
  }

  public PropertyBuilder put(String key, Double value) {
    props.put(key, value.toString());
    return this;
  }

  public PropertyBuilder put(String key, Boolean value) {
    props.put(key, value.toString());
    return this;
  }

  public PropertyBuilder put(File file)
          throws IOException {
    try (InputStream input = new BufferedInputStream(new FileInputStream(file))) {
      props.load(input);
    }
    return this;
  }

  public PropertyBuilder put(Properties properties) {
    for (Map.Entry<Object, Object> e : properties.entrySet()) {
      props.put(e.getKey(), e.getValue());
    }
    return this;
  }

  public VeniceProperties build() {
     return new VeniceProperties(this.props);
  }
}
