package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public enum ViewType {
  CHANGE_CAPTURE(0);

  public final int value;

  ViewType(int value) {
    this.value = value;
  }

  private static final Map<Integer, ViewType> idMapping = new HashMap<>();
  static {
    Arrays.stream(values()).forEach(s -> idMapping.put(s.value, s));
  }

  public static ViewType getViewTypeFromInt(int v) {
    ViewType strategy = idMapping.get(v);
    if (strategy == null) {
      throw new VeniceException("Invalid ViewType id: " + v);
    }
    return strategy;
  }
}
