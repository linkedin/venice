package com.linkedin.venice.utils;

public interface PathResourceRegistry<T> {
  // Wildcard to match any path
  String WILDCARD_MATCH_ANY = "*";

  void register(String path, T resource);

  void unregister(String path);

  T find(String path);
}
