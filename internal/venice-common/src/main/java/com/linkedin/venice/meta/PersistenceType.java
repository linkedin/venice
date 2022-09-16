package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * Enums of persistence types in Venice.
 */
public enum PersistenceType {
  /**
   * Volatile storage engine based on a simple Java {@link java.util.concurrent.ConcurrentHashMap}.
   */
  IN_MEMORY(0),

  /**
   * Persistent storage engine that writes to durable media and maintains a B+ tree in the Java heap.
   */
  BDB(1),

  /**
   * Persistent storage engine that writes to durable media and maintains an off-heap in-memory index.
   */
  ROCKS_DB(2),

  /**
   * Fastest lock-free most secure of all storage engines. Ignores data put in it, always returns null.
   */
  BLACK_HOLE(3),

  /**
   * Similar to IN_MEMORY but with different retention rules of data (that is, data is evicted under certain circumstances)
   */
  CACHE(4);

  public final int value;

  PersistenceType(int v) {
    this.value = v;
  }

  private static final Map<Integer, PersistenceType> idMapping = new HashMap<>();
  static {
    Arrays.stream(values()).forEach(p -> idMapping.put(p.value, p));
  }

  public static PersistenceType getPersistenceTypeFromInt(int i) {
    PersistenceType pType = idMapping.get(i);
    if (pType == null) {
      throw new VeniceException("Invalid PersistenceType id: " + i);
    }
    return pType;
  }
}
