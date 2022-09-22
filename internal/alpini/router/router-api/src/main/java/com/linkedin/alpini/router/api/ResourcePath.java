package com.linkedin.alpini.router.api;

import java.util.Collection;
import java.util.Iterator;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface ResourcePath<K> {
  default boolean hasMultiplePartitionKeys() {
    Iterator<K> it = getPartitionKeys().iterator();
    if (it.hasNext()) {
      it.next();
      return it.hasNext();
    }
    return false;
  }

  default K getPartitionKey() {
    Iterator<K> it = getPartitionKeys().iterator();
    K key = it.next();
    if (it.hasNext()) {
      throw new IllegalStateException("path has multiple keys");
    }
    return key;
  }

  @Nonnull
  String getLocation();

  @Nonnull
  Collection<K> getPartitionKeys();

  @Nonnull
  String getResourceName();

  /**
   * If the inherited class wants to know whether the current path belongs to a retry request or not,
   * it needs to implement this method properly to maintain the internal state.
   */
  default void setRetryRequest() {
  }
}
