package com.linkedin.venice.client.store.streaming;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;


public class VeniceResponseMapImpl<K, V> extends HashMap<K, V> implements VeniceResponseMap<K, V> {
  private static final long serialVersionUID = 1L;

  private final Set<K> nonExistingKeys;
  private final boolean fullResponse;

  public VeniceResponseMapImpl(Map<K, V> validEntries, Queue<K> nonExistingKeyList, boolean fullResponse) {
    /**
     * Take a snapshot of both "validEntries" and "nonExistingKeyList", and both of them could be changed
     * later by the caller of this function.
     */
    super(validEntries);
    this.nonExistingKeys = new HashSet<>(nonExistingKeyList);
    this.fullResponse = fullResponse;
  }

  @Override
  public boolean isFullResponse() {
    return fullResponse;
  }

  @Override
  public Set<K> getNonExistingKeys() {
    return nonExistingKeys;
  }

  @Override
  public int getTotalEntryCount() {
    return size() + nonExistingKeys.size();
  }
}
