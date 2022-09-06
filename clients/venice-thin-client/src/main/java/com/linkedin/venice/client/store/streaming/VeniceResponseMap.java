package com.linkedin.venice.client.store.streaming;

import java.util.Map;
import java.util.Set;


/**
 * Venice customized map, which could contains either a full response or a partial response.
 *
 * TODO: we could add more methods, such as get the total result cnt including valid entries and non-existing entries,
 * or response quality in the future if necessary.
 */
public interface VeniceResponseMap<K, V> extends Map<K, V> {
  /**
   * Whether the result is a full response or not.
   * @return
   */
  boolean isFullResponse();

  /**
   * Retrieve all the non-existing keys known so far, and if the current response is a full response, this will
   * contain all the missing keys in the request.
   * @return
   */
  Set<K> getNonExistingKeys();

  /**
   * Return the total number of entries, including non-existing keys.
   */
  int getTotalEntryCount();

}
