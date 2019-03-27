package com.linkedin.venice;

/**
 * Venice resource that could be refreshed to retrieve the newest data or clear the current data in memory.
 */
public interface VeniceResource {
  void refresh();

  /**
   * TODO: we may need to rename this function to be 'close' since this resource should not used any more
   * after calling this function.
   */
  void clear();
}
