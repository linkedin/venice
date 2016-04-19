package com.linkedin.venice;

/**
 * Venice resource that could be refreshed to retrieve the newest data or clear the current data in memory.
 */
public interface VeniceResource {
  public void refresh();

  public void clear();
}
