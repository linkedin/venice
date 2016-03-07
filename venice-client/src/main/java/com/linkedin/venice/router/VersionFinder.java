package com.linkedin.venice.router;

/***
 * A version finder will look up the active version of a store.
 */
public interface VersionFinder {

  public int getVersion(String store);

}
