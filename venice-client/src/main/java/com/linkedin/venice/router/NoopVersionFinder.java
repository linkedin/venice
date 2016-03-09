package com.linkedin.venice.router;

/**
 * To be used for testing.  A real version finder will use the Helix Spectator to lookup the active version
 */
public class NoopVersionFinder implements VersionFinder {
  @Override
  public int getVersion(String store) {
    return 1;
  }
}
