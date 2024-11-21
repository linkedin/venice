package com.linkedin.venice.beam.consumer;

import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import java.util.Objects;
import java.util.Set;


/**
 * Properties used by {@link com.linkedin.davinci.consumer.VeniceChangelogConsumer} to seek
 * checkpoints.
 */
public class CheckPointProperties {
  private Set<VeniceChangeCoordinate> coordinates;
  private long seekTimestamp;
  private String store;

  public CheckPointProperties(Set<VeniceChangeCoordinate> coordinates, long seekTimestamp, String store) {
    this.coordinates = coordinates;
    this.seekTimestamp = seekTimestamp;
    this.store = Objects.requireNonNull(store);
  }

  public Set<VeniceChangeCoordinate> getCoordinates() {
    return coordinates;
  }

  public long getSeekTimestamp() {
    return seekTimestamp;
  }

  public String getStore() {
    return store;
  }
}
