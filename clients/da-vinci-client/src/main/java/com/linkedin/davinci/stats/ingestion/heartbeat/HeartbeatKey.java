package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;


/**
 * Composite key for the flattened heartbeat timestamp map
 */
public final class HeartbeatKey {
  final String storeName;
  final int version;
  final int partition;
  final String region;
  private final int hashCode;

  public HeartbeatKey(String storeName, int version, int partition, String region) {
    this.storeName = storeName;
    this.version = version;
    this.partition = partition;
    this.region = region;
    // Manual hash computation avoids Objects.hash() varargs Object[] allocation and Integer autoboxing
    int h = storeName.hashCode();
    h = 31 * h + version;
    h = 31 * h + partition;
    h = 31 * h + region.hashCode();
    this.hashCode = h;
  }

  String getReplicaId() {
    return Utils.getReplicaId(Version.composeKafkaTopic(storeName, version), partition);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HeartbeatKey)) {
      return false;
    }
    HeartbeatKey that = (HeartbeatKey) o;
    return version == that.version && partition == that.partition && storeName.equals(that.storeName)
        && region.equals(that.region);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public String toString() {
    return getReplicaId() + "-" + region;
  }
}
