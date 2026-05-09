package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.dimensions.VeniceChunkingStatus;
import com.linkedin.venice.stats.dimensions.VeniceRegionLocality;
import com.linkedin.venice.stats.dimensions.VeniceStoreWriteType;
import com.linkedin.venice.utils.Utils;


/**
 * Composite key for the flattened heartbeat timestamp map.
 *
 * <p>Identity is {@code (storeName, version, partition, region)}. The SLO classification
 * labels {@link #writeType}, {@link #chunkingStatus}, {@link #locality} are passenger fields:
 * resolved once when the caller (SIT) builds the key, carried along the per-record hot path
 * to avoid repeated derivation, and intentionally excluded from {@link #equals}/{@link #hashCode}.
 */
public final class HeartbeatKey {
  final String storeName;
  final int version;
  final int partition;
  final String region;
  /*
   * Passenger labels: not part of identity. Used by per-record OTel emission so each record
   * carries pre-resolved enum references (zero allocation on the hot path).
   */
  final VeniceStoreWriteType writeType;
  final VeniceChunkingStatus chunkingStatus;
  final VeniceRegionLocality locality;
  private final int hashCode;

  /**
   * Lookup-only constructor: builds a key without the SLO labels. Use only when the resulting
   * key is consumed for identity (map lookup, equality) and never flows to per-record OTel emission.
   * The canonical hot-path constructor is the 7-arg variant below — that one is what the SIT/PCS
   * pipeline uses so each record carries pre-resolved enum references.
   */
  public HeartbeatKey(String storeName, int version, int partition, String region) {
    this(storeName, version, partition, region, null, null, null);
  }

  public HeartbeatKey(
      String storeName,
      int version,
      int partition,
      String region,
      VeniceStoreWriteType writeType,
      VeniceChunkingStatus chunkingStatus,
      VeniceRegionLocality locality) {
    this.storeName = storeName;
    this.version = version;
    this.partition = partition;
    this.region = region;
    this.writeType = writeType;
    this.chunkingStatus = chunkingStatus;
    this.locality = locality;
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

  /**
   * Public accessor for cross-package tests; the package-private field is the canonical source.
   */
  public VeniceStoreWriteType getWriteType() {
    return writeType;
  }

  /**
   * Public accessor for cross-package tests; the package-private field is the canonical source.
   */
  public VeniceChunkingStatus getChunkingStatus() {
    return chunkingStatus;
  }

  /**
   * Public accessor for cross-package tests; the package-private field is the canonical source.
   */
  public VeniceRegionLocality getLocality() {
    return locality;
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
