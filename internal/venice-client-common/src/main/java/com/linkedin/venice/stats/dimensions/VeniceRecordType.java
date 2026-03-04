package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RECORD_TYPE;


/**
 * Dimension values for the {@link VeniceMetricsDimensions#VENICE_RECORD_TYPE} dimension, representing the type of
 * record being operated on during ingestion.
 *
 * <p>Venice stores two types of records for active-active enabled stores:
 * <ul>
 *   <li><b>Data records</b> — The actual user data (key-value pairs) stored in the storage engine.</li>
 *   <li><b>Replication metadata (RMD) records</b> — Metadata that tracks per-field timestamps and
 *       other information needed for deterministic conflict resolution (DCR) across regions.</li>
 * </ul>
 *
 * <p>This dimension is used by several metrics to distinguish between operations on data vs. RMD:
 * <ul>
 *   <li>{@code ingestion.dcr.lookup.time} — Storage engine lookup latency for data vs. RMD records</li>
 *   <li>{@code ingestion.dcr.lookup.cache.hit_count} — Cache hit counts for data vs. RMD lookups</li>
 *   <li>{@code ingestion.record.assembled_size} — Assembled record size for data vs. RMD records</li>
 * </ul>
 *
 * @see com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity#DCR_LOOKUP_TIME
 * @see com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity#DCR_LOOKUP_CACHE_HIT_COUNT
 * @see com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity#RECORD_ASSEMBLED_SIZE
 */
public enum VeniceRecordType implements VeniceDimensionInterface {
  /** The actual user data record (key-value pair) stored in the storage engine */
  DATA,
  /** The replication metadata (RMD) record used for deterministic conflict resolution in active-active stores */
  REPLICATION_METADATA;

  /**
   * All instances of this enum share the same dimension name.
   * Refer to {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_RECORD_TYPE;
  }
}
