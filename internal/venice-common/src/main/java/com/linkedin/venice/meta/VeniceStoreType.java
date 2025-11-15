package com.linkedin.venice.meta;

/**
 * Enumeration of Venice store types used to classify stores based on their data ingestion patterns
 * and operational characteristics
 */
public enum VeniceStoreType {
  /**
   * Batch-only stores that are populated exclusively through offline batch processes.
   * <p>
   * Data is ingested via:
   * <ul>
   *   <li>Offline push jobs (Hadoop/Spark)</li>
   *   <li>Samza batch reprocessing jobs</li>
   * </ul>
   * <p>
   * These stores do not support real-time updates or incremental pushes. All data changes
   * require a full batch push that replaces the previous version atomically.
   */
  BATCH,

  /**
   * Hybrid stores that support both batch and real-time data ingestion.
   * <p>
   * Data is ingested via:
   * <ul>
   *   <li>Offline push jobs (Hadoop/Spark) - for bulk data</li>
   *   <li>Samza real-time updates - for streaming near-line updates</li>
   *   <li>Incremental push from Venice push jobs - for incremental batch updates</li>
   *   <li>Online application writes - for direct real-time updates</li>
   * </ul>
   * <p>
   * Batch pushes create a new store version as the base snapshot, then real-time and incremental
   * updates are applied on top of that version. This allows hybrid stores to serve the freshest
   * data by combining batch baseline with continuous updates.
   */
  HYBRID,

  /**
   * System stores used internally by Venice for metadata, operational state, and control plane data.
   * <p>
   * System store names are prefixed with {@link Store#SYSTEM_STORE_NAME_PREFIX} and are
   * automatically managed by the Venice infrastructure. Examples include:
   * <ul>
   *   <li>Meta system stores - store metadata and schemas</li>
   *   <li>Push status system stores - track push job progress</li>
   * </ul>
   * <p>
   */
  SYSTEM,

  /**
   * Unknown or indeterminate store type.
   * <p>
   * This value is used as a fallback when:
   * <ul>
   *   <li>Store metadata is not yet available (e.g., during initialization)</li>
   *   <li>Version information cannot be resolved</li>
   *   <li>An error occurs while determining the store type</li>
   * </ul>
   * <p>
   * Operations should handle this type gracefully and may need to retry classification
   * once store metadata becomes available.
   */
  UNKNOWN;
}
