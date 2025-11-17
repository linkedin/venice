package com.linkedin.venice.meta;

/**
 * Enumeration of Venice store types used to classify stores based on
 * their ingestion patterns and operational characteristics.
 */
public enum VeniceStoreType {

  /**
   * Batch-only stores populated exclusively through offline batch processes.
   *
   * <p>Data sources include:
   * <ul>
   *   <li>Offline push jobs (Hadoop or Spark)</li>
   *   <li>Samza batch reprocessing jobs</li>
   * </ul>
   *
   * <p>These stores do not support real-time updates or incremental pushes.
   * Any data change requires a full batch push, which atomically replaces
   * the previous store version.
   */
  BATCH,

  /**
   * Hybrid stores that ingest data from both batch and real-time sources.
   *
   * <p>Supported ingestion paths:
   * <ul>
   *   <li>Offline push jobs (Hadoop or Spark) for bulk data</li>
   *   <li>Samza real-time pipelines for streaming updates</li>
   *   <li>Incremental pushes from Venice push jobs</li>
   *   <li>Online application writes for direct real-time updates</li>
   * </ul>
   *
   * <p>Batch pushes create a new version that serves as the baseline snapshot.
   * Real-time and incremental updates are applied on top of that version,
   * allowing hybrid stores to serve fresh data by combining batch baselines
   * with continuous updates.
   */
  HYBRID,

  /**
   * System stores used internally by Venice for metadata, operational state,
   * and control plane information.
   *
   * <p>System store names are prefixed with
   * {@link Store#SYSTEM_STORE_NAME_PREFIX} and are automatically managed by
   * Venice infrastructure. Examples include:
   * <ul>
   *   <li>Meta system stores for schemas and metadata</li>
   *   <li>Push status system stores for tracking push job progress</li>
   * </ul>
   */
  SYSTEM,

  /**
   * Unknown or indeterminate store type.
   *
   * <p>This type is used when:
   * <ul>
   *   <li>Store metadata is not yet available (for example, during startup)</li>
   *   <li>Version information cannot be resolved</li>
   *   <li>An error occurs while determining the store type</li>
   * </ul>
   *
   * <p>Callers should handle this type gracefully and retry classification
   * once metadata becomes available.
   */
  UNKNOWN
}
