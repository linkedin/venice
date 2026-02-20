package com.linkedin.venice.stats.dimensions;

/**
 * Dimension enum representing the sub-phases of admin message processing pipelines.
 * Each value corresponds to a distinct phase within an admin message handler, enabling
 * per-phase latency breakdown within the
 * {@code controller.admin_consumption.message.processing_time_per_component} OTel histogram.
 *
 * <p>This dimension is paired with {@link VeniceMetricsDimensions#VENICE_ADMIN_MESSAGE_TYPE} so
 * that phases can be attributed to the specific admin message type being processed. Currently
 * the values below cover the AddVersion pipeline, but new values should be added here as other
 * admin message types gain per-phase instrumentation.
 *
 * Maps to {@link VeniceMetricsDimensions#VENICE_ADMIN_MESSAGE_PROCESSING_PHASE}.
 */
public enum AdminMessageProcessingPhase implements VeniceDimensionInterface {
  /** Time spent retiring outdated store versions before creating a new one. */
  RETIRE_OLD_VERSIONS,

  /** Time spent waiting for Helix to assign storage node resources for the new version. */
  RESOURCE_ASSIGNMENT_WAIT,

  /** Time spent handling version creation failures (cleanup, error propagation). */
  FAILURE_HANDLING,

  /** Time spent handling the existing source version (e.g., checking compatibility, preparing swap). */
  EXISTING_VERSION_HANDLING,

  /** Time spent sending the start-of-push signal to initiate data ingestion. */
  START_OF_PUSH,

  /** Time spent creating Kafka batch topics (version topics) for the new store version. */
  BATCH_TOPIC_CREATION,

  /** Time spent creating Helix storage cluster resources for partition assignment. */
  HELIX_RESOURCE_CREATION;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_ADMIN_MESSAGE_PROCESSING_PHASE;
  }
}
