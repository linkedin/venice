package com.linkedin.venice.stats;

/**
 * This enum tracks the error codes that we use to report anomalies in metrics.
 *
 * Background: there are various edge cases, both intentional and unintentional,
 * that can cause us to not emit a metric, either because we don't want to or
 * because we cannot. Historically, we had many such edge cases that would make
 * a metric default to zero. This is problematic when debugging why a metric is
 * not acting right, since we cannot easily disambiguate between the various edge
 * cases.
 *
 * In order to ease the debugging of metric anomalies, we are using this enum to
 * track which code paths emit which error code. This ensures that we don't use
 * the same sentinel values for two different meanings.
 *
 * By convention, because most, if not all, metrics are positive, we will use
 * negative values for the error codes. We'll start at -10 and go down from there.
 *
 * If we need these error codes outside of the server module, we can move it out.
 */
public enum StatsErrorCode {
  /**
   * The original metrics implementation dealt with the problem of proliferation
   * of transient metrics in downstream reporting systems by only reporting on
   * one of the store versions, and by avoiding the inclusion of the version number
   * as part of the metric name.
   *
   * This has since been superseded by a new versioned pattern where we maintain
   * backup/current/future metrics in order to abstract away the specific version
   * number while still providing visibility into the various versions. Once we
   * migrate all relevant metrics to this new pattern, then this error code should
   * not be used anymore. Until then, we might see this.
   *
   * This should only happen because of a race condition where the
   * {@link io.tehuti.metrics.MetricsReporter} queries a metric while the
   * {@link StoreIngestionService} has identified
   * that this is an old push but hasn't switched to the new push yet.
   */
  STORE_VERSION_SHOULD_NOT_EMIT_METRICS(-10),

  /**
   * Some metrics only make sense in the context of hybrid stores and should not
   * be queried otherwise. If they are queried regardless, then we will emit this
   * error code.
   */
  METRIC_ONLY_AVAILABLE_FOR_HYBRID_STORES(-11),

  /**
   * Some metrics rely on the data contained in:
   *
   * {@link com.linkedin.venice.kafka.protocol.state.StoreVersionState}
   *
   * If this state is not available, the metric cannot be computed and this error
   * code will be emitted.
   */
  STORE_VERSION_STATE_UNAVAILABLE(-12),

  /**
   * Some metrics aggregate data across many partitions.
   *
   * If no partitions are subscribed, then we may be querying that metric in a
   * situation where it is inappropriate to do so, hence we will report this error
   * code.
   */
  NO_SUBSCRIBED_PARTITION(-13),

  /**
   * Inactive {@link StoreIngestionTask} instances
   * should not be asked for metrics in the first place, but in case they are, there
   * is a check that will prevent them from being interrogated. If this happens, this
   * error code will be sent.
   */
  INACTIVE_STORE_INGESTION_TASK(-14),

  /**
   * When the {@link DIVStatsReporter} attempts to get stats, it tries to get hold of
   * an instance of {@link DIVStats} which should have been set previously. If this
   * instance has not been set, then it will be null, and this error code will be sent.
   *
   * This error code is expected for metrics related to store-versions which don't exist,
   * such as:
   *
   * 1. the future version when there is no ongoing push,
   * 2. the backup version for a store which contains only one or zero versions, or
   * 3. the current version for a store which contains no versions at all.
   *
   * There may also be unexpected scenarios that cause this instance to be null...
   *
   * TODO: Need to find a better way to disambiguate the expected and unexpected case...
   */
  NULL_DIV_STATS(-15),

  /**
   * The BDB stats depend on getting a handle of {@link com.sleepycat.je.Environment}.
   * If the instance is null, then this error code will be sent.
   */
  NULL_BDB_ENVIRONMENT(-16),

  /**
   * Since storage engine stats has been migrated to versioned stats. It would encounter
   * the similar edge cases as {@link DIVStatsReporter} has. Check {@link #NULL_DIV_STATS}
   * for more details.
   */
  NULL_BDB_STATS(-17),

  /**
   * Check {@link AggVersionedStorageEngineStats} to find more details.
   */
  NULL_STORAGE_ENGINE_STATS(-18),

  NOT_SUPPORTED(-19),

  /**
   * Used by {@link AggVersionedStorageIngestionStats} when the stats reporter fetches a null stats.
   */
  NULL_INGESTION_STATS(-20),

  /**
   * Some metrics only make sense in the context of L/F stores and should not
   * be queried otherwise. If they are queried regardless, then we will emit this
   * error code. We deliberately set it value to be 0 since it's mostly benign. If
   * the resource doesn't belong to L/F model, it shouldn't encounter any L/F error :D
   */
  METRIC_ONLY_AVAILABLE_FOR_LEADER_FOLLOWER_STORES(0),

  /**
   * This is bubbled up when write compute adapter fails to get the current
   * value from the storage engine.
   */
  WRITE_COMPUTE_DESERIALIZATION_FAILURE(-21),

  /**
   * This is bubbled up when write compute adapter fails to perform update
   * operations on top of the current value.
   */
  WRITE_COMPUTE_UPDATE_FAILURE(-22),

  /**
   * This may be used when kafka topic's offset lag measurement may fail due to any reason.
   */
  LAG_MEASUREMENT_FAILURE(-23),

  /**
   * Default value for kafka client metrics. This is used when emitting metric configured via {@link ConfigKeys#KAFKA_PRODUCER_METRICS}
   * and that metric is missing from those returned by the Kafka client.
   */
  KAFKA_CLIENT_METRICS_DEFAULT(-24),

  /**
   * There was an exception when retrieving a metric value.  Please consult application logs to determine the root cause!
   */
  UNKNOWN_METRIC_EXCEPTION(-25),

  /**
   * This metric should not be emitted as it is a metric specific to an A/A store.
   */
  ACTIVE_ACTIVE_NOT_ENABLED(-25);

  public final int code;

  StatsErrorCode(int code) {
    this.code = code;
  }
}
