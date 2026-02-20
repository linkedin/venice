package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ADMIN_MESSAGE_PROCESSING_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ADMIN_MESSAGE_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.AdminMessageProcessingComponent;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateTwoEnums;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;


/**
 * Tracks per-component latency for the {@link AdminMessageType#ADD_VERSION ADD_VERSION} admin message pipeline.
 * <p>
 * All sensors share a single OTel metric entity
 * ({@link AddVersionLatencyOtelMetricEntity#ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME_PER_COMPONENT})
 * and are differentiated by the {@link AdminMessageProcessingComponent} dimension. This class always records with
 * {@link AdminMessageType#ADD_VERSION}; the {@code AdminMessageType} dimension exists on the metric so that other
 * admin message types can reuse the same metric entity for their own per-component breakdowns in the future.
 * <p>
 * The following components are tracked (all in milliseconds):
 * <ul>
 *   <li><b>retireOldVersions</b> –
 *       Measures the time taken to retire outdated store versions.</li>
 *   <li><b>resourceAssignmentWait</b> –
 *       Captures time spent waiting for node resource assignments.</li>
 *   <li><b>failureHandling</b> –
 *       Tracks latency during version creation failure handling.</li>
 *   <li><b>existingVersionHandling</b> –
 *       Measures latency for add version requests where the source version already exists.</li>
 *   <li><b>startOfPush</b> –
 *       Records the time taken to send the start-of-push signal.</li>
 *   <li><b>batchTopicCreation</b> –
 *       Tracks latency for creating batch topics (used by both child and parent controllers).</li>
 *   <li><b>helixResourceCreation</b> –
 *       Monitors the time required to create Helix storage cluster resources.</li>
 * </ul>
 */
public class AddVersionLatencyStats extends AbstractVeniceStats {
  /** Measures the time taken to retire outdated store versions. */
  private final MetricEntityStateTwoEnums<AdminMessageType, AdminMessageProcessingComponent> retireOldVersionsMetric;

  /** Captures the latency while waiting for node resource assignments. */
  private final MetricEntityStateTwoEnums<AdminMessageType, AdminMessageProcessingComponent> resourceAssignmentWaitMetric;

  /** Tracks latency during the handling of version creation failures. */
  private final MetricEntityStateTwoEnums<AdminMessageType, AdminMessageProcessingComponent> failureHandlingMetric;

  /** Measures latency for add version requests where the source version already exists. */
  private final MetricEntityStateTwoEnums<AdminMessageType, AdminMessageProcessingComponent> existingVersionHandlingMetric;

  /** Records the time taken to send the start-of-push signal. */
  private final MetricEntityStateTwoEnums<AdminMessageType, AdminMessageProcessingComponent> startOfPushMetric;

  /** Tracks latency for creating batch topics, applicable to both parent and child controllers. */
  private final MetricEntityStateTwoEnums<AdminMessageType, AdminMessageProcessingComponent> batchTopicCreationMetric;

  /** Monitors the time required to create Helix storage cluster resources. */
  private final MetricEntityStateTwoEnums<AdminMessageType, AdminMessageProcessingComponent> helixResourceCreationMetric;

  public AddVersionLatencyStats(MetricsRepository metricsRepository, String clusterName) {
    super(metricsRepository, clusterName);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();

    retireOldVersionsMetric = MetricEntityStateTwoEnums.create(
        AddVersionLatencyOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME_PER_COMPONENT
            .getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_RETIRE_OLD_VERSIONS_LATENCY,
        Arrays.asList(
            new Avg(),
            new Max(),
            TehutiUtils.getPercentileStat(
                getName() + AbstractVeniceStats.DELIMITER
                    + AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_RETIRE_OLD_VERSIONS_LATENCY.getMetricName())),
        baseDimensionsMap,
        AdminMessageType.class,
        AdminMessageProcessingComponent.class);

    resourceAssignmentWaitMetric = MetricEntityStateTwoEnums.create(
        AddVersionLatencyOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME_PER_COMPONENT
            .getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_RESOURCE_ASSIGNMENT_WAIT_LATENCY,
        Arrays.asList(
            new Avg(),
            new Max(),
            TehutiUtils.getPercentileStat(
                getName() + AbstractVeniceStats.DELIMITER
                    + AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_RESOURCE_ASSIGNMENT_WAIT_LATENCY
                        .getMetricName())),
        baseDimensionsMap,
        AdminMessageType.class,
        AdminMessageProcessingComponent.class);

    failureHandlingMetric = MetricEntityStateTwoEnums.create(
        AddVersionLatencyOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME_PER_COMPONENT
            .getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_CREATION_FAILURE_LATENCY,
        Arrays.asList(
            new Avg(),
            new Max(),
            TehutiUtils.getPercentileStat(
                getName() + AbstractVeniceStats.DELIMITER
                    + AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_CREATION_FAILURE_LATENCY.getMetricName())),
        baseDimensionsMap,
        AdminMessageType.class,
        AdminMessageProcessingComponent.class);

    existingVersionHandlingMetric = MetricEntityStateTwoEnums.create(
        AddVersionLatencyOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME_PER_COMPONENT
            .getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_EXISTING_SOURCE_HANDLING_LATENCY,
        Arrays.asList(
            new Avg(),
            new Max(),
            TehutiUtils.getPercentileStat(
                getName() + AbstractVeniceStats.DELIMITER
                    + AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_EXISTING_SOURCE_HANDLING_LATENCY
                        .getMetricName())),
        baseDimensionsMap,
        AdminMessageType.class,
        AdminMessageProcessingComponent.class);

    startOfPushMetric = MetricEntityStateTwoEnums.create(
        AddVersionLatencyOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME_PER_COMPONENT
            .getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_START_OF_PUSH_LATENCY,
        Arrays.asList(
            new Avg(),
            new Max(),
            TehutiUtils.getPercentileStat(
                getName() + AbstractVeniceStats.DELIMITER
                    + AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_START_OF_PUSH_LATENCY.getMetricName())),
        baseDimensionsMap,
        AdminMessageType.class,
        AdminMessageProcessingComponent.class);

    batchTopicCreationMetric = MetricEntityStateTwoEnums.create(
        AddVersionLatencyOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME_PER_COMPONENT
            .getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_BATCH_TOPIC_CREATION_LATENCY,
        Arrays.asList(
            new Avg(),
            new Max(),
            TehutiUtils.getPercentileStat(
                getName() + AbstractVeniceStats.DELIMITER
                    + AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_BATCH_TOPIC_CREATION_LATENCY.getMetricName())),
        baseDimensionsMap,
        AdminMessageType.class,
        AdminMessageProcessingComponent.class);

    helixResourceCreationMetric = MetricEntityStateTwoEnums.create(
        AddVersionLatencyOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME_PER_COMPONENT
            .getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_HELIX_RESOURCE_CREATION_LATENCY,
        Arrays.asList(
            new Avg(),
            new Max(),
            TehutiUtils.getPercentileStat(
                getName() + AbstractVeniceStats.DELIMITER
                    + AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_HELIX_RESOURCE_CREATION_LATENCY
                        .getMetricName())),
        baseDimensionsMap,
        AdminMessageType.class,
        AdminMessageProcessingComponent.class);
  }

  public void recordRetireOldVersionsLatency(long latency) {
    retireOldVersionsMetric
        .record(latency, AdminMessageType.ADD_VERSION, AdminMessageProcessingComponent.RETIRE_OLD_VERSIONS);
  }

  public void recordResourceAssignmentWaitLatency(long latency) {
    resourceAssignmentWaitMetric
        .record(latency, AdminMessageType.ADD_VERSION, AdminMessageProcessingComponent.RESOURCE_ASSIGNMENT_WAIT);
  }

  public void recordExistingSourceVersionHandlingLatency(long latency) {
    existingVersionHandlingMetric
        .record(latency, AdminMessageType.ADD_VERSION, AdminMessageProcessingComponent.EXISTING_VERSION_HANDLING);
  }

  public void recordStartOfPushLatency(long latency) {
    startOfPushMetric.record(latency, AdminMessageType.ADD_VERSION, AdminMessageProcessingComponent.START_OF_PUSH);
  }

  public void recordBatchTopicCreationLatency(long latency) {
    batchTopicCreationMetric
        .record(latency, AdminMessageType.ADD_VERSION, AdminMessageProcessingComponent.BATCH_TOPIC_CREATION);
  }

  public void recordHelixResourceCreationLatency(long latency) {
    helixResourceCreationMetric
        .record(latency, AdminMessageType.ADD_VERSION, AdminMessageProcessingComponent.HELIX_RESOURCE_CREATION);
  }

  public void recordVersionCreationFailureLatency(long latency) {
    failureHandlingMetric
        .record(latency, AdminMessageType.ADD_VERSION, AdminMessageProcessingComponent.FAILURE_HANDLING);
  }

  enum AddVersionLatencyTehutiMetricNameEnum implements TehutiMetricNameEnum {
    ADD_VERSION_RETIRE_OLD_VERSIONS_LATENCY, ADD_VERSION_RESOURCE_ASSIGNMENT_WAIT_LATENCY,
    ADD_VERSION_CREATION_FAILURE_LATENCY, ADD_VERSION_EXISTING_SOURCE_HANDLING_LATENCY,
    ADD_VERSION_START_OF_PUSH_LATENCY, ADD_VERSION_BATCH_TOPIC_CREATION_LATENCY,
    ADD_VERSION_HELIX_RESOURCE_CREATION_LATENCY
  }

  public enum AddVersionLatencyOtelMetricEntity implements ModuleMetricEntityInterface {
    /**
     * Per-component breakdown of start-to-end processing time for admin messages. The
     * {@link com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions#VENICE_ADMIN_MESSAGE_TYPE AdminMessageType}
     * dimension identifies the admin message type and the
     * {@link com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions#VENICE_ADMIN_MESSAGE_PROCESSING_COMPONENT
     * AdminMessageProcessingComponent} dimension identifies the processing step. Currently only
     * {@link AdminMessageType#ADD_VERSION ADD_VERSION} records against this metric (via this class); other admin
     * message types can adopt it when per-component tracking is added for them.
     */
    ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME_PER_COMPONENT(
        "admin_consumption.message.phase.start_to_end_processing.time_per_component", MetricType.HISTOGRAM,
        MetricUnit.MILLISECOND, "Per-component breakdown of start-to-end processing time",
        setOf(VENICE_CLUSTER_NAME, VENICE_ADMIN_MESSAGE_TYPE, VENICE_ADMIN_MESSAGE_PROCESSING_COMPONENT)
    );

    private final MetricEntity metricEntity;

    AddVersionLatencyOtelMetricEntity(
        String metricName,
        MetricType metricType,
        MetricUnit unit,
        String description,
        Set<VeniceMetricsDimensions> dimensionsList) {
      this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensionsList);
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }
}
