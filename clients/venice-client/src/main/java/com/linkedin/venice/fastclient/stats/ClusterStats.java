package com.linkedin.venice.fastclient.stats;

import static com.linkedin.venice.fastclient.stats.ClusterMetricEntity.INSTANCE_ERROR_COUNT;
import static com.linkedin.venice.fastclient.stats.ClusterMetricEntity.STORE_VERSION_CURRENT;
import static com.linkedin.venice.fastclient.stats.ClusterMetricEntity.STORE_VERSION_UPDATE_FAILURE_COUNT;
import static com.linkedin.venice.stats.dimensions.InstanceErrorType.BLOCKED;
import static com.linkedin.venice.stats.dimensions.InstanceErrorType.OVERLOADED;
import static com.linkedin.venice.stats.dimensions.InstanceErrorType.UNHEALTHY;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.InstanceErrorType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class includes the metrics in the cluster-level.
 * So far it is per store.
 */
public class ClusterStats extends AbstractVeniceStats {
  private static final Logger LOGGER = LogManager.getLogger(ClusterStats.class);

  private final String storeName;
  private final MetricEntityStateBase versionUpdateFailureCount;
  private final AsyncMetricEntityStateBase currentVersionNumber;

  // OTel metrics for instance error counts
  private final MetricEntityStateOneEnum<InstanceErrorType> blockedInstanceErrorCount;
  private final MetricEntityStateOneEnum<InstanceErrorType> unhealthyInstanceErrorCount;
  private final MetricEntityStateOneEnum<InstanceErrorType> overloadedInstanceErrorCount;
  private final AtomicLong currentVersion = new AtomicLong(-1);

  // OTel support
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private final Attributes baseAttributes;

  public ClusterStats(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);
    this.storeName = storeName;

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            // set all base dimensions for this stats class and build
            .setStoreName(storeName)
            .build();

    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();
    this.baseAttributes = otelData.getBaseAttributes();

    // Initialize OTel metrics
    this.versionUpdateFailureCount = MetricEntityStateBase.create(
        STORE_VERSION_UPDATE_FAILURE_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ClusterTehutiMetricName.VERSION_UPDATE_FAILURE,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        baseAttributes);

    this.currentVersionNumber = AsyncMetricEntityStateBase.create(
        STORE_VERSION_CURRENT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ClusterTehutiMetricName.CURRENT_VERSION,
        Collections.singletonList(
            new AsyncGauge(
                (ignored, ignored2) -> this.currentVersion.get(),
                ClusterTehutiMetricName.CURRENT_VERSION.getMetricName())),
        baseDimensionsMap,
        baseAttributes,
        this.currentVersion::get);

    // Initialize OTel metrics for instance error counts
    this.blockedInstanceErrorCount = MetricEntityStateOneEnum.create(
        INSTANCE_ERROR_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ClusterTehutiMetricName.BLOCKED_INSTANCE_COUNT,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        InstanceErrorType.class);

    this.unhealthyInstanceErrorCount = MetricEntityStateOneEnum.create(
        INSTANCE_ERROR_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ClusterTehutiMetricName.UNHEALTHY_INSTANCE_COUNT,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        InstanceErrorType.class);

    this.overloadedInstanceErrorCount = MetricEntityStateOneEnum.create(
        INSTANCE_ERROR_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ClusterTehutiMetricName.OVERLOADED_INSTANCE_COUNT,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        InstanceErrorType.class);
  }

  public void recordBlockedInstanceCount(int count) {
    blockedInstanceErrorCount.record(count, BLOCKED);
  }

  public void recordUnhealthyInstanceCount(int count) {
    unhealthyInstanceErrorCount.record(count, UNHEALTHY);
  }

  public void recordOverloadedInstanceCount(int count) {
    overloadedInstanceErrorCount.record(count, OVERLOADED);
  }

  public void updateCurrentVersion(int currentVersion) {
    this.currentVersion.set(currentVersion);
  }

  public void recordVersionUpdateFailure() {
    versionUpdateFailureCount.record(1);
  }

  public List<Double> getMetricValues(String sensorName, String... stats) {
    String sensorFullName = getSensorFullName(sensorName);
    List<Double> collect = Arrays.stream(stats).map((stat) -> {
      Metric metric = getMetricsRepository().getMetric(sensorFullName + "." + stat);
      return (metric != null ? metric.value() : Double.NaN);
    }).collect(Collectors.toList());
    return collect;
  }

  /**
   * Metric names for tehuti metrics used in this class.
   */
  public enum ClusterTehutiMetricName implements TehutiMetricNameEnum {
    VERSION_UPDATE_FAILURE, CURRENT_VERSION, BLOCKED_INSTANCE_COUNT, UNHEALTHY_INSTANCE_COUNT,
    OVERLOADED_INSTANCE_COUNT;

    private final String metricName;

    ClusterTehutiMetricName() {
      this.metricName = name().toLowerCase();
    }

    @Override
    public String getMetricName() {
      return this.metricName;
    }
  }
}
