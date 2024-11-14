package com.linkedin.venice.stats;

import io.tehuti.Metric;
import io.tehuti.metrics.Measurable;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsReporter;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import java.io.Closeable;
import java.util.Map;
import java.util.Objects;


/** extends MetricsRepository to keep the changes to a minimum. Next step would be to create a MetricsRepository inside rather than extending it */
public class VeniceMetricsRepository extends MetricsRepository implements Closeable {
  private MetricsRepository delegate = null;
  private VeniceMetricsConfig veniceMetricsConfig;
  VeniceOpenTelemetryMetricsRepository openTelemetryMetricsRepository;

  public VeniceMetricsRepository() {
    super();
    this.veniceMetricsConfig = new VeniceMetricsConfig.VeniceMetricsConfigBuilder().build();
    this.openTelemetryMetricsRepository = new VeniceOpenTelemetryMetricsRepository(veniceMetricsConfig);
  }

  public VeniceMetricsRepository(VeniceMetricsConfig veniceMetricsConfig) {
    super(veniceMetricsConfig.getTehutiMetricConfig());
    this.veniceMetricsConfig = veniceMetricsConfig;
    this.openTelemetryMetricsRepository = new VeniceOpenTelemetryMetricsRepository(veniceMetricsConfig);
  }

  public VeniceMetricsRepository(
      VeniceMetricsConfig veniceMetricsConfig,
      VeniceOpenTelemetryMetricsRepository openTelemetryMetricsRepository) {
    super(veniceMetricsConfig.getTehutiMetricConfig());
    this.veniceMetricsConfig = veniceMetricsConfig;
    this.openTelemetryMetricsRepository = openTelemetryMetricsRepository;
  }

  /** if MetricsRepository is passed in, then use it as the delegate, can be removed after the migration */
  public VeniceMetricsRepository(MetricsRepository metricsRepository, VeniceMetricsConfig veniceMetricsConfig) {
    this.delegate = Objects.requireNonNull(metricsRepository);
    this.veniceMetricsConfig = veniceMetricsConfig;
    this.openTelemetryMetricsRepository = new VeniceOpenTelemetryMetricsRepository(veniceMetricsConfig);
  }

  public VeniceMetricsRepository(
      MetricsRepository metricsRepository,
      VeniceMetricsConfig veniceMetricsConfig,
      VeniceOpenTelemetryMetricsRepository openTelemetryMetricsRepository) {
    this.delegate = Objects.requireNonNull(metricsRepository);
    this.veniceMetricsConfig = veniceMetricsConfig;
    this.openTelemetryMetricsRepository = openTelemetryMetricsRepository;
  }

  public VeniceOpenTelemetryMetricsRepository getOpenTelemetryMetricsRepository() {
    return this.openTelemetryMetricsRepository;
  }

  public VeniceMetricsConfig getVeniceMetricsConfig() {
    return veniceMetricsConfig;
  }

  @Override
  public void close() {
    if (delegate != null) {
      delegate.close();
    } else {
      super.close();
    }
    openTelemetryMetricsRepository.close();
  }

  // all other overrides from MetricsRepository to use delegate
  @Override
  public Sensor getSensor(String name) {
    if (delegate != null) {
      return delegate.getSensor(name);
    } else {
      return super.getSensor(name);
    }
  }

  @Override
  public Sensor sensor(String name) {
    if (delegate != null) {
      return delegate.sensor(name);
    } else {
      return super.sensor(name);
    }
  }

  @Override
  public Sensor sensor(String name, Sensor... parents) {
    if (delegate != null) {
      return delegate.sensor(name, parents);
    } else {
      return super.sensor(name, parents);
    }
  }

  @Override
  public synchronized Sensor sensor(String name, MetricConfig config, Sensor... parents) {
    if (delegate != null) {
      return delegate.sensor(name, config, parents);
    } else {
      return super.sensor(name, config, parents);
    }
  }

  @Override
  public synchronized void removeSensor(String name) {
    if (delegate != null) {
      delegate.removeSensor(name);
    } else {
      super.removeSensor(name);
    }
  }

  @Override
  public Metric addMetric(String name, Measurable measurable) {
    if (delegate != null) {
      return delegate.addMetric(name, measurable);
    } else {
      return super.addMetric(name, measurable);
    }
  }

  @Override
  public Metric addMetric(String name, String description, Measurable measurable) {
    if (delegate != null) {
      return delegate.addMetric(name, description, measurable);
    } else {
      return super.addMetric(name, description, measurable);
    }
  }

  @Override
  public Metric addMetric(String name, MetricConfig config, Measurable measurable) {
    if (delegate != null) {
      return delegate.addMetric(name, config, measurable);
    } else {
      return super.addMetric(name, config, measurable);
    }
  }

  @Override
  public synchronized Metric addMetric(String name, String description, MetricConfig config, Measurable measurable) {
    if (delegate != null) {
      return delegate.addMetric(name, description, config, measurable);
    } else {
      return super.addMetric(name, description, config, measurable);
    }
  }

  @Override
  public synchronized void addReporter(MetricsReporter reporter) {
    if (delegate != null) {
      delegate.addReporter(reporter);
    } else {
      super.addReporter(reporter);
    }
  }

  @Override
  public Map<String, ? extends Metric> metrics() {
    if (delegate != null) {
      return delegate.metrics();
    } else {
      return super.metrics();
    }
  }

  @Override
  public Metric getMetric(String name) {
    if (delegate != null) {
      return delegate.getMetric(name);
    } else {
      return super.getMetric(name);
    }
  }

  @Override
  public AsyncGauge.AsyncGaugeExecutor getAsyncGaugeExecutor() {
    if (delegate != null) {
      return delegate.getAsyncGaugeExecutor();
    } else {
      return super.getAsyncGaugeExecutor();
    }
  }
}
