package com.linkedin.venice.ingestion;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;


/**
 * IngestionProcessStats is a metrics collecting class that aims to collect metrics from isolated ingestion process.
 * It maintains a map of String to Double to keep the collected metrics from forked process. updateMetricMap() will be called
 * by a dedicated thread which collects metrics from child process at a fixed rate.
 * Metrics collected will be registered to main process's MetricRepository and be reported to metrics reporters(like InGraph).
 * In order to distinguish the collected metrics from the main process metrics, prefix "ingestion_isolation" is added to every metric names.
 */
public class IngestionProcessStats extends AbstractVeniceStats {
    private static final Logger logger = Logger.getLogger(IngestionProcessStats.class);

    private static final String METRIC_PREFIX = "ingestion_isolation";

    private final Map<String, Double> metricValueMap = new HashMap<>();

    public IngestionProcessStats(MetricsRepository metricsRepository) {
        super(metricsRepository, METRIC_PREFIX);
    }

    public void updateMetricMap(Map<CharSequence, Double> updateMetricValueMap) {
        updateMetricValueMap.forEach((name, value) -> {
            /**
             * Metric name from the isolated ingestion process may contain attribute name like METRIC_NAME.AVG
             * And it will cause the reporter to parse the metric name wrongly and fail to add to reporter, thus
             * here we replace the dot separator in metric name with underscore.
             */
            String correctedMetricName = name.toString().replace('.', '_');
            if (!metricValueMap.containsKey(correctedMetricName)) {
                /**
                 * Although different metrics is set up for different purpose (like AVG, MAX, COUNT), the calculation is
                 * done in child process side, so we don't know how they are calculated and we don't need to know,
                 * all we need here is just to add it to the value map for reporter retrieval, so a Gauge lambda expression
                 * is enough.
                 */
                registerSensor(correctedMetricName, new Gauge(() -> this.metricValueMap.get(correctedMetricName)));
                logger.info("Registering metric: " + correctedMetricName);
            }
            metricValueMap.put(correctedMetricName, value);
        });
    }
}
