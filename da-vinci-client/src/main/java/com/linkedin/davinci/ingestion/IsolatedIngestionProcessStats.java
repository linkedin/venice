package com.linkedin.davinci.ingestion;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * IsolatedIngestionProcessStats is a metrics collecting class that aims to collect metrics from isolated ingestion process.
 * It maintains a map of String to Double to keep the collected metrics from forked process. updateMetricMap() will be called
 * by a dedicated thread which collects metrics from child process at a fixed rate.
 * Metrics collected will be registered to main process's MetricRepository and be reported to metrics reporters(like InGraph).
 * In order to distinguish the collected metrics from the main process metrics, prefix "ingestion_isolation" is added to every metric names.
 */
public class IsolatedIngestionProcessStats extends AbstractVeniceStats {
    private static final Logger logger = LogManager.getLogger(IsolatedIngestionProcessStats.class);

    private static final String METRIC_PREFIX = "ingestion_isolation";

    private final Map<String, Double> metricValueMap = new HashMap<>();

    public IsolatedIngestionProcessStats(MetricsRepository metricsRepository) {
        super(metricsRepository, METRIC_PREFIX);
    }

    public void updateMetricMap(Map<CharSequence, Double> updateMetricValueMap) {
        Set<String> newMetricNameSet = new HashSet<>();
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
                newMetricNameSet.add(correctedMetricName);
            }
            metricValueMap.put(correctedMetricName, value);
        });
        if (!newMetricNameSet.isEmpty()) {
            logger.info("Registered " + newMetricNameSet.size() + " new metrics.");
            logger.debug("New metrics list: " + newMetricNameSet.toString());
        }
    }
}
