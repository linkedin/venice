package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_BLOB_TRANSFER_SOURCE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class BlobTransferOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(BlobTransferOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<BlobTransferOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<BlobTransferOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        BlobTransferOtelMetricEntity.REQUEST_COUNT,
        new MetricEntityExpectation(
            "ingestion.blob_transfer.request.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of blob transfer requests by source and status",
            setOf(
                VENICE_STORE_NAME,
                VENICE_CLUSTER_NAME,
                VENICE_VERSION_ROLE,
                VENICE_BLOB_TRANSFER_SOURCE,
                VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    map.put(
        BlobTransferOtelMetricEntity.KAFKA_FALLBACK_NO_CANDIDATES_COUNT,
        new MetricEntityExpectation(
            "ingestion.blob_transfer.kafka_fallback.no_candidates.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of Kafka fallbacks because no blob candidates were discovered",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)));
    map.put(
        BlobTransferOtelMetricEntity.KAFKA_FALLBACK_FAILED_HOSTS_COUNT,
        new MetricEntityExpectation(
            "ingestion.blob_transfer.kafka_fallback.failed_hosts.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of Kafka fallbacks after all usable blob hosts failed",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)));
    map.put(
        BlobTransferOtelMetricEntity.RESPONSE_COUNT,
        new MetricEntityExpectation(
            "ingestion.blob_transfer.response.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of blob transfer responses by status (success/fail)",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    map.put(
        BlobTransferOtelMetricEntity.TIME,
        new MetricEntityExpectation(
            "ingestion.blob_transfer.time",
            MetricType.HISTOGRAM,
            MetricUnit.SECOND,
            "Blob transfer time in seconds",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)));
    map.put(
        BlobTransferOtelMetricEntity.BYTES_RECEIVED,
        new MetricEntityExpectation(
            "ingestion.blob_transfer.bytes.received",
            MetricType.COUNTER,
            MetricUnit.BYTES,
            "Bytes received via blob transfer",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)));
    map.put(
        BlobTransferOtelMetricEntity.BYTES_SENT,
        new MetricEntityExpectation(
            "ingestion.blob_transfer.bytes.sent",
            MetricType.COUNTER,
            MetricUnit.BYTES,
            "Bytes sent via blob transfer",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)));
    return map;
  }
}
