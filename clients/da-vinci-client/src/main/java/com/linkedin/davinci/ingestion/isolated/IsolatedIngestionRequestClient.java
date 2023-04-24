package com.linkedin.davinci.ingestion.isolated;

import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_REQUEST_TIMEOUT_SECONDS;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.HttpClientTransport;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.venice.ingestion.protocol.IngestionMetricsReport;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.enums.IngestionAction;
import com.linkedin.venice.ingestion.protocol.enums.IngestionReportType;
import com.linkedin.venice.security.SSLFactory;
import java.io.Closeable;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class sends requests to monitor service in main process and retrieves responses.
 */
public class IsolatedIngestionRequestClient implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(IsolatedIngestionRequestClient.class);

  private HttpClientTransport httpClientTransport;

  public IsolatedIngestionRequestClient(VeniceConfigLoader configLoader) {
    Optional<SSLFactory> sslFactory = IsolatedIngestionUtils.getSSLFactory(configLoader);
    int port = configLoader.getVeniceServerConfig().getIngestionApplicationPort();
    int requestTimeoutInSeconds =
        configLoader.getCombinedProperties().getInt(SERVER_INGESTION_ISOLATION_REQUEST_TIMEOUT_SECONDS, 120);
    httpClientTransport = new HttpClientTransport(sslFactory, port, requestTimeoutInSeconds);
  }

  public boolean reportIngestionStatus(IngestionTaskReport report) {
    String topicName = report.topicName.toString();
    int partitionId = report.partitionId;
    // Avoid sending binary data in OffsetRecord and pollute logs.
    LOGGER.info(
        "Sending ingestion report {}, isPositive: {}, message: {} for partition: {} of topic: {} at offset: {}",
        IngestionReportType.valueOf(report.reportType),
        report.isPositive,
        report.message,
        partitionId,
        topicName,
        report.offset);
    try {
      httpClientTransport.sendRequest(IngestionAction.REPORT, report);
      return true;
    } catch (Exception e) {
      LOGGER.warn("Failed to send report with exception for topic: {}, partition: {}", topicName, partitionId, e);
      return false;
    }
  }

  public void reportMetricUpdate(IngestionMetricsReport report) {
    try {
      httpClientTransport.sendRequest(IngestionAction.METRIC, report);
    } catch (Exception e) {
      LOGGER.warn("Failed to send metrics update with exception", e);
    }
  }

  // Visible for testing
  protected void setHttpClientTransport(HttpClientTransport clientTransport) {
    this.httpClientTransport = clientTransport;
  }

  @Override
  public void close() {
    httpClientTransport.close();
  }
}
