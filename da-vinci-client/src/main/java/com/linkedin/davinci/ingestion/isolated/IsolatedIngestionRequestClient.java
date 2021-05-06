package com.linkedin.davinci.ingestion.isolated;

import com.linkedin.davinci.ingestion.IngestionRequestTransport;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.enums.IngestionAction;
import java.io.Closeable;
import org.apache.log4j.Logger;

/**
 * IsolatedIngestionRequestClient sends requests to monitor service in main process and retrieves responses.
 */
public class IsolatedIngestionRequestClient implements Closeable {
  private static final Logger logger = Logger.getLogger(IsolatedIngestionRequestClient.class);

  private final IngestionRequestTransport ingestionRequestTransport;

  public IsolatedIngestionRequestClient(int port) {
    ingestionRequestTransport = new IngestionRequestTransport(port);
  }

  public void reportIngestionTask(IngestionTaskReport report) {
    String topicName = report.topicName.toString();
    int partitionId = report.partitionId;
    logger.info("Sending ingestion report: " + report);
    try {
      ingestionRequestTransport.sendRequest(IngestionAction.REPORT, report);
    } catch (Exception e) {
      logger.warn("Failed to send report with exception for topic: " + topicName + ", partition: " + partitionId , e);
    }
  }

  @Override
  public void close() {
    ingestionRequestTransport.close();
  }
}