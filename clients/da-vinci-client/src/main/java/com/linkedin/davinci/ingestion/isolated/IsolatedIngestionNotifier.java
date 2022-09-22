package com.linkedin.davinci.ingestion.isolated;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.createIngestionTaskReport;

import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.enums.IngestionReportType;


public class IsolatedIngestionNotifier implements VeniceNotifier {
  private final IsolatedIngestionServer isolatedIngestionServer;

  public IsolatedIngestionNotifier(IsolatedIngestionServer isolatedIngestionServer) {
    this.isolatedIngestionServer = isolatedIngestionServer;
  }

  @Override
  public void completed(String kafkaTopic, int partitionId, long offset, String message) {
    IngestionTaskReport report =
        createIngestionTaskReport(IngestionReportType.COMPLETED, kafkaTopic, partitionId, offset, message);
    isolatedIngestionServer.reportIngestionStatus(report);
  }

  @Override
  public void error(String kafkaTopic, int partitionId, String message, Exception e) {
    IngestionTaskReport report = createIngestionTaskReport(
        IngestionReportType.ERROR,
        kafkaTopic,
        partitionId,
        e.getClass().getSimpleName() + "_" + e.getMessage());
    isolatedIngestionServer.reportIngestionStatus(report);
  }

  @Override
  public void started(String kafkaTopic, int partitionId, String message) {
    IngestionTaskReport report =
        createIngestionTaskReport(IngestionReportType.STARTED, kafkaTopic, partitionId, message);
    isolatedIngestionServer.reportIngestionStatus(report);
  }

  @Override
  public void restarted(String kafkaTopic, int partitionId, long offset, String message) {
    IngestionTaskReport report =
        createIngestionTaskReport(IngestionReportType.RESTARTED, kafkaTopic, partitionId, offset, message);
    isolatedIngestionServer.reportIngestionStatus(report);
  }

  @Override
  public void endOfPushReceived(String kafkaTopic, int partitionId, long offset, String message) {
    IngestionTaskReport report =
        createIngestionTaskReport(IngestionReportType.END_OF_PUSH_RECEIVED, kafkaTopic, partitionId, offset, message);
    isolatedIngestionServer.reportIngestionStatus(report);
  }

  @Override
  public void startOfIncrementalPushReceived(
      String kafkaTopic,
      int partitionId,
      long offset,
      String incrementalPushVersion) {
    IngestionTaskReport report = createIngestionTaskReport(
        IngestionReportType.START_OF_INCREMENTAL_PUSH_RECEIVED,
        kafkaTopic,
        partitionId,
        offset,
        incrementalPushVersion);
    isolatedIngestionServer.reportIngestionStatus(report);
  }

  @Override
  public void endOfIncrementalPushReceived(
      String kafkaTopic,
      int partitionId,
      long offset,
      String incrementalPushVersion) {
    IngestionTaskReport report = createIngestionTaskReport(
        IngestionReportType.END_OF_INCREMENTAL_PUSH_RECEIVED,
        kafkaTopic,
        partitionId,
        offset,
        incrementalPushVersion);
    isolatedIngestionServer.reportIngestionStatus(report);
  }

  @Override
  public void topicSwitchReceived(String kafkaTopic, int partitionId, long offset, String message) {
    IngestionTaskReport report =
        createIngestionTaskReport(IngestionReportType.TOPIC_SWITCH_RECEIVED, kafkaTopic, partitionId, offset, message);
    isolatedIngestionServer.reportIngestionStatus(report);
  }

  @Override
  public void dataRecoveryCompleted(String kafkaTopic, int partitionId, long offset, String message) {
    IngestionTaskReport report = createIngestionTaskReport(
        IngestionReportType.DATA_RECOVERY_COMPLETED,
        kafkaTopic,
        partitionId,
        offset,
        message);
    isolatedIngestionServer.reportIngestionStatus(report);
  }

  @Override
  public void progress(String kafkaTopic, int partitionId, long offset, String message) {
    IngestionTaskReport report =
        createIngestionTaskReport(IngestionReportType.PROGRESS, kafkaTopic, partitionId, offset, message);
    isolatedIngestionServer.reportIngestionStatus(report);
  }

}
