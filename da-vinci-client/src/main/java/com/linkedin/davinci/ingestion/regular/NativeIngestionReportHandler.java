package com.linkedin.davinci.ingestion.regular;

import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.enums.IngestionAction;
import com.linkedin.venice.ingestion.protocol.enums.IngestionReportType;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.function.Consumer;
import org.apache.log4j.Logger;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.*;


/**
 * NativeIngestionReportHandler is the handler class for {@link NativeIngestionMonitorService}. It handles {@link IngestionTaskReport}
 * sent from child process and triggers corresponding notifier actions. For all these status, the handler will notify all
 * the registered notifiers in main process.
 */
public class NativeIngestionReportHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger logger = Logger.getLogger(NativeIngestionReportHandler.class);
  private static final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer = AvroProtocolDefinition.PARTITION_STATE.getSerializer();
  private final NativeIngestionMonitorService nativeIngestionMonitorService;

  public NativeIngestionReportHandler(NativeIngestionMonitorService nativeIngestionMonitorService) {
    logger.info("NativeIngestionReportHandler created.");
    this.nativeIngestionMonitorService = nativeIngestionMonitorService;
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) {
    // Decode ingestion report from incoming http request content.
    IngestionTaskReport report = deserializeIngestionActionRequest(IngestionAction.REPORT, readHttpRequestContent(msg));
    IngestionReportType ingestionReportType = IngestionReportType.valueOf(report.reportType);
    String topicName = report.topicName.toString();
    int partitionId = report.partitionId;
    long offset = report.offset;
    logger.info("Received ingestion report " + ingestionReportType.name() + " for topic: " + topicName + ", partition: " + partitionId + " from ingestion service.");
    // TODO: Use more flexible pull model to sync storage metadata from child process to main process.
    updateLocalStorageMetadata(report);
    // Relay the notification to parent service's listener.
    switch (ingestionReportType) {
      case COMPLETED:
        // TODO: Set leader state in local KafkaStoreIngestionService during server integration.
        nativeIngestionMonitorService.removeVersionPartitionFromIngestionMap(topicName, partitionId);
        notifierHelper(notifier -> notifier.completed(topicName, partitionId, report.offset));
        break;
      case ERROR:
        nativeIngestionMonitorService.removeVersionPartitionFromIngestionMap(topicName, partitionId);
        notifierHelper(notifier -> notifier.error(topicName, partitionId, report.message.toString(), new VeniceException(report.message.toString())));
        break;
      case STARTED:
        notifierHelper(notifier -> notifier.started(topicName, partitionId));
        break;
      case RESTARTED:
        notifierHelper(notifier -> notifier.restarted(topicName, partitionId, offset));
        break;
      case PROGRESS:
        notifierHelper(notifier -> notifier.progress(topicName, partitionId, offset));
        break;
      case END_OF_PUSH_RECEIVED:
        notifierHelper(notifier -> notifier.endOfPushReceived(topicName, partitionId, offset));
        break;
      case START_OF_BUFFER_REPLAY_RECEIVED:
        notifierHelper(notifier -> notifier.startOfBufferReplayReceived(topicName, partitionId, offset));
        break;
      case START_OF_INCREMENTAL_PUSH_RECEIVED:
        notifierHelper(notifier -> notifier.startOfIncrementalPushReceived(topicName, partitionId, offset));
        break;
      case END_OF_INCREMENTAL_PUSH_RECEIVED:
        notifierHelper(notifier -> notifier.endOfIncrementalPushReceived(topicName, partitionId, offset));
        break;
      case TOPIC_SWITCH_RECEIVED:
        notifierHelper(notifier -> notifier.topicSwitchReceived(topicName, partitionId, offset));
        break;
      default:
        logger.warn("Received unsupported ingestion report:\n" + report.toString() + "\n it will be ignored for now.");
    }
    ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.OK, getDummyContent()));
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    logger.error("Encounter exception during ingestion task report handling.", cause);
    ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, cause.getClass().getSimpleName() + "_" + cause.getMessage()));
    ctx.close();
  }

  private void notifierHelper(Consumer<VeniceNotifier> lambda) {
    nativeIngestionMonitorService.getPushStatusNotifierList().forEach(lambda);
    nativeIngestionMonitorService.getIngestionNotifierList().forEach(lambda);
  }

  private void updateLocalStorageMetadata(IngestionTaskReport report) {
    String topicName = report.topicName.toString();
    int partitionId = report.partitionId;
    long offset = report.offset;
    // Sync up offset record & store version state before report ingestion complete to parent process.
    if (nativeIngestionMonitorService.getStorageMetadataService() != null) {
      if (report.offsetRecord != null) {
        OffsetRecord offsetRecord = new OffsetRecord(report.offsetRecord.array(), partitionStateSerializer);
        nativeIngestionMonitorService.getStorageMetadataService().putOffsetRecord(topicName, partitionId, offsetRecord);
        logger.info("Updated offsetRecord for (topic, partition): " + topicName + " " + partitionId + " " + offsetRecord.toString());
      }
      if (report.storeVersionState != null) {
        StoreVersionState storeVersionState = IsolatedIngestionUtils.deserializeStoreVersionState(topicName, report.storeVersionState.array());
        nativeIngestionMonitorService.getStorageMetadataService().putStoreVersionState(topicName, storeVersionState);
        logger.info("Updated storeVersionState for topic: " + topicName + " " + storeVersionState.toString());
      }
    }
  }
}

