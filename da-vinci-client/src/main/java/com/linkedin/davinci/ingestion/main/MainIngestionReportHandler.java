package com.linkedin.davinci.ingestion.main;

import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
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
import com.linkedin.venice.utils.PartitionUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.log4j.Logger;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.*;


/**
 * MainIngestionReportHandler is the handler class for {@link MainIngestionMonitorService}. It handles {@link IngestionTaskReport}
 * sent from child process and triggers corresponding notifier actions. For all these status, the handler will notify all
 * the registered notifiers in main process.
 */
public class MainIngestionReportHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger logger = Logger.getLogger(MainIngestionReportHandler.class);
  private static final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer = AvroProtocolDefinition.PARTITION_STATE.getSerializer();
  private final MainIngestionMonitorService mainIngestionMonitorService;

  public MainIngestionReportHandler(MainIngestionMonitorService mainIngestionMonitorService) {
    logger.info("MainIngestionReportHandler created.");
    this.mainIngestionMonitorService = mainIngestionMonitorService;
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
    int amplificationFactor = PartitionUtils.getAmplificationFactor(mainIngestionMonitorService.getStoreRepository(), topicName);
    // TODO: Use more flexible pull model to sync storage metadata from child process to main process.
    updateLocalStorageMetadata(report, amplificationFactor);
    // Relay the notification to parent service's listener.
    switch (ingestionReportType) {
      case COMPLETED:
        for (int subPartitionId : PartitionUtils.getSubPartitions(partitionId, amplificationFactor)) {
          mainIngestionMonitorService.removeVersionPartitionFromIngestionMap(topicName, subPartitionId);
        }
        // Set LeaderState passed from child process to cache.
        LeaderFollowerStateType leaderFollowerStateType = LeaderFollowerStateType.valueOf(report.leaderFollowerState);
        notifierHelper(notifier -> notifier.completed(topicName, partitionId, report.offset, "", Optional.of(leaderFollowerStateType)));
        break;
      case ERROR:
        for (int subPartitionId : PartitionUtils.getSubPartitions(partitionId, amplificationFactor)) {
          mainIngestionMonitorService.removeVersionPartitionFromIngestionMap(topicName, subPartitionId);
        }
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
    mainIngestionMonitorService.getPushStatusNotifierList().forEach(lambda);
    mainIngestionMonitorService.getIngestionNotifierList().forEach(lambda);
  }

  private void updateLocalStorageMetadata(IngestionTaskReport report, int amplificationFactor) {
    String topicName = report.topicName.toString();
    int partitionId = report.partitionId;
    // Sync up offset record & store version state before report ingestion complete to parent process.
    if (mainIngestionMonitorService.getStorageMetadataService() != null) {
      if (!report.offsetRecordArray.isEmpty()) {
        int idx = 0;
        for (int subPartitionId : PartitionUtils.getSubPartitions(partitionId, amplificationFactor)) {
          OffsetRecord offsetRecord = new OffsetRecord(report.offsetRecordArray.get(idx).array(), partitionStateSerializer);
          mainIngestionMonitorService.getStorageMetadataService().putOffsetRecord(topicName, subPartitionId, offsetRecord);
          logger.info("Updated offsetRecord for subPartition: "  + subPartitionId + " of topic: " + topicName + " as " + offsetRecord.toString());
          idx++;
        }
      }
      if (report.storeVersionState != null) {
        StoreVersionState storeVersionState = IsolatedIngestionUtils.deserializeStoreVersionState(topicName, report.storeVersionState.array());
        mainIngestionMonitorService.getStorageMetadataService().putStoreVersionState(topicName, storeVersionState);
        logger.info("Updated storeVersionState for topic: " + topicName + " " + storeVersionState.toString());
      }
    }
  }
}

