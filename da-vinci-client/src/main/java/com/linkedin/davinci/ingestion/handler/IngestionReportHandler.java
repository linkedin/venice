package com.linkedin.davinci.ingestion.handler;

import com.linkedin.davinci.ingestion.IngestionReportListener;
import com.linkedin.davinci.ingestion.IngestionUtils;
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

import static com.linkedin.davinci.ingestion.IngestionUtils.*;


public class IngestionReportHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger logger = Logger.getLogger(IngestionReportHandler.class);
  private static final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer = AvroProtocolDefinition.PARTITION_STATE.getSerializer();
  private final IngestionReportListener ingestionReportListener;

  public IngestionReportHandler(IngestionReportListener ingestionReportListener) {
    logger.info("IngestionReportHandler created.");
    this.ingestionReportListener = ingestionReportListener;
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) {
    // Decode ingestion report from incoming http request content.
    IngestionTaskReport report = deserializeIngestionActionRequest(IngestionAction.REPORT, readHttpRequestContent(msg));
    logger.info("Received ingestion task report " + report + " from ingestion service.");
    String topicName = report.topicName.toString();
    int partitionId = report.partitionId;
    long offset = report.offset;

    // Sync up offset record & store version state before report ingestion complete to parent process.
    if (ingestionReportListener.getStorageMetadataService() != null) {
      if (report.offsetRecord != null) {
        OffsetRecord offsetRecord = new OffsetRecord(report.offsetRecord.array(), partitionStateSerializer);
        ingestionReportListener.getStorageMetadataService().putOffsetRecord(topicName, partitionId, offsetRecord);
        logger.info("Updated offsetRecord for (topic, partition): " + topicName + " " + partitionId + " " + offsetRecord.toString());
      }
      if (report.storeVersionState != null) {
        StoreVersionState storeVersionState = IngestionUtils.deserializeStoreVersionState(topicName, report.storeVersionState.array());
        ingestionReportListener.getStorageMetadataService().putStoreVersionState(topicName, storeVersionState);
        logger.info("Updated storeVersionState for topic: " + topicName + " " + storeVersionState.toString());
      }
    }

    // Relay the notification to parent service's listener.
    switch (IngestionReportType.valueOf(report.reportType)) {
      case COMPLETED:
        // TODO: Set leader state in local KafkaStoreIngestionService during server integration.
        ingestionReportListener.removeVersionPartitionFromIngestionMap(topicName, partitionId);
        notifierHelper(notifier -> notifier.completed(topicName, partitionId, report.offset));
        break;
      case ERROR:
        ingestionReportListener.removeVersionPartitionFromIngestionMap(topicName, partitionId);
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
    ingestionReportListener.getPushStatusNotifierList().forEach(lambda);
    ingestionReportListener.getIngestionNotifierList().forEach(lambda);
  }
}

