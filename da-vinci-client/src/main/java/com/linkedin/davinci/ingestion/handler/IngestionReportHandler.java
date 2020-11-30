package com.linkedin.davinci.ingestion.handler;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.davinci.ingestion.IngestionReportListener;
import com.linkedin.davinci.ingestion.IngestionUtils;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.log4j.Logger;

import static com.linkedin.davinci.ingestion.IngestionUtils.*;


public class IngestionReportHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger logger = Logger.getLogger(IngestionReportHandler.class);
  private final IngestionReportListener ingestionReportListener;
  private final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;

  public IngestionReportHandler(IngestionReportListener ingestionReportListener,
                                InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer) {
    logger.info("IngestionReportHandler created.");
    this.ingestionReportListener = ingestionReportListener;
    this.partitionStateSerializer = partitionStateSerializer;
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) {
    // Decode ingestion report from incoming http request content.
    IngestionTaskReport report = deserializeIngestionTaskReport(readHttpRequestContent(msg));
    logger.info("Received ingestion task report " + report + " from ingestion service.");
    String topicName = report.topicName.toString();
    int partitionId = report.partitionId;

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
    if (ingestionReportListener.getIngestionNotifier() != null) {
      if (report.isComplete) {
        ingestionReportListener.getIngestionNotifier().completed(topicName, partitionId, report.offset);
        ingestionReportListener.removeVersionPartitionFromIngestionMap(topicName, partitionId);
      } else if (report.isError) {
        ingestionReportListener.getIngestionNotifier().error(topicName, partitionId, report.errorMessage.toString(), new VeniceException(report.errorMessage.toString()));
        ingestionReportListener.removeVersionPartitionFromIngestionMap(topicName, partitionId);
      } else {
        logger.info("Received unsupported ingestion report, it will be ignored for now.");
      }
    }


    ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.OK, "OK!"));
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    logger.error("Encounter exception during ingestion task report handling.", cause);
    ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, cause.getClass().getSimpleName() + "_" + cause.getMessage()));
    ctx.close();
  }
}

