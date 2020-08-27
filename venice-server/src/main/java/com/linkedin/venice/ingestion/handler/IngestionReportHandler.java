package com.linkedin.venice.ingestion.handler;

import com.linkedin.venice.ingestion.IngestionReportListener;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ingestion.IngestionUtils.*;


public class IngestionReportHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger logger = Logger.getLogger(IngestionReportHandler.class);
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
    IngestionTaskReport report = deserializeIngestionTaskReport(readHttpRequestContent(msg));
    logger.info("Received ingestion task report " + report + " from ingestion service.");

    // Notify the parent ingestion service's listener.
    if (ingestionReportListener.getIngestionListener() != null) {
      if (report.isComplete) {
        ingestionReportListener.getIngestionListener().completed(report.topicName.toString(), report.partitionId, report.offset);
      }
    }
    ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.OK, "OK!"));
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    logger.error("Encounter exception " + cause.getMessage());
    ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, cause.getMessage()));
    ctx.close();
  }

}

