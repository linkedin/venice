package com.linkedin.venice.utils;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.buildHttpResponse;
import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.createIngestionTaskReport;
import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.deserializeIngestionActionRequest;
import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.getDummyContent;
import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.readHttpRequestContent;
import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.serializeIngestionActionResponse;

import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionTaskCommand;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.enums.IngestionAction;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SimpleServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger LOGGER = LogManager.getLogger(SimpleServerHandler.class);
  private static final AtomicInteger counter = new AtomicInteger();
  private final int id = counter.getAndIncrement();
  private final SimpleServer simpleServer;
  static final EventExecutorGroup group = new DefaultEventExecutorGroup(16);

  public SimpleServerHandler(SimpleServer simpleServer) {
    super();
    this.simpleServer = simpleServer;
    LOGGER.info(
        "{} id: {}, created for listener service at {}.",
        SimpleServerHandler.class.getSimpleName(),
        id,
        (System.currentTimeMillis() % 1000000) / 1000.0d);

  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
    simpleServer.getExecutorService().execute(() -> {
      try {
        IngestionAction action = IsolatedIngestionUtils.getIngestionActionFromRequest(msg);
        byte[] result = getDummyContent();
        if (!simpleServer.isInitiated()) {
          throw new VeniceException("Server is not initialized yet!");
        }
        switch (action) {
          case COMMAND:
            IngestionTaskCommand ingestionTaskCommand =
                deserializeIngestionActionRequest(action, readHttpRequestContent(msg));
            IngestionTaskReport report = handleIngestionTaskCommand(ingestionTaskCommand);
            result = serializeIngestionActionResponse(action, report);
            break;
          case HEARTBEAT:
            LOGGER.info("Received heartbeat.");
            break;
          default:
            throw new UnsupportedOperationException("Unrecognized ingestion action: " + action);
        }
        ctx.writeAndFlush(buildHttpResponse(HttpResponseStatus.OK, result));
      } catch (UnsupportedOperationException e) {
        // Here we only handles the bad requests exception. Other errors are handled in exceptionCaught() method.
        LOGGER.error("Caught unrecognized request action:", e);
        ctx.writeAndFlush(
            buildHttpResponse(
                HttpResponseStatus.BAD_REQUEST,
                ExceptionUtils.compactExceptionDescription(e, "channelRead0")));
      }
    });
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOGGER.error("Encounter exception -  message: {}, cause: {}", cause.getMessage(), cause);
    ctx.writeAndFlush(
        buildHttpResponse(
            HttpResponseStatus.INTERNAL_SERVER_ERROR,
            ExceptionUtils.compactExceptionDescription(cause, "exceptionCaught")));
    ctx.close();
  }

  private IngestionTaskReport handleIngestionTaskCommand(IngestionTaskCommand ingestionTaskCommand) {
    String topicName = ingestionTaskCommand.topicName.toString();
    int partitionId = ingestionTaskCommand.partitionId;
    IngestionTaskReport report = createIngestionTaskReport(topicName, partitionId);
    LOGGER.info("Handling consumption request for topic: {}, partition: {} {}", topicName, partitionId, logIndex());
    if (topicName.contains("zombie_topic")) {
      LOGGER.warn("Start sleeping for topic: {}, partition: {} {}", topicName, partitionId, logIndex());
      Utils.sleep(10000);
      LOGGER.warn("Finished sleeping for topic: {}, partition: {} {}", topicName, partitionId, logIndex());
      report.isPositive = false;
      report.exceptionThrown = true;
      report.message = "Resource does not exist";
      // throw new VeniceException("Topic: " + topicName + ", version: " + partitionId + " does not exist.");
    }
    return report;
  }

  private String logIndex() {
    return " [index=" + id + "]";
  }
}
