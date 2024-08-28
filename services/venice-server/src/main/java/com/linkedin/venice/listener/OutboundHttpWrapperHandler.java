package com.linkedin.venice.listener;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.davinci.listener.response.TopicPartitionIngestionContextResponse;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.listener.response.BinaryResponse;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.ObjectMapperFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.charset.StandardCharsets;


/***
 * wraps raw bytes into an HTTP response object that HttpServerCodec expects
 */

public class OutboundHttpWrapperHandler extends ChannelOutboundHandlerAdapter {
  private final StatsHandler statsHandler;
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  public OutboundHttpWrapperHandler(StatsHandler handler) {
    super();
    statsHandler = handler;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    ByteBuf body;
    String contentType = HttpConstants.AVRO_BINARY;
    HttpResponseStatus responseStatus = OK;
    int schemaIdHeader = -1;
    int responseRcu = 1;
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
    boolean isStreamingResponse = false;
    try {
      if (msg instanceof ReadResponse) {
        ReadResponse obj = (ReadResponse) msg;
        ServerStatsContext statsContext = statsHandler.getServerStatsContext();
        setStats(statsContext, obj);

        compressionStrategy = obj.getCompressionStrategy();
        if (obj.isFound()) {
          body = obj.getResponseBody();
          schemaIdHeader = obj.getResponseSchemaIdHeader();
          statsContext.setResponseSize(body.readableBytes());
        } else {
          body = Unpooled.EMPTY_BUFFER;
          responseStatus = NOT_FOUND;
          statsContext.setResponseSize(0);
        }
        isStreamingResponse = obj.isStreamingResponse();
        responseRcu = obj.getRCU();
      } else if (msg instanceof HttpShortcutResponse) {
        // For Early terminated requests
        HttpShortcutResponse shortcutResponse = (HttpShortcutResponse) msg;
        responseStatus = shortcutResponse.getStatus();
        String message = shortcutResponse.getMessage();
        if (message == null) {
          message = "";
        }
        body = Unpooled.wrappedBuffer(message.getBytes(StandardCharsets.UTF_8));
        contentType = HttpConstants.TEXT_PLAIN;
        if (shortcutResponse.getStatus().equals(VeniceRequestEarlyTerminationException.getHttpResponseStatus())) {
          statsHandler.setRequestTerminatedEarly();
        }
        statsHandler.setMisroutedStoreVersionRequest(shortcutResponse.isMisroutedStoreVersion());
      } else if (msg instanceof BinaryResponse) {
        // For dictionary Fetch requests
        body = ((BinaryResponse) msg).getBody();
        contentType = HttpConstants.BINARY;
        responseStatus = ((BinaryResponse) msg).getStatus();
      } else if (msg instanceof AdminResponse) {
        AdminResponse adminResponse = (AdminResponse) msg;
        if (!adminResponse.isError()) {
          body = adminResponse.getResponseBody();
          schemaIdHeader = adminResponse.getResponseSchemaIdHeader();
        } else {
          /**
           * If error happens, return error message if any as well as 500 error code
           */
          String errorMessage = adminResponse.getMessage();
          if (errorMessage == null) {
            errorMessage = "Unknown error";
          }
          body = Unpooled.wrappedBuffer(errorMessage.getBytes(StandardCharsets.UTF_8));
          contentType = HttpConstants.TEXT_PLAIN;
          responseStatus = INTERNAL_SERVER_ERROR;
        }
      } else if (msg instanceof MetadataResponse) {
        MetadataResponse metadataResponse = (MetadataResponse) msg;
        if (!metadataResponse.isError()) {
          body = metadataResponse.getResponseBody();
          schemaIdHeader = metadataResponse.getResponseSchemaIdHeader();
        } else {
          String errorMessage = metadataResponse.getMessage();
          if (errorMessage == null) {
            errorMessage = "Unknown error";
          }
          body = Unpooled.wrappedBuffer(errorMessage.getBytes(StandardCharsets.UTF_8));
          contentType = HttpConstants.TEXT_PLAIN;
          responseStatus = INTERNAL_SERVER_ERROR;
        }
      } else if (msg instanceof ServerCurrentVersionResponse) {
        ServerCurrentVersionResponse currentVersionResponse = (ServerCurrentVersionResponse) msg;
        if (!currentVersionResponse.isError()) {
          body = Unpooled.wrappedBuffer(OBJECT_MAPPER.writeValueAsBytes(currentVersionResponse));
        } else {
          String errorMessage = currentVersionResponse.getMessage();
          if (errorMessage == null) {
            errorMessage = "Unknown error";
          }
          body = Unpooled.wrappedBuffer(errorMessage.getBytes(StandardCharsets.UTF_8));
          contentType = HttpConstants.TEXT_PLAIN;
          responseStatus = INTERNAL_SERVER_ERROR;
        }
      } else if (msg instanceof DefaultFullHttpResponse) {
        ctx.writeAndFlush(msg);
        return;
      } else if (msg instanceof TopicPartitionIngestionContextResponse) {
        TopicPartitionIngestionContextResponse topicPartitionIngestionContextResponse =
            (TopicPartitionIngestionContextResponse) msg;
        if (!topicPartitionIngestionContextResponse.isError()) {
          body = Unpooled.wrappedBuffer(OBJECT_MAPPER.writeValueAsBytes(topicPartitionIngestionContextResponse));
        } else {
          String errorMessage = topicPartitionIngestionContextResponse.getMessage();
          if (errorMessage == null) {
            errorMessage = "Unknown error";
          }
          body = Unpooled.wrappedBuffer(errorMessage.getBytes(StandardCharsets.UTF_8));
          contentType = HttpConstants.TEXT_PLAIN;
          responseStatus = INTERNAL_SERVER_ERROR;
        }
      } else {
        responseStatus = INTERNAL_SERVER_ERROR;
        body = Unpooled.wrappedBuffer(
            "Internal Server Error: Unrecognized object in OutboundHttpWrapperHandler"
                .getBytes(StandardCharsets.UTF_8));
        contentType = HttpConstants.TEXT_PLAIN;
      }
    } catch (Exception e) {
      responseStatus = INTERNAL_SERVER_ERROR;
      body = Unpooled.wrappedBuffer(
          ("Internal Server Error:\n\n" + ExceptionUtils.stackTraceToString(e) + "\n(End of server-side stacktrace)\n")
              .getBytes(StandardCharsets.UTF_8));
      contentType = HttpConstants.TEXT_PLAIN;
    } finally {
      statsHandler.setResponseStatus(responseStatus);
    }

    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, responseStatus, body);
    response.headers().set(CONTENT_TYPE, contentType);
    response.headers().set(CONTENT_LENGTH, body.readableBytes());
    response.headers().set(HttpConstants.VENICE_COMPRESSION_STRATEGY, compressionStrategy.getValue());
    response.headers().set(HttpConstants.VENICE_SCHEMA_ID, schemaIdHeader);
    response.headers().set(HttpConstants.VENICE_REQUEST_RCU, responseRcu);
    if (isStreamingResponse) {
      response.headers().set(HttpConstants.VENICE_STREAMING_RESPONSE, "1");
    }

    /** {@link io.netty.handler.timeout.IdleStateHandler} is in charge of detecting the state
     *  of connection, and {@link RouterRequestHttpHandler} will close the connection if necessary.
     *
     *  writeAndFlush may have some performance issue since it will call the actual send every time.
     */
    ctx.writeAndFlush(response);
  }

  public void setStats(ServerStatsContext statsContext, ReadResponse obj) {
    statsContext.setDatabaseLookupLatency(obj.getDatabaseLookupLatency());
    statsContext.setStorageExecutionHandlerSubmissionWaitTime(obj.getStorageExecutionHandlerSubmissionWaitTime());
    statsContext.setStorageExecutionQueueLen(obj.getStorageExecutionQueueLen());
    statsContext.setSuccessRequestKeyCount(obj.getRecordCount());
    statsContext.setMultiChunkLargeValueCount(obj.getMultiChunkLargeValueCount());
    statsContext.setReadComputeLatency(obj.getReadComputeLatency());
    statsContext.setReadComputeDeserializationLatency(obj.getReadComputeDeserializationLatency());
    statsContext.setReadComputeSerializationLatency(obj.getReadComputeSerializationLatency());
    statsContext.setDotProductCount(obj.getDotProductCount());
    statsContext.setCosineSimilarityCount(obj.getCosineSimilarityCount());
    statsContext.setHadamardProductCount(obj.getHadamardProductCount());
    statsContext.setCountOperatorCount(obj.getCountOperatorCount());
    statsContext.setKeySizeList(obj.getKeySizeList());
    statsContext.setValueSizeList(obj.getValueSizeList());
    statsContext.setValueSize(obj.getValueSize());
    statsContext.setReadComputeOutputSize(obj.getReadComputeOutputSize());
  }
}
