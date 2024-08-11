package com.linkedin.venice.listener;

import static com.linkedin.venice.HttpConstants.CONTENT_LENGTH_HEADER;
import static com.linkedin.venice.HttpConstants.CONTENT_TYPE_HEADER;
import static com.linkedin.venice.HttpConstants.VENICE_COMPRESSION_STRATEGY_HEADER;
import static com.linkedin.venice.HttpConstants.VENICE_REQUEST_RCU_HEADER;
import static com.linkedin.venice.HttpConstants.VENICE_SCHEMA_ID_HEADER;
import static com.linkedin.venice.HttpConstants.VENICE_STREAMING_RESPONSE_HEADER;
import static com.linkedin.venice.HttpConstants.VENICE_STREAMING_RESPONSE_HEADER_VALUE;
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
        } else {
          body = Unpooled.EMPTY_BUFFER;
          responseStatus = NOT_FOUND;
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
    response.headers().set(CONTENT_TYPE_HEADER, contentType);
    response.headers().set(CONTENT_LENGTH_HEADER, body.readableBytes());
    response.headers().set(VENICE_COMPRESSION_STRATEGY_HEADER, compressionStrategy.getValue());
    response.headers().set(VENICE_SCHEMA_ID_HEADER, schemaIdHeader);
    response.headers().set(VENICE_REQUEST_RCU_HEADER, responseRcu);
    if (isStreamingResponse) {
      response.headers().set(VENICE_STREAMING_RESPONSE_HEADER, VENICE_STREAMING_RESPONSE_HEADER_VALUE);
    }

    /** {@link io.netty.handler.timeout.IdleStateHandler} is in charge of detecting the state
     *  of connection, and {@link RouterRequestHttpHandler} will close the connection if necessary.
     *
     *  writeAndFlush may have some performance issue since it will call the actual send every time.
     */
    ctx.writeAndFlush(response);
  }

  public void setStats(ServerStatsContext statsContext, ReadResponse readResponse) {
    statsContext.setDatabaseLookupLatency(readResponse.getDatabaseLookupLatency());
    statsContext.setStorageExecutionQueueLen(readResponse.getStorageExecutionQueueLen());
    statsContext.setSuccessRequestKeyCount(readResponse.getRecordCount());
    statsContext.setMultiChunkLargeValueCount(readResponse.getMultiChunkLargeValueCount());
    statsContext.setReadComputeLatency(readResponse.getReadComputeLatency());
    statsContext.setReadComputeDeserializationLatency(readResponse.getReadComputeDeserializationLatency());
    statsContext.setReadComputeSerializationLatency(readResponse.getReadComputeSerializationLatency());
    statsContext.setDotProductCount(readResponse.getDotProductCount());
    statsContext.setCosineSimilarityCount(readResponse.getCosineSimilarityCount());
    statsContext.setHadamardProductCount(readResponse.getHadamardProductCount());
    statsContext.setCountOperatorCount(readResponse.getCountOperatorCount());
    statsContext.setKeySizeList(readResponse.getKeySizeList());
    statsContext.setValueSizeList(readResponse.getValueSizeList());
    statsContext.setValueSize(readResponse.getValueSize());
    statsContext.setReadComputeOutputSize(readResponse.getReadComputeOutputSize());
  }
}
