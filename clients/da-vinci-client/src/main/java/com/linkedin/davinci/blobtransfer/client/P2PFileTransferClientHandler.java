package com.linkedin.davinci.blobtransfer.client;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.davinci.blobtransfer.BlobTransferPartitionMetadata;
import com.linkedin.davinci.blobtransfer.BlobTransferPayload;
import com.linkedin.venice.exceptions.VeniceException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The client-side Netty handler to process responses for P2P file transfer. It's not shareable among multiple requests since it
 * maintains the states for a single partition.
 * It's important to note that this handler is operated in a single thread, and it processes file transfers sequentially.
 */
public class P2PFileTransferClientHandler extends SimpleChannelInboundHandler<HttpObject> {
  private static final Logger LOGGER = LogManager.getLogger(P2PFileTransferClientHandler.class);
  private static final Pattern FILENAME_PATTERN = Pattern.compile("filename=\"(.+?)\"");
  private final CompletionStage<BlobTransferPartitionMetadata> metadataFuture;
  private final BlobTransferPayload payload;

  // mutable states for a single file transfer. It will be updated for each file transfer.
  private FileChannel outputFileChannel;
  private String fileName;
  private long fileContentLength;
  private BlobTransferPartitionMetadata metadata;
  private ByteBuf accumulatedMetadataContent = Unpooled.buffer();
  private String currentTransferType;

  public P2PFileTransferClientHandler(
      String baseDir,
      CompletionStage<BlobTransferPartitionMetadata> metadataFuture,
      String storeName,
      int version,
      int partition) {
    this.metadataFuture = metadataFuture;
    this.payload = new BlobTransferPayload(baseDir, storeName, version, partition);
  }

  /**
   * Handles the response from the server.
   * @param ctx the channel context
   * @param msg the message to handle
   * @throws Exception
   */
  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
    if (msg instanceof HttpResponse) {
      HttpResponse response = (HttpResponse) msg;
      if (!response.status().equals(HttpResponseStatus.OK)) {
        throw new VeniceException("Failed to fetch file from remote peer. Response: " + response.status());
      }
      // Already end of transfer. Close the connection and completes the future
      if (response.headers().get(BLOB_TRANSFER_STATUS) != null
          && response.headers().get(BLOB_TRANSFER_STATUS).equals(BLOB_TRANSFER_COMPLETED)) {
        LOGGER.info("Received blob transfer completed response.");
        resetState();
        handleEndOfTransfer(ctx);
        return;
      }
      currentTransferType = response.headers().get(BLOB_TRANSFER_TYPE);
    }

    if (msg instanceof HttpResponse || msg instanceof HttpContent) {
      if (currentTransferType != null && currentTransferType.equals(BlobTransferType.METADATA.name())) {
        LOGGER.info("Start to handle metadata transfer");
        metadata = handlerMetadataTransferResponse(ctx, msg);
      } else {
        LOGGER.info("Start to handle file transfer");
        handleFileTransferResponse(ctx, msg);
      }
    }
  }

  /**
   * Handles the file transfer response.
   * @param ctx the channel context
   * @param msg the http object
   * @throws Exception
   */
  private void handleFileTransferResponse(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
    if (msg instanceof HttpResponse) {
      HttpResponse response = (HttpResponse) msg;
      if (!response.status().equals(HttpResponseStatus.OK)) {
        throw new VeniceException("Failed to fetch file from remote peer. Response: " + response.status());
      }

      // Parse the file name
      this.fileName = getFileNameFromHeader(response);

      if (this.fileName == null) {
        throw new VeniceException("No file name specified in the response for " + payload.getFullResourceName());
      }
      LOGGER.info("Starting blob transfer for file: {}", fileName);
      this.fileContentLength = Long.parseLong(response.headers().get(HttpHeaderNames.CONTENT_LENGTH));

      // Create the directory
      Path partitionDir = Paths.get(payload.getPartitionDir());
      Files.createDirectories(partitionDir);

      // Prepare the file
      Path file = Files.createFile(partitionDir.resolve(fileName));
      outputFileChannel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND);

    } else if (msg instanceof HttpContent) {
      HttpContent content = (HttpContent) msg;
      ByteBuf byteBuf = content.content();
      if (byteBuf.readableBytes() == 0) {
        // hit EMPTY_LAST_CONTENT, it indicates the end of all file transfer.
        // Skip it since it's not going to be used
        LOGGER.info("Empty content received for {}", payload.getFullResourceName());
        return;
      }
      // defensive check
      if (outputFileChannel == null) {
        throw new VeniceException("No file opened to write for " + payload.getFullResourceName());
      }

      // Append content to the given file
      // TODO: need to do perf test to see if this NIO implementation is really faster than regular I/O libs
      long count = 0L;
      long position = outputFileChannel.size();
      long totalBytesToTransfer = byteBuf.readableBytes();
      try (ByteBufInputStream byteBufInputStream = new ByteBufInputStream(byteBuf)) {
        ReadableByteChannel inputChannel = Channels.newChannel(byteBufInputStream);
        while (count < totalBytesToTransfer) {
          long bytesToTransfer = totalBytesToTransfer - count;
          long transferred = outputFileChannel.transferFrom(inputChannel, position, bytesToTransfer);
          if (transferred == 0) {
            break;
          }
          position += transferred;
          count += transferred;
        }
      }

      if (content instanceof DefaultLastHttpContent) {
        // End of a single file transfer
        LOGGER.debug("A file {} received successfully for {}", fileName, payload.getFullResourceName());
        outputFileChannel.force(true);

        // Size validation
        if (outputFileChannel.size() != fileContentLength) {
          throw new VeniceException(
              "File size mismatch for " + fileName + ". Expected: " + fileContentLength + ", Actual: "
                  + outputFileChannel.size());
        }
      }
    } else {
      throw new VeniceException("Unexpected message received: " + msg.getClass().getName());
    }
  }

  /**
   * Handles the metadata transfer response.
   * @param ctx the channel context
   * @param msg the http object
   * @throws JsonProcessingException
   */
  private BlobTransferPartitionMetadata handlerMetadataTransferResponse(ChannelHandlerContext ctx, HttpObject msg)
      throws JsonProcessingException {
    if (msg instanceof HttpContent) {
      HttpContent httpContent = (HttpContent) msg;
      ByteBuf content = httpContent.content();
      if (content.readableBytes() == 0) {
        LOGGER.info("Empty content received for metadata transfer");
        return null;
      }
      if (content.isReadable()) {
        accumulatedMetadataContent.writeBytes(content);
      }
      if (msg instanceof DefaultLastHttpContent) {
        if (accumulatedMetadataContent.isReadable()) {
          byte[] contentBytes = new byte[accumulatedMetadataContent.readableBytes()];
          accumulatedMetadataContent.readBytes(contentBytes);
          ObjectMapper objectMapper = new ObjectMapper();
          BlobTransferPartitionMetadata metadata = objectMapper
              .readValue(new String(contentBytes, StandardCharsets.UTF_8), BlobTransferPartitionMetadata.class);
          LOGGER.info("Metadata transfer completed, topic {} metadata is {}", payload.getTopicName(), metadata);
          return metadata;
        }
      }
    }
    return null;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
    if (outputFileChannel != null) {
      outputFileChannel.force(true);
      outputFileChannel.close();
    }
    resetState();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOGGER
        .error("Exception caught in when transferring files for {}, cause is {}", payload.getFullResourceName(), cause);
    metadataFuture.toCompletableFuture().completeExceptionally(cause);
    ctx.close();
  }

  private String getFileNameFromHeader(HttpResponse response) {
    String contentDisposition = response.headers().get(HttpHeaderNames.CONTENT_DISPOSITION);
    if (contentDisposition != null) {
      Matcher matcher = FILENAME_PATTERN.matcher(contentDisposition);
      if (matcher.find()) {
        return matcher.group(1);
      }
    }
    return null;
  }

  private void handleEndOfTransfer(ChannelHandlerContext ctx) {
    LOGGER.info("All files received successfully for {}", payload.getFullResourceName());
    metadataFuture.toCompletableFuture().complete(metadata);
    ctx.close();
  }

  private void resetState() {
    outputFileChannel = null;
    fileName = null;
    fileContentLength = 0;
    currentTransferType = null;
    accumulatedMetadataContent.clear();
  }
}
