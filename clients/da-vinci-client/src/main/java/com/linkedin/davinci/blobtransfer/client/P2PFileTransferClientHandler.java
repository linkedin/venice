package com.linkedin.davinci.blobtransfer.client;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_COMPLETED;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_STATUS;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_TYPE;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.davinci.blobtransfer.BlobTransferPartitionMetadata;
import com.linkedin.davinci.blobtransfer.BlobTransferPayload;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
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
import java.io.InputStream;
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
  private static final InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer =
      AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
  private static final Pattern FILENAME_PATTERN = Pattern.compile("filename=\"(.+?)\"");
  private final CompletionStage<InputStream> inputStreamFuture;
  private final BlobTransferPayload payload;

  // mutable states for a single file transfer. It will be updated for each file transfer.
  private FileChannel outputFileChannel;
  private String fileName;
  private long fileContentLength;
  private BlobTransferPartitionMetadata metadata;
  private StorageService storageService;
  private KafkaStoreIngestionService kafkaStoreIngestionService;
  private ByteBuf accumulatedMetadataContent = Unpooled.buffer();
  private String currentTransferType;

  public P2PFileTransferClientHandler(
      KafkaStoreIngestionService kafkaStoreIngestionService,
      StorageService storageService,
      String baseDir,
      CompletionStage<InputStream> inputStreamFuture,
      String storeName,
      int version,
      int partition) {
    this.kafkaStoreIngestionService = kafkaStoreIngestionService;
    this.storageService = storageService;
    this.inputStreamFuture = inputStreamFuture;
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

      // check the response status
      if (!response.status().equals(HttpResponseStatus.OK)) {
        throw new VeniceException("Failed to fetch file from remote peer. Response: " + response.status());
      }

      // check the blob transfer status, if already completed, then we can end the transfer.
      if (response.headers().get(BLOB_TRANSFER_STATUS) != null
          && response.headers().get(BLOB_TRANSFER_STATUS).equals(BLOB_TRANSFER_COMPLETED)) {
        LOGGER.debug("Received blob transfer completed response.");
        updateStorePartitionMetadata(kafkaStoreIngestionService, storageService, metadata);
        handleEndOfTransfer(ctx);
        return;
      }

      // The response is not the completed flag, so it's either metadata or file response.
      if (response.headers().get(BLOB_TRANSFER_TYPE) == null) {
        throw new VeniceException("No transfer type specified in the response " + msg);
      }
      currentTransferType = response.headers().get(BLOB_TRANSFER_TYPE);
      if (currentTransferType.equals(BlobTransferType.FILE.toString())) {
        LOGGER.debug("Start to handle file response transfer");
        handleFileTransferResponse((HttpResponse) msg);
      }
    } else if (msg instanceof HttpContent) {
      if (currentTransferType != null && currentTransferType.equals(BlobTransferType.METADATA.toString())) {
        LOGGER.debug("Start to handle metadata transfer");
        metadata = handleMetadataTransferContent((HttpContent) msg);
      } else {
        LOGGER.debug("Start to handle file content transfer");
        handleFileTransferContent((HttpContent) msg);
      }
    } else {
      throw new VeniceException("Unexpected message received: " + msg.getClass().getName());
    }
  }

  /**
   * Handles the file transfer response, prepares the file to write.
   * @param msg the http object
   * @throws Exception
   */
  private void handleFileTransferResponse(HttpResponse msg) throws Exception {
    HttpResponse response = msg;
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
  }

  /**
   * Handles the file transfer content, writing the content to the file.
   * @param msg the http content
   * @throws Exception
   */
  private void handleFileTransferContent(HttpContent msg) throws Exception {
    HttpContent content = msg;
    ByteBuf byteBuf = content.content();
    if (byteBuf.readableBytes() == 0) {
      LOGGER.debug("Empty content received for {}", payload.getFullResourceName());
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
      resetState();
    }
  }

  /**
   * Handles the metadata transfer content.
   * @param msg the http content
   * @throws JsonProcessingException
   */
  private BlobTransferPartitionMetadata handleMetadataTransferContent(HttpContent msg) throws JsonProcessingException {
    HttpContent httpContent = msg;
    ByteBuf content = httpContent.content();
    if (content.readableBytes() == 0) {
      LOGGER.info("Empty content received for metadata transfer");
      return metadata;
    }
    if (content.isReadable()) {
      accumulatedMetadataContent.writeBytes(content);
    }
    if (msg instanceof DefaultLastHttpContent) {
      if (accumulatedMetadataContent.isReadable()) {
        byte[] contentBytes = new byte[accumulatedMetadataContent.readableBytes()];
        accumulatedMetadataContent.readBytes(contentBytes);
        ObjectMapper objectMapper = new ObjectMapper();
        BlobTransferPartitionMetadata transferredMetadata = objectMapper
            .readValue(new String(contentBytes, StandardCharsets.UTF_8), BlobTransferPartitionMetadata.class);
        LOGGER.info(
            "Metadata transfer completed, topic {} transferredMetadata is {}",
            payload.getTopicName(),
            transferredMetadata);
        // release the buffer
        accumulatedMetadataContent.release();
        return transferredMetadata;
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
    inputStreamFuture.toCompletableFuture().completeExceptionally(cause);
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
    // In the short term, we decided to let the netty client handle writing files to the disk, so
    // the future completes with a null. It's subject to change.
    inputStreamFuture.toCompletableFuture().complete(null);
    ctx.close();
  }

  private void resetState() {
    outputFileChannel = null;
    fileName = null;
    fileContentLength = 0;
    currentTransferType = null;
  }

  /**
   * Given the metadata from blob transfer, update the store partition metadata in storage service.
   */
  public void updateStorePartitionMetadata(
      KafkaStoreIngestionService kafkaStoreIngestionService,
      StorageService storageService,
      BlobTransferPartitionMetadata metadata) {
    if (metadata == null) {
      LOGGER.error("No metadata received for topic {}. skip updating.", payload.getTopicName());
      throw new VeniceException("No metadata received to update for topic " + payload.getTopicName());
    }

    LOGGER.info("Start updating store partition metadata for topic {}. ", metadata.topicName);
    // update the offset record in storage service
    kafkaStoreIngestionService
        .updatePartitionOffsetRecords(metadata.topicName, metadata.partitionId, metadata.offsetRecord);
    // update the metadata SVS
    StoreVersionState storeVersionState =
        storeVersionStateSerializer.deserialize(metadata.topicName, metadata.storeVersionState.array());
    storageService.getStoreVersionStateSyncer().accept(metadata.topicName, storeVersionState);
  }

  @VisibleForTesting
  public BlobTransferPartitionMetadata getMetadata() {
    return metadata;
  }
}
