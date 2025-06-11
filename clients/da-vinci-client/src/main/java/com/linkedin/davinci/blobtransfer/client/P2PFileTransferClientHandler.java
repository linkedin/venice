package com.linkedin.davinci.blobtransfer.client;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_COMPLETED;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_STATUS;

import com.linkedin.davinci.blobtransfer.BlobTransferPayload;
import com.linkedin.davinci.blobtransfer.BlobTransferUtils;
import com.linkedin.venice.exceptions.VeniceBlobTransferFileNotFoundException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
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
  private final CompletionStage<InputStream> inputStreamFuture;
  private final BlobTransferPayload payload;

  // mutable states for a single file transfer. It will be updated for each file transfer.
  private FileChannel outputFileChannel;
  private String fileName;
  private String fileChecksum;
  private Path file;
  private long fileContentLength;

  public P2PFileTransferClientHandler(
      String baseDir,
      CompletionStage<InputStream> inputStreamFuture,
      String storeName,
      int version,
      int partition,
      BlobTransferUtils.BlobTransferTableFormat tableFormat) {
    this.inputStreamFuture = inputStreamFuture;
    this.payload = new BlobTransferPayload(baseDir, storeName, version, partition, tableFormat);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
    if (msg instanceof HttpResponse) {
      HttpResponse response = (HttpResponse) msg;

      if (!response.status().equals(HttpResponseStatus.OK)) {
        if (response.status().equals(HttpResponseStatus.NOT_FOUND)) {
          throw new VeniceBlobTransferFileNotFoundException(
              "Requested files from remote peer are not found. Response: " + response.status());
        } else {
          throw new VeniceException("Failed to fetch file from remote peer. Response: " + response.status());
        }
      }

      // redirect the message to the next handler if it's a metadata transfer
      boolean isMetadataMessage = BlobTransferUtils.isMetadataMessage(response);
      if (isMetadataMessage) {
        ReferenceCountUtil.retain(msg);
        ctx.fireChannelRead(msg);
        return;
      }

      // Already end of transfer. Close the connection and completes the future
      if (response.headers().get(BLOB_TRANSFER_STATUS) != null
          && response.headers().get(BLOB_TRANSFER_STATUS).equals(BLOB_TRANSFER_COMPLETED)) {
        handleEndOfTransfer(ctx);
        return;
      }

      // Parse the file name and checksum from the response
      this.fileName = getFileNameFromHeader(response);
      this.fileChecksum = response.headers().get(HttpHeaderNames.CONTENT_MD5);

      if (this.fileName == null) {
        throw new VeniceException("No file name specified in the response for " + payload.getFullResourceName());
      }

      LOGGER.info(
          "Starting blob file receiving for file: {} for {}",
          fileName,
          Utils.getReplicaId(payload.getTopicName(), payload.getPartition()));
      this.fileContentLength = Long.parseLong(response.headers().get(HttpHeaderNames.CONTENT_LENGTH));

      // Create the directory
      Path partitionDir = Paths.get(payload.getPartitionDir());
      Files.createDirectories(partitionDir);

      // Prepare the file, remove it if it exists
      if (Files.deleteIfExists(partitionDir.resolve(fileName))) {
        LOGGER.warn(
            "File {} already exists for {}. Overwriting it.",
            fileName,
            Utils.getReplicaId(payload.getTopicName(), payload.getPartition()));
      }

      this.file = Files.createFile(partitionDir.resolve(fileName));

      outputFileChannel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND);

    } else if (msg instanceof HttpContent) {
      HttpContent content = (HttpContent) msg;
      ByteBuf byteBuf = content.content();
      if (byteBuf.readableBytes() == 0) {
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
        LOGGER.info(
            "A file {} received successfully for {}",
            fileName,
            Utils.getReplicaId(payload.getTopicName(), payload.getPartition()));
        outputFileChannel.force(true);

        // Size validation
        if (outputFileChannel.size() != fileContentLength) {
          throw new VeniceException(
              "File size mismatch for " + fileName + ". Expected: " + fileContentLength + ", Actual: "
                  + outputFileChannel.size());
        }

        // Checksum validation
        String receivedFileChecksum = BlobTransferUtils.generateFileChecksum(file);
        if (!receivedFileChecksum.equals(fileChecksum)) {
          throw new VeniceException(
              "File checksum mismatch for " + fileName + ". Expected: " + fileChecksum + ", Actual: "
                  + receivedFileChecksum);
        }

        resetState();
      }
    } else {
      throw new VeniceException("Unexpected message received: " + msg.getClass().getName());
    }
  }

  /**
   * Handles channel deactivation, typically triggered when the server gracefully closes
   * the connection.
   *
   * This is called when the server sends a FIN packet (graceful shutdown), which is
   * different from abrupt termination handled by {@link #userEventTriggered}.
   *
   * If the transfer was incomplete, this indicates the server shut down unexpectedly during
   * the transfer process, so we complete the input stream future exceptionally for fast failover.
   *
   * @param ctx
   * @throws Exception
   */
  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
    if (outputFileChannel != null) {
      outputFileChannel.force(true);
      outputFileChannel.close();
    }
    resetState();
    fastFailoverIncompleteTransfer(
        "Channel close before completing transfer, might due to server unexpected graceful shutdown.",
        ctx);
  }

  /**
   * Handles idle state events to detect unresponsive server connections during blob transfer.
   *
   * When no data is received within the timeout (READER_IDLE), this method assumes
   * the server is unavailable, completes the transfer future exceptionally, and closes the channel.
   * This enables fast failover to the next available peer instead of waiting for the longer
   * client timeout configured in NettyFileTransferClient#blobReceiveTimeoutInMin.
   *
   * Please note that if traffic is legitimately slow but continuous, it should NOT trigger READER_IDLE.
   * However, if traffic has long gaps, it WILL trigger.
   *
   * @param ctx the channel handler context
   * @param evt the user event, expected to be IdleStateEvent for timeout detection
   */
  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) evt;
      if (e.state() == IdleState.READER_IDLE) {
        fastFailoverIncompleteTransfer(
            "Channel idle before completing transfer, might due to server unexpected abrupt termination.",
            ctx);
        ctx.close();
        return;
      }
    }
    super.userEventTriggered(ctx, evt);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOGGER.error(
        "Exception caught in when receiving files for {} with cause {}",
        Utils.getReplicaId(payload.getTopicName(), payload.getPartition()),
        cause);
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
  }

  private void fastFailoverIncompleteTransfer(String causeForFailPendingTransfer, ChannelHandlerContext ctx) {
    if (!inputStreamFuture.toCompletableFuture().isDone()) {
      String errorMessage = String.format(
          "%s for %s. Server host: %s, channel active: %b.",
          causeForFailPendingTransfer,
          Utils.getReplicaId(payload.getTopicName(), payload.getPartition()),
          ctx.channel().remoteAddress(),
          ctx.channel().isActive());

      LOGGER.error(errorMessage);
      inputStreamFuture.toCompletableFuture().completeExceptionally(new VeniceException(errorMessage));
    }
  }
}
