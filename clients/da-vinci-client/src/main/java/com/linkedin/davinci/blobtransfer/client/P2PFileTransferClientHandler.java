package com.linkedin.davinci.blobtransfer.client;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_COMPLETED;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_STATUS;

import com.linkedin.davinci.blobtransfer.BlobTransferPayload;
import com.linkedin.davinci.blobtransfer.BlobTransferUtils;
import com.linkedin.venice.exceptions.VeniceBlobTransferFileNotFoundException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.LatencyUtils;
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
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
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
  private final AtomicReference<Throwable> checksumExceptionHolder = new AtomicReference<>(null);
  private final BlobTransferPayload payload;
  // mutable states for a single file transfer. It will be updated for each file transfer.
  private DigestOutputStream digestOutputStream;
  private OutputStream fileOutputStream;
  private String fileName;
  private String fileChecksum;
  private Path file;
  private long fileContentLength;
  private final String replicaId;
  private long fileTransferStartTime;
  private final long replicaTransferStartTime;
  private MessageDigest actualFileCheckSum;
  private long actualBytesWritten = 0;
  private long fileStreamingChecksumTimeInMs = 0;

  public P2PFileTransferClientHandler(
      String baseDir,
      CompletionStage<InputStream> inputStreamFuture,
      String storeName,
      int version,
      int partition,
      BlobTransferUtils.BlobTransferTableFormat tableFormat) {
    this.inputStreamFuture = inputStreamFuture;
    this.payload = new BlobTransferPayload(baseDir, storeName, version, partition, tableFormat);
    this.replicaId = Utils.getReplicaId(payload.getTopicName(), payload.getPartition());
    this.replicaTransferStartTime = System.currentTimeMillis();
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

      LOGGER.info("Starting blob file receiving for file: {} for {}", fileName, replicaId);
      this.fileTransferStartTime = System.currentTimeMillis();
      this.actualFileCheckSum = MessageDigest.getInstance("MD5");
      this.fileContentLength = Long.parseLong(response.headers().get(HttpHeaderNames.CONTENT_LENGTH));

      // Create a temp directory
      Path tempPartitionDir = Paths.get(payload.getTempPartitionDir());
      Files.createDirectories(tempPartitionDir);

      // Prepare the file, remove it if it exists
      if (Files.deleteIfExists(tempPartitionDir.resolve(fileName))) {
        LOGGER.warn(
            "File {} already exists for {}. Overwriting it.",
            fileName,
            Utils.getReplicaId(payload.getTopicName(), payload.getPartition()));
      }

      this.file = Files.createFile(tempPartitionDir.resolve(fileName));

      // Create output stream with digest wrapper for checksum computation during write
      fileOutputStream = Files.newOutputStream(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
      digestOutputStream = new DigestOutputStream(fileOutputStream, actualFileCheckSum);

    } else if (msg instanceof HttpContent) {
      HttpContent content = (HttpContent) msg;
      ByteBuf byteBuf = content.content();
      if (byteBuf.readableBytes() == 0) {
        return;
      }
      // defensive check
      if (digestOutputStream == null) {
        throw new VeniceException("No file opened to write for " + payload.getFullResourceName());
      }

      // Write content to file, checksum is computed automatically during write
      long startTime = System.currentTimeMillis();
      try (ByteBufInputStream byteBufInputStream = new ByteBufInputStream(byteBuf)) {
        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = byteBufInputStream.read(buffer)) != -1) {
          // This will write to disk AND update the checksum
          digestOutputStream.write(buffer, 0, bytesRead);
          actualBytesWritten += bytesRead;
        }
      }

      fileStreamingChecksumTimeInMs += LatencyUtils.getElapsedTimeFromMsToMs(startTime);

      if (content instanceof DefaultLastHttpContent) {
        // Flush all data to disk before getting checksum
        digestOutputStream.flush();

        // Force sync to ensure data is physically written to disk
        if (fileOutputStream instanceof java.io.FileOutputStream) {
          ((java.io.FileOutputStream) fileOutputStream).getFD().sync();
        }

        // Get the checksum, this reflects what's actually on disk
        byte[] digest = actualFileCheckSum.digest();
        StringBuilder hexString = new StringBuilder();
        for (byte b: digest) {
          hexString.append(String.format("%02x", b));
        }
        String md5Hex = hexString.toString();

        // End of a single file transfer
        LOGGER.info(
            "A file: {} received successfully for replica: {} took: {}ms; Streaming MD5 checksum result: {} computed in {} ms, expected checksum: {}",
            fileName,
            replicaId,
            LatencyUtils.getElapsedTimeFromMsToMs(fileTransferStartTime),
            md5Hex,
            fileStreamingChecksumTimeInMs,
            fileChecksum);

        // Size validation
        if (actualBytesWritten != fileContentLength) {
          throw new VeniceException(
              "File size mismatch for " + fileName + ". Expected: " + fileContentLength + ", Actual: "
                  + actualBytesWritten);
        }

        // Close the output streams
        try {
          digestOutputStream.close();
          LOGGER.info("Closed file output stream for: {} for replica: {}", fileName, replicaId);
        } catch (Exception e) {
          LOGGER.warn(
              "Failed to close file output stream for file {} for replica {} with error: {}",
              fileName,
              replicaId,
              e.getMessage());
        }

        // Perform checksum validation
        if (!md5Hex.equals(fileChecksum)) {
          String errorMessage = String.format(
              "File checksum mismatch for file: %s for replica: %s. Expected: %s, actual: %s",
              file,
              replicaId,
              fileChecksum,
              md5Hex);
          throw new VeniceException(errorMessage);
        }
        // Reset state for the next file transfer
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
    fastFailoverIncompleteTransfer(
        "Channel close before completing transfer, might due to server graceful shutdown.",
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
    handleExceptionGracefully(cause);
    ctx.close();
  }

  void handleExceptionGracefully(Throwable cause) {
    if (!inputStreamFuture.toCompletableFuture().isDone()) {
      LOGGER.error("Exception caught in when receiving files for replica: {}", replicaId, cause);
      cleanupResources();
      inputStreamFuture.toCompletableFuture().completeExceptionally(cause);
    }
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
    LOGGER.info(
        "All files received successfully for replica: {} took: {}ms",
        replicaId,
        LatencyUtils.getElapsedTimeFromMsToMs(replicaTransferStartTime));

    LOGGER.info(
        "All files received and checksum validated successfully for replica: {} took: {}ms",
        replicaId,
        LatencyUtils.getElapsedTimeFromMsToMs(replicaTransferStartTime));

    try {
      RocksDBUtils.renameTempTransferredPartitionDirToPartitionDir(
          payload.getBaseDir(),
          payload.getTopicName(),
          payload.getPartition());
      LOGGER.info("Renamed temp partition dir to partition dir for {}", replicaId);
      inputStreamFuture.toCompletableFuture().complete(null);
    } catch (Exception e) {
      LOGGER.error(
          "Failed to rename temp partition dir to partition dir for {}. Even all the files are received, "
              + "the transfer future will be completed exceptionally. ",
          replicaId,
          e);
      // Complete future exceptionally, no need to do resource cleanup here because if files are all received, means
      // that reset was done.
      inputStreamFuture.toCompletableFuture().completeExceptionally(e);
    } finally {
      ctx.close();
    }
  }

  private void cleanupResources() {
    // 1. Close digest output stream safely by ensuring data is flushed to disk.
    if (digestOutputStream != null) {
      try {
        digestOutputStream.flush();
        digestOutputStream.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close digest output stream for {}", replicaId, e);
      }
    }

    if (fileOutputStream != null) {
      try {
        fileOutputStream.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close file output stream for {}", replicaId, e);
      }
    }
    // 2. clean up partial transferred file if exists,
    // because file will be reset if one cycle of file transfer is completed successfully,
    // then if file != null means the transfer is incomplete.
    if (file != null) {
      try {
        Files.delete(file);
      } catch (Exception e) {
        LOGGER.warn("Failed to cleanup partial file {} for {}: {}", file.getFileName(), replicaId, e.getMessage());
      }
    }
    // 3. reset states only
    resetState();
  }

  /**
   * Resets the state of this {@link P2PFileTransferClientHandler} to its initial state.
   * This method is called when a transfer is completed or failed.
   * It sets all the instance variables to their initial values, effectively resetting the state of the handler.
   */
  private void resetState() {
    digestOutputStream = null;
    fileOutputStream = null;
    fileName = null;
    fileContentLength = 0;
    file = null;
    fileChecksum = null;
    actualFileCheckSum = null;
    actualBytesWritten = 0;
    fileStreamingChecksumTimeInMs = 0;
  }

  private void fastFailoverIncompleteTransfer(String causeForFailPendingTransfer, ChannelHandlerContext ctx) {
    if (!inputStreamFuture.toCompletableFuture().isDone()) {
      String errorMessage = String.format(
          "%s for %s. Server host: %s, channel active: %b.",
          causeForFailPendingTransfer,
          replicaId,
          ctx.channel().remoteAddress(),
          ctx.channel().isActive());

      LOGGER.error(errorMessage);
      cleanupResources();
      inputStreamFuture.toCompletableFuture().completeExceptionally(new VeniceException(errorMessage));
    }
  }
}
