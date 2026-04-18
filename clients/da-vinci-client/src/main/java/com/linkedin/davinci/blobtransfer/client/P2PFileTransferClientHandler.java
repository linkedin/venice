package com.linkedin.davinci.blobtransfer.client;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_COMPLETED;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_STATUS;

import com.linkedin.davinci.blobtransfer.BlobTransferPayload;
import com.linkedin.davinci.blobtransfer.BlobTransferUtils;
import com.linkedin.davinci.stats.AggBlobTransferStats;
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
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private final ExecutorService checksumValidationExecutorService;
  private final AggBlobTransferStats aggBlobTransferStats;
  private final List<CompletableFuture<Void>> checksumValidationFutureList = new ArrayList<>();
  private final String storeName;
  private final int version;
  // When true, AUTO_READ=false on the bootstrap and the handler drives socket reads via explicit ctx.read() calls
  // after each chunk's disk write completes. Blocking FileChannel writes also run on {@link #diskWriteExecutorService}
  // instead of the Netty event loop.
  private final boolean backpressureEnabled;
  // Non-null iff backpressureEnabled is true.
  private final ExecutorService diskWriteExecutorService;
  // FIFO queue of pending disk-write units, along with a single-flight flag that ensures at most one unit per channel
  // is executing on {@link #diskWriteExecutorService}. Content chunks and the end-of-transfer marker both go through
  // this queue so that the end-of-transfer processing observes the complete {@link #checksumValidationFutureList}.
  // Only touched from the channel's event loop, so no synchronization beyond the AtomicBoolean flag is required.
  private final Deque<ThrowingRunnable> pendingDiskTasks = new ArrayDeque<>();
  private final AtomicBoolean diskWriteTaskInFlight = new AtomicBoolean(false);

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  // mutable states for a single file transfer. It will be updated for each file transfer.
  private FileChannel outputFileChannel;
  private String fileName;
  private String fileChecksum;
  private Path file;
  private long fileContentLength;
  private final String replicaId;
  private long fileTransferStartTime;
  private final long replicaTransferStartTime;

  public P2PFileTransferClientHandler(
      String baseDir,
      CompletionStage<InputStream> inputStreamFuture,
      String storeName,
      int version,
      int partition,
      BlobTransferUtils.BlobTransferTableFormat tableFormat,
      AggBlobTransferStats aggBlobTransferStats,
      ExecutorService checksumValidationExecutorService) {
    this(
        baseDir,
        inputStreamFuture,
        storeName,
        version,
        partition,
        tableFormat,
        aggBlobTransferStats,
        checksumValidationExecutorService,
        false,
        null);
  }

  public P2PFileTransferClientHandler(
      String baseDir,
      CompletionStage<InputStream> inputStreamFuture,
      String storeName,
      int version,
      int partition,
      BlobTransferUtils.BlobTransferTableFormat tableFormat,
      AggBlobTransferStats aggBlobTransferStats,
      ExecutorService checksumValidationExecutorService,
      boolean backpressureEnabled,
      ExecutorService diskWriteExecutorService) {
    this.inputStreamFuture = inputStreamFuture;
    this.payload = new BlobTransferPayload(baseDir, storeName, version, partition, tableFormat);
    this.storeName = storeName;
    this.version = version;
    this.replicaId = Utils.getReplicaId(payload.getTopicName(), payload.getPartition());
    this.checksumValidationExecutorService = checksumValidationExecutorService;
    this.aggBlobTransferStats = aggBlobTransferStats;
    this.replicaTransferStartTime = System.currentTimeMillis();
    this.backpressureEnabled = backpressureEnabled;
    if (backpressureEnabled && diskWriteExecutorService == null) {
      throw new IllegalArgumentException(
          "diskWriteExecutorService must be provided when backpressureEnabled is true for replica " + replicaId);
    }
    this.diskWriteExecutorService = diskWriteExecutorService;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    // When AUTO_READ is disabled, Netty will not initiate the first socket read automatically.
    // Request the first read once the pipeline is ready so the GET response can be received.
    if (backpressureEnabled) {
      ctx.read();
    }
    super.channelActive(ctx);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
    if (!backpressureEnabled) {
      // Legacy path: process everything inline on the event loop.
      handleMessage(ctx, msg);
      return;
    }

    if (msg instanceof HttpContent) {
      // Retain and enqueue the chunk; the FIFO queue ensures sequential writes to the same FileChannel even when
      // one ctx.read() synchronously dispatches multiple HttpContent objects via HttpClientCodec.
      HttpContent content = (HttpContent) msg;
      content.retain();
      enqueueDiskTask(ctx, () -> {
        try {
          handleMessage(ctx, content);
        } finally {
          ReferenceCountUtil.release(content);
        }
      });
      return;
    }

    // HttpResponse path: most headers are cheap, but the end-of-transfer marker joins on the checksum-validation
    // futures — those futures are enrolled during LastHttpContent processing on the disk executor, so we MUST
    // defer end-of-transfer handling through the same queue. Otherwise an EndOfTransfer that arrives in the same
    // socket read as the last content chunk would observe an empty {@link #checksumValidationFutureList}.
    //
    // File-header HttpResponse messages also go through the queue when there are pending content writes, so the
    // next file's outputFileChannel setup stays serialized behind the previous file's LastHttpContent write.
    if ((msg instanceof HttpResponse && isEndOfTransferResponse((HttpResponse) msg)) || !pendingDiskTasks.isEmpty()
        || diskWriteTaskInFlight.get()) {
      enqueueDiskTask(ctx, () -> handleMessage(ctx, msg));
      return;
    }

    // Fast-path: nothing pending on disk → handle inline on the event loop, then pull the next socket read.
    handleMessage(ctx, msg);
    if (ctx.channel().isActive() && !inputStreamFuture.toCompletableFuture().isDone()) {
      ctx.read();
    }
  }

  private static boolean isEndOfTransferResponse(HttpResponse response) {
    return response.headers().get(BLOB_TRANSFER_STATUS) != null
        && response.headers().get(BLOB_TRANSFER_STATUS).equals(BLOB_TRANSFER_COMPLETED)
        && !BlobTransferUtils.isMetadataMessage(response);
  }

  private void enqueueDiskTask(ChannelHandlerContext ctx, ThrowingRunnable task) {
    pendingDiskTasks.addLast(task);
    if (diskWriteTaskInFlight.compareAndSet(false, true)) {
      submitNextDiskTask(ctx);
    }
  }

  /**
   * Pops the next pending unit off {@link #pendingDiskTasks} and runs it on {@link #diskWriteExecutorService},
   * chaining to itself on completion to drain the queue in FIFO order. When the queue is empty, releases the
   * single-flight flag and issues {@code ctx.read()} so the next socket read can bring more data into the pipeline.
   */
  private void submitNextDiskTask(ChannelHandlerContext ctx) {
    ThrowingRunnable next = pendingDiskTasks.pollFirst();
    if (next == null) {
      diskWriteTaskInFlight.set(false);
      if (ctx.channel().isActive() && !inputStreamFuture.toCompletableFuture().isDone()) {
        ctx.read();
      }
      return;
    }
    diskWriteExecutorService.execute(() -> {
      Throwable failure = null;
      try {
        next.run();
      } catch (Throwable t) {
        failure = t;
      }
      final Throwable firedFailure = failure;
      Runnable followUp = () -> {
        if (firedFailure != null) {
          // Mirror what SimpleChannelInboundHandler would do if channelRead0 had thrown inline: route the failure
          // through this handler's own exceptionCaught, not to the next handler in the pipeline. Any still-queued
          // tasks are dropped because handleExceptionGracefully also drains the queue and releases retained buffers.
          exceptionCaught(ctx, firedFailure);
          return;
        }
        submitNextDiskTask(ctx);
      };
      // When the disk task runs on the event-loop thread (e.g. under a direct/caller-runs executor in tests),
      // run the follow-up inline; otherwise hop back to the event loop to keep pipeline mutations and queue access
      // single-threaded.
      if (ctx.channel().eventLoop().inEventLoop()) {
        followUp.run();
      } else {
        ctx.channel().eventLoop().execute(followUp);
      }
    });
  }

  private void handleMessage(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
    // Early return if the transfer is already completed or failed.
    if (inputStreamFuture.toCompletableFuture().isDone()) {
      return;
    }

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
      aggBlobTransferStats.recordBlobTransferBytesReceived(storeName, version, totalBytesToTransfer);

      if (content instanceof DefaultLastHttpContent) {
        // End of a single file transfer
        LOGGER.info(
            "A file: {} received successfully for replica: {} took: {}ms",
            fileName,
            replicaId,
            LatencyUtils.getElapsedTimeFromMsToMs(fileTransferStartTime));
        outputFileChannel.force(true);

        // Size validation
        if (outputFileChannel.size() != fileContentLength) {
          throw new VeniceException(
              "File size mismatch for " + fileName + ". Expected: " + fileContentLength + ", Actual: "
                  + outputFileChannel.size());
        }
        // Close the file channel
        try {
          outputFileChannel.close();
          LOGGER.info("Closed file channel for: {} for replica: {}", fileName, replicaId);
        } catch (Exception e) {
          LOGGER.warn(
              "Failed to close file channel for file {} for replica {} with error: {}",
              fileName,
              replicaId,
              e.getMessage());
        }

        // Perform checksum validation asynchronously to avoid blocking the Netty event loop
        CompletableFuture<Void> checksumFuture = new CompletableFuture<>();
        checksumValidationFutureList.add(checksumFuture);
        performAsyncChecksumValidation(checksumFuture, this.file, this.fileChecksum, this.fileName);

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
        "Channel close before completing transfer, might due to server graceful shutdown or timeout.",
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
      LOGGER.error("Exception caught in when receiving files for replica: {} with cause: {}", replicaId, cause);
      cleanupResources();
      // Mark the future done BEFORE draining pending disk tasks so their handleMessage calls short-circuit via the
      // isDone() guard instead of writing to the just-closed outputFileChannel. releasePendingContents then only
      // exists to release retained ByteBufs.
      inputStreamFuture.toCompletableFuture().completeExceptionally(cause);
      releasePendingContents();
    }

  }

  /**
   * Drains any disk tasks that were queued but not yet executed (e.g. because an earlier chunk failed) so the channel
   * can be closed cleanly. Each queued task runnable internally owns its buffer release via its own try/finally, so
   * running the remaining tasks here is the safest way to guarantee retained ByteBufs are released without re-running
   * file-write logic against a closed file channel — {@link #handleMessage} short-circuits when the transfer future
   * is already done, which {@link #handleExceptionGracefully} sets before calling this method.
   */
  private void releasePendingContents() {
    ThrowingRunnable pending;
    while ((pending = pendingDiskTasks.pollFirst()) != null) {
      try {
        pending.run();
      } catch (Throwable ignored) {
        // Best-effort drain — we're already in an error path; swallow secondary failures to avoid masking the root
        // cause that initiated the cleanup.
      }
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

    // Wait for all the checksum validation futures to complete.
    CompletableFuture.allOf(checksumValidationFutureList.toArray(new CompletableFuture[0])).join();
    // Check the exception holder to see if any checksum validation failed.
    Throwable checksumThrowable = checksumExceptionHolder.get();
    if (checksumThrowable != null) {
      LOGGER.error(
          "Caught exception: {} in checksum validation for replica: {}",
          checksumThrowable.getMessage(),
          replicaId);
      throw new VeniceException(checksumThrowable);
    }

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

    // 1. Close file channel safely by ensuring data is flushed to disk.
    if (outputFileChannel != null) {
      try {
        if (outputFileChannel.isOpen()) {
          outputFileChannel.force(true);
          outputFileChannel.close();
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to close file channel for {}", replicaId, e);
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
    outputFileChannel = null;
    fileName = null;
    fileContentLength = 0;
    file = null;
    fileChecksum = null;
  }

  /**
   * Performs checksum validation asynchronously to avoid blocking the Netty event loop thread.
   * This method offloads the CPU-intensive checksum computation to a separate thread pool.
   */
  private void performAsyncChecksumValidation(
      CompletableFuture<Void> checksumValidationFuture,
      Path fileToValidate,
      String expectedChecksum,
      String fileNameToValidate) {
    // Capture current state variables before async execution
    long startTime = System.currentTimeMillis();
    CompletableFuture.supplyAsync(() -> {
      try {
        // Perform the CPU-intensive checksum computation in the background thread
        long checksumGenerationStartTime = System.currentTimeMillis();
        String checksum = BlobTransferUtils.generateFileChecksum(fileToValidate);
        LOGGER.info(
            "Checksum generation for file: {} for replica: {} took: {}ms",
            fileNameToValidate,
            replicaId,
            LatencyUtils.getElapsedTimeFromMsToMs(checksumGenerationStartTime));
        return checksum;
      } catch (Exception e) {
        throw new RuntimeException("Failed to generate checksum for file: " + fileNameToValidate, e);
      }
    }, checksumValidationExecutorService).whenComplete((actualChecksum, throwable) -> {
      if (throwable != null) {
        LOGGER.error(
            "Caught exception when generating checksum for file: {} for replica: {}",
            fileNameToValidate,
            replicaId);
        checksumExceptionHolder.compareAndSet(null, throwable);
        checksumValidationFuture.complete(null);
        return;
      }
      if (!actualChecksum.equals(expectedChecksum)) {
        String errorMessage = String.format(
            "File checksum mismatch for file: %s for replica: %s. Expected: %s, actual: %s",
            fileToValidate,
            replicaId,
            expectedChecksum,
            actualChecksum);
        LOGGER.error(errorMessage);
        VeniceException exception = new VeniceException(errorMessage);
        checksumExceptionHolder.compareAndSet(null, exception);
        checksumValidationFuture.complete(null);
        return;
      }
      checksumValidationFuture.complete(null);

      // Checksum validation passed, complete the file transfer
      LOGGER.info(
          "Checksum validation passed for file {}: for replica: {} took: {}ms",
          fileNameToValidate,
          replicaId,
          LatencyUtils.getElapsedTimeFromMsToMs(startTime));
    });
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
      // Mark done BEFORE releasePendingContents so that drained runnables short-circuit via handleMessage's
      // isDone() guard and only release the retained ByteBufs instead of writing to the just-closed file channel.
      inputStreamFuture.toCompletableFuture().completeExceptionally(new VeniceException(errorMessage));
      releasePendingContents();
    }
  }
}
