package com.linkedin.davinci.blobtransfer.server;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_COMPLETED;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_REQUEST_ORIGIN;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_STATUS;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_TYPE;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferRequestOrigin;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferTableFormat;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferType;
import static com.linkedin.venice.utils.NettyUtils.setupResponseAndFlush;
import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_OCTET_STREAM;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.davinci.blobtransfer.BlobSnapshotManager;
import com.linkedin.davinci.blobtransfer.BlobTransferPartitionMetadata;
import com.linkedin.davinci.blobtransfer.BlobTransferPayload;
import com.linkedin.davinci.blobtransfer.BlobTransferUtils;
import com.linkedin.davinci.stats.AggBlobTransferStats;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.request.RequestHelper;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.Utils;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.stream.ChunkedFile;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The server-side Netty handler to process requests for P2P file transfer. It's shareable among multiple requests since it doesn't
 * maintain states.
 */
@ChannelHandler.Sharable
public class P2PFileTransferServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger LOGGER = LogManager.getLogger(P2PFileTransferServerHandler.class);
  private static final String TRANSFER_TIMEOUT_ERROR_MSG_FORMAT = "Timeout for transferring blob %s file %s";
  private final String baseDir;
  // Maximum timeout for blob transfer in minutes per partition
  private final int blobTransferMaxTimeoutInMin;
  // Max allowed global concurrent snapshot users
  private final int maxAllowedConcurrentSnapshotUsers;
  private final BlobSnapshotManager blobSnapshotManager;
  // Global counter for all active transfer requests across all topics and partitions
  private final AtomicInteger globalConcurrentTransferRequests = new AtomicInteger(0);
  private final AggBlobTransferStats aggBlobTransferStats;
  // Admission control bounding concurrent client-origin (Stateful CDC) transfers against their own reservation.
  private final BlobTransferAdmissionController admissionController;
  // Whether this server accepts client-origin (e.g. Stateful CDC) blob requests.
  private final boolean serverAcceptClientBlobRequestEnabled;
  private static final AttributeKey<BlobTransferPayload> BLOB_TRANSFER_REQUEST =
      AttributeKey.valueOf("blobTransferRequest");
  private static final AttributeKey<AtomicBoolean> SUCCESS_COUNTED =
      AttributeKey.valueOf("successCountedAsActiveCurrentUser");
  // Set when a client-origin request is admitted, so channelInactive releases exactly that slot.
  private static final AttributeKey<Boolean> CLIENT_ADMITTED = AttributeKey.valueOf("blobTransferClientAdmitted");

  public P2PFileTransferServerHandler(
      String baseDir,
      int blobTransferMaxTimeoutInMin,
      BlobSnapshotManager blobSnapshotManager,
      AggBlobTransferStats aggBlobTransferStats,
      int maxAllowedConcurrentSnapshotUsers,
      BlobTransferAdmissionController admissionController,
      boolean serverAcceptClientBlobRequestEnabled) {
    this.baseDir = baseDir;
    this.blobTransferMaxTimeoutInMin = blobTransferMaxTimeoutInMin;
    this.blobSnapshotManager = blobSnapshotManager;
    this.aggBlobTransferStats = aggBlobTransferStats;
    this.maxAllowedConcurrentSnapshotUsers = maxAllowedConcurrentSnapshotUsers;
    if (serverAcceptClientBlobRequestEnabled && admissionController == null) {
      throw new IllegalArgumentException(
          "admissionController is required when client-origin blob requests are enabled");
    }
    this.admissionController = admissionController;
    this.serverAcceptClientBlobRequestEnabled = serverAcceptClientBlobRequestEnabled;
  }

  /**
   * This method is called with the request that is received from the client.
   * It validates the incoming request, and currently it only supports GET
   * @param ctx           the {@link ChannelHandlerContext} which this {@link SimpleChannelInboundHandler}
   *                      belongs to
   * @param httpRequest           the message to handle
   * @throws Exception
   */
  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest httpRequest) throws Exception {
    AtomicBoolean successCountedAsActiveCurrentUser = new AtomicBoolean(false);
    // validation
    if (!httpRequest.decoderResult().isSuccess()) {
      setupResponseAndFlush(HttpResponseStatus.BAD_REQUEST, "Request decoding failed".getBytes(), false, ctx);
      return;
    }
    if (!httpRequest.method().equals(HttpMethod.GET)) {
      setupResponseAndFlush(
          HttpResponseStatus.METHOD_NOT_ALLOWED,
          "Request method is not supported".getBytes(),
          false,
          ctx);
      return;
    }
    BlobTransferPayload blobTransferRequest = null;
    final File snapshotDir;
    BlobTransferPartitionMetadata transferPartitionMetadata;

    try {
      blobTransferRequest = parseBlobTransferPayload(URI.create(httpRequest.uri()));
      snapshotDir = new File(blobTransferRequest.getSnapshotDir());

      // Check the snapshot table format
      BlobTransferTableFormat currentSnapshotTableFormat = blobSnapshotManager.getBlobTransferTableFormat();
      if (blobTransferRequest.getRequestTableFormat() != currentSnapshotTableFormat) {
        byte[] errBody = ("Table format mismatch for " + blobTransferRequest.getFullResourceName()
            + ", current snapshot format is " + currentSnapshotTableFormat.name() + ", requested format is "
            + blobTransferRequest.getRequestTableFormat().name()).getBytes();
        setupResponseAndFlush(HttpResponseStatus.NOT_FOUND, errBody, false, ctx);
        return;
      }

      // Check the requester's metadata schema versions before doing any file work.
      // If the local binary cannot serialize PartitionState/StoreVersionState in a
      // version the requester can read, fail fast with 412 PRECONDITION_FAILED so the
      // client doesn't pay for file bytes only to reject the metadata at the end.
      String schemaMismatch = BlobTransferUtils.compareRequestedSchemaVersionsAgainstLocal(httpRequest);
      if (schemaMismatch != null) {
        LOGGER.warn("Rejecting blob-transfer request from {}: {}", ctx.channel().remoteAddress(), schemaMismatch);
        setupResponseAndFlush(
            HttpResponseStatus.PRECONDITION_FAILED,
            schemaMismatch.getBytes(StandardCharsets.UTF_8),
            false,
            ctx);
        return;
      }

      // Client-origin requests (Stateful CDC server-fallback) are admitted against their own reservation; all other
      // requests use the global concurrent-user counter.
      BlobTransferRequestOrigin origin = ctx.channel().attr(BLOB_TRANSFER_REQUEST_ORIGIN).get();
      if (serverAcceptClientBlobRequestEnabled && origin == BlobTransferRequestOrigin.CLIENT) {
        // Reserve a client-admission slot before any snapshot work, on a budget separate from the global counter so
        // client traffic cannot delay server-to-server transfers. Released in channelInactive via CLIENT_ADMITTED.
        if (!admissionController.tryAdmitClient()) {
          String errMessage = "Client-origin blob transfer rejected (client reservation full) for "
              + blobTransferRequest.getFullResourceName();
          LOGGER.warn(errMessage);
          setupResponseAndFlush(HttpResponseStatus.TOO_MANY_REQUESTS, errMessage.getBytes(), false, ctx);
          return;
        }
        ctx.channel().attr(CLIENT_ADMITTED).set(Boolean.TRUE);
        LOGGER.info(
            "Admitted client-origin (server-fallback) blob transfer request for {}; client in-flight {}/{}.",
            blobTransferRequest.getFullResourceName(),
            admissionController.getClientInFlight(),
            admissionController.getMaxClientTransfers());
        try {
          // getTransferMetadata increments the snapshot-user count and sets the success flag, then can still throw
          // (stale snapshot in use); stamp the cleanup attributes first so channelInactive releases that count.
          ctx.channel().attr(SUCCESS_COUNTED).set(successCountedAsActiveCurrentUser);
          ctx.channel().attr(BLOB_TRANSFER_REQUEST).set(blobTransferRequest);
          transferPartitionMetadata =
              blobSnapshotManager.getTransferMetadata(blobTransferRequest, successCountedAsActiveCurrentUser);
        } catch (Exception e) {
          // Close the channel (last arg) so channelInactive runs the existing cleanup and releases any
          // snapshot-user count and admission slot getTransferMetadata may have taken before throwing.
          String message = e.getMessage() == null ? "Failed to fetch transfer metadata" : e.getMessage();
          setupResponseAndFlush(HttpResponseStatus.NOT_FOUND, message.getBytes(), false, ctx, true);
          return;
        }
      } else {
        // Check the concurrent request limit
        if (globalConcurrentTransferRequests.get() >= maxAllowedConcurrentSnapshotUsers) {
          String errMessage =
              "The number of concurrent snapshot users exceeds the limit of " + maxAllowedConcurrentSnapshotUsers
                  + ", won't be able to process the request for " + blobTransferRequest.getFullResourceName();
          LOGGER.error(errMessage);
          setupResponseAndFlush(HttpResponseStatus.TOO_MANY_REQUESTS, errMessage.getBytes(), false, ctx);
          return;
        }

        try {
          transferPartitionMetadata =
              blobSnapshotManager.getTransferMetadata(blobTransferRequest, successCountedAsActiveCurrentUser);
          ctx.channel().attr(SUCCESS_COUNTED).set(successCountedAsActiveCurrentUser);
          ctx.channel().attr(BLOB_TRANSFER_REQUEST).set(blobTransferRequest);
          if (successCountedAsActiveCurrentUser.get()) {
            if (globalConcurrentTransferRequests.incrementAndGet() >= maxAllowedConcurrentSnapshotUsers) {
              String errMessage =
                  "The number of concurrent snapshot users exceeds the limit of " + maxAllowedConcurrentSnapshotUsers
                      + ", won't be able to process the request for " + blobTransferRequest.getFullResourceName();
              LOGGER.error(errMessage);
              setupResponseAndFlush(HttpResponseStatus.TOO_MANY_REQUESTS, errMessage.getBytes(), false, ctx);
              return;
            }
          }
        } catch (Exception e) {
          setupResponseAndFlush(HttpResponseStatus.NOT_FOUND, e.getMessage().getBytes(), false, ctx);
          return;
        }
      }

      if (!snapshotDir.exists() || !snapshotDir.isDirectory()) {
        byte[] errBody = ("Snapshot for " + blobTransferRequest.getFullResourceName() + " doesn't exist").getBytes();
        LOGGER.debug("Snapshot missing for {}; returning NOT_FOUND.", blobTransferRequest.getFullResourceName());
        setupResponseAndFlush(HttpResponseStatus.NOT_FOUND, errBody, false, ctx, isAdmittedClientOrigin(ctx));
        return;
      }
    } catch (IllegalArgumentException e) {
      setupResponseAndFlush(HttpResponseStatus.BAD_REQUEST, e.getMessage().getBytes(), false, ctx);
      return;
    } catch (SecurityException e) {
      setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, e.getMessage().getBytes(), false, ctx);
      return;
    }

    File[] files = snapshotDir.listFiles();
    if (files == null || files.length == 0) {
      setupResponseAndFlush(
          HttpResponseStatus.INTERNAL_SERVER_ERROR,
          ("Failed to access files at " + snapshotDir).getBytes(),
          false,
          ctx,
          isAdmittedClientOrigin(ctx));
      return;
    }

    // Set up the time limitation for the transfer
    long startTime = System.currentTimeMillis();
    String replicaInfo = Utils.getReplicaId(blobTransferRequest.getTopicName(), blobTransferRequest.getPartition());
    LOGGER.info(
        "Start transferring {} files for replica {} to remote host {}.",
        files.length,
        replicaInfo,
        ctx.channel().remoteAddress());
    // transfer files
    for (File file: files) {
      // check if the transfer for all files is timed out for this partition
      if (System.currentTimeMillis() - startTime >= TimeUnit.MINUTES.toMillis(blobTransferMaxTimeoutInMin)) {
        String errMessage =
            String.format(TRANSFER_TIMEOUT_ERROR_MSG_FORMAT, blobTransferRequest.getFullResourceName(), file.getName());
        LOGGER.error(errMessage);
        setupResponseAndFlush(
            HttpResponseStatus.REQUEST_TIMEOUT,
            errMessage.getBytes(),
            false,
            ctx,
            isAdmittedClientOrigin(ctx));
        return;
      }
      // send file
      sendFile(file, ctx, blobTransferRequest, replicaInfo);
    }

    sendMetadata(ctx, transferPartitionMetadata);

    // end of transfer
    HttpResponse endOfTransfer = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    endOfTransfer.headers().set(BLOB_TRANSFER_STATUS, BLOB_TRANSFER_COMPLETED);
    ctx.writeAndFlush(endOfTransfer).addListener(future -> {
      if (future.isSuccess()) {
        LOGGER.info("All files sent successfully for {} to host {}", replicaInfo, ctx.channel().remoteAddress());
      } else {
        LOGGER.error(
            "Failed to send all files for {} to host {}",
            replicaInfo,
            ctx.channel().remoteAddress(),
            future.cause());
      }
    });
  }

  /**
   * This method is called when the channel is inactive. It is used to decrease the concurrent user count.
   * Because the channel is inactive, we can assume that the transfer is complete.
   * If we decrease the concurrent user at channelRead0, when the connection is break in half, we will not be able to decrease the count in server side
   * @param ctx
   */
  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    AtomicBoolean successCountedAsActiveCurrentUser = ctx.channel().attr(SUCCESS_COUNTED).get();
    BlobTransferPayload blobTransferRequest = ctx.channel().attr(BLOB_TRANSFER_REQUEST).get();
    boolean clientOrigin = Boolean.TRUE.equals(ctx.channel().attr(CLIENT_ADMITTED).get());
    if (successCountedAsActiveCurrentUser != null && successCountedAsActiveCurrentUser.get()
        && blobTransferRequest != null) {
      try {
        blobSnapshotManager.decreaseConcurrentUserCount(blobTransferRequest);
        if (!clientOrigin) {
          globalConcurrentTransferRequests.decrementAndGet();
        }
      } catch (Exception e) {
        LOGGER.error("Failed to decrease the snapshot concurrent user count for request {}", blobTransferRequest, e);
      }
    }
    if (clientOrigin) {
      admissionController.releaseClient();
    }
    ctx.fireChannelInactive();
  }

  private boolean isAdmittedClientOrigin(ChannelHandlerContext ctx) {
    return Boolean.TRUE.equals(ctx.channel().attr(CLIENT_ADMITTED).get());
  }

  /**
   * Netty calls this function when events that we have registered for, occur (in this case we are specifically waiting
   * for {@link IdleStateEvent} so that we close connections that have been idle too long - maybe due to client failure)
   * @param ctx The {@link ChannelHandlerContext} that can be used to perform operations on the channel.
   * @param event The event that occurred.
   */
  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
    if (event instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) event;
      if (e.state() == IdleState.ALL_IDLE) {
        // Close the connection after idling for a certain period
        ctx.close();
        return;
      }
    }
    super.userEventTriggered(ctx, event);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    setupResponseAndFlush(HttpResponseStatus.INTERNAL_SERVER_ERROR, cause.getMessage().getBytes(), false, ctx);
    ctx.close();
  }

  private void sendFile(
      File file,
      ChannelHandlerContext ctx,
      BlobTransferPayload blobTransferPayload,
      String replicaInfo) throws IOException {
    LOGGER.info(
        "Sending file: {} for replica {} to host {}.",
        file.getName(),
        replicaInfo,
        ctx.channel().remoteAddress());
    RandomAccessFile raf = new RandomAccessFile(file, "r");
    ChannelFuture sendFileFuture;
    long length = raf.length();

    long checksumStartTime = System.currentTimeMillis();
    String fileChecksum = BlobTransferUtils.generateFileChecksum(file.toPath());
    LOGGER.info(
        "Checksum calculation for file: {} for replica {} took {} ms.",
        file.getName(),
        replicaInfo,
        System.currentTimeMillis() - checksumStartTime);

    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, length);
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, APPLICATION_OCTET_STREAM);
    response.headers().set(HttpHeaderNames.CONTENT_DISPOSITION, "attachment; filename=\"" + file.getName() + "\"");
    response.headers().set(HttpHeaderNames.CONTENT_MD5, fileChecksum);
    response.headers().set(BLOB_TRANSFER_TYPE, BlobTransferType.FILE);

    ctx.write(response);

    // Use ChunkedFile with adaptive chunk size
    // It means minimum chunk size: 16 KB (16384 bytes), maximum chunk size: 2 MB (1024 * 1024 bytes)
    int chunkSize = Math.min(2 * 1024 * 1024, (int) Math.max(16384, length / 4));
    sendFileFuture = ctx.writeAndFlush(new HttpChunkedInput(new ChunkedFile(raf, 0, length, chunkSize)));

    sendFileFuture.addListener(future -> {
      if (future.isSuccess()) {
        /**
         * Note: This does not record the real-time byte rate of files sent. If we want to pursue more accurate read metric,
         * we will need to overwrite the {@link HttpChunkedInput} above to intercept the traffic and record the byte rate.
         */
        aggBlobTransferStats.recordBlobTransferBytesSent(
            blobTransferPayload.getStoreName(),
            Version.parseVersionFromKafkaTopicName(blobTransferPayload.getTopicName()),
            length);
        LOGGER.info(
            "Sent file: {} successfully for replica: {} to host: {}",
            file.getName(),
            replicaInfo,
            ctx.channel().remoteAddress());
      } else {
        LOGGER.error(
            "Failed to send file: {} for replica: {} to host: {}",
            file.getName(),
            replicaInfo,
            ctx.channel().remoteAddress(),
            future.cause());
      }
    });
  }

  /**
   * Send metadata for the given blob transfer request
   * @param ctx the channel context
   * @param metadata the metadata to be sent
   * @throws JsonProcessingException
   */
  public void sendMetadata(ChannelHandlerContext ctx, BlobTransferPartitionMetadata metadata)
      throws JsonProcessingException {
    ObjectMapper objectMapper = ObjectMapperFactory.getInstance();
    String jsonMetadata = objectMapper.writeValueAsString(metadata);
    byte[] metadataBytes = jsonMetadata.getBytes();

    // send metadata
    FullHttpResponse metadataResponse =
        new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(metadataBytes));
    metadataResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, metadataBytes.length);
    metadataResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, APPLICATION_JSON);
    metadataResponse.headers().set(BLOB_TRANSFER_TYPE, BlobTransferType.METADATA);

    ctx.writeAndFlush(metadataResponse).addListener(future -> {
      if (future.isSuccess()) {
        LOGGER.info(
            "Metadata for {} sent successfully with size {}",
            Utils.getReplicaId(metadata.getTopicName(), metadata.getPartitionId()),
            metadataBytes.length);
      } else {
        LOGGER.error(
            "Failed to send metadata for {}",
            Utils.getReplicaId(metadata.getTopicName(), metadata.getPartitionId()),
            future.cause());
      }
    });
  }

  /**
   * Parse the URI to locate the blob
   * @param uri
   * @return
   */
  private BlobTransferPayload parseBlobTransferPayload(URI uri) throws IllegalArgumentException {
    // Parse the request uri to obtain the storeName and partition
    String[] requestParts = RequestHelper.getRequestParts(uri);

    // Ensure table format is valid
    BlobTransferTableFormat requestTableFormat;
    try {
      requestTableFormat = BlobTransferTableFormat.valueOf(requestParts[4]);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid table format: " + requestParts[4] + " for fetching blob at " + uri.getPath());
    }

    if (requestParts.length == 5) {
      // [0]""/[1]"store"/[2]"version"/[3]"partition/[4]"table format"
      return new BlobTransferPayload(
          baseDir,
          requestParts[1],
          Integer.parseInt(requestParts[2]),
          Integer.parseInt(requestParts[3]),
          requestTableFormat);
    } else {
      throw new IllegalArgumentException("Invalid request for fetching blob at " + uri.getPath());
    }
  }
}
