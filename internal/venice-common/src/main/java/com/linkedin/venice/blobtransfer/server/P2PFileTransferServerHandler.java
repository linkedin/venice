package com.linkedin.venice.blobtransfer.server;

import static com.linkedin.venice.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_COMPLETED;
import static com.linkedin.venice.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_STATUS;
import static com.linkedin.venice.utils.NettyUtils.setupResponseAndFlush;
import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_OCTET_STREAM;

import com.linkedin.venice.blobtransfer.BlobTransferPayload;
import com.linkedin.venice.request.RequestHelper;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedFile;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The server-side Netty handler to process requests for P2P file transfer. It's shareable among multiple requests since it doesn't
 * maintain states.
 */
@ChannelHandler.Sharable
public class P2PFileTransferServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger LOGGER = LogManager.getLogger(P2PFileTransferServerHandler.class);
  private boolean useZeroCopy = false;
  private final String baseDir;

  public P2PFileTransferServerHandler(String baseDir) {
    this.baseDir = baseDir;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    LOGGER.trace("Channel {} active", ctx.channel());
    if (ctx.pipeline().get(SslHandler.class) == null) {
      useZeroCopy = true;
      LOGGER.debug("SSL not enabled. Use Zero-Copy for file transfer");
    }
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
    final BlobTransferPayload blobTransferRequest;
    final File snapshotDir;
    try {
      blobTransferRequest = parseBlobTransferPayload(httpRequest);
      snapshotDir = new File(blobTransferRequest.getSnapshotDir());
      if (!snapshotDir.exists() || !snapshotDir.isDirectory()) {
        byte[] errBody = ("Snapshot for " + blobTransferRequest.getFullResourceName() + " doesn't exist").getBytes();
        setupResponseAndFlush(HttpResponseStatus.NOT_FOUND, errBody, false, ctx);
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
          ctx);
      return;
    }

    // transfer files
    for (File file: files) {
      sendFile(file, ctx);
    }

    // end of transfer
    HttpResponse endOfTransfer = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    endOfTransfer.headers().set(BLOB_TRANSFER_STATUS, BLOB_TRANSFER_COMPLETED);
    ctx.writeAndFlush(endOfTransfer).addListener(future -> {
      if (future.isSuccess()) {
        LOGGER.debug("All files sent successfully for {}", blobTransferRequest.getFullResourceName());
      } else {
        LOGGER.error("Failed to send all files for {}", blobTransferRequest.getFullResourceName(), future.cause());
      }
    });
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

  private void sendFile(File file, ChannelHandlerContext ctx) throws IOException {
    RandomAccessFile raf = new RandomAccessFile(file, "r");
    ChannelFuture sendFileFuture;
    ChannelFuture lastContentFuture;
    long length = raf.length();
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, length);
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, APPLICATION_OCTET_STREAM);
    response.headers().set(HttpHeaderNames.CONTENT_DISPOSITION, "attachment; filename=\"" + file.getName() + "\"");

    ctx.write(response);

    if (useZeroCopy) {
      sendFileFuture = ctx.writeAndFlush(new DefaultFileRegion(raf.getChannel(), 0, length));
      lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
    } else {
      sendFileFuture = ctx.writeAndFlush(new HttpChunkedInput(new ChunkedFile(raf)));
      lastContentFuture = sendFileFuture;
    }

    sendFileFuture.addListener(future -> {
      if (future.isSuccess()) {
        LOGGER.debug("File {} sent successfully", file.getName());
      } else {
        LOGGER.error("Failed to send file {}", file.getName());
      }
    });

    lastContentFuture.addListener(future -> {
      if (future.isSuccess()) {
        LOGGER.debug("Last content sent successfully for {}", file.getName());
      } else {
        LOGGER.error("Failed to send last content for {}", file.getName());
      }
    });
  }

  /**
   * Parse the URI to locate the blob
   * @param request
   * @return
   */
  private BlobTransferPayload parseBlobTransferPayload(HttpRequest request) throws IllegalArgumentException {
    // Parse the request uri to obtain the storeName and partition
    String uri = request.uri();
    String[] requestParts = RequestHelper.getRequestParts(uri);
    if (requestParts.length == 4) {
      // [0]""/[1]"store"/[2]"version"/[3]"partition"
      return new BlobTransferPayload(
          baseDir,
          requestParts[1],
          Integer.parseInt(requestParts[2]),
          Integer.parseInt(requestParts[3]));
    } else {
      throw new IllegalArgumentException("Invalid request for fetching blob at " + uri);
    }
  }
}
