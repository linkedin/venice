package com.linkedin.venice.router.streaming;

import static com.linkedin.venice.router.api.VeniceResponseAggregator.COMPUTE_VALID_HEADER_MAP;
import static com.linkedin.venice.router.api.VeniceResponseAggregator.MULTI_GET_VALID_HEADER_MAP;
import static com.linkedin.venice.streaming.StreamingConstants.KEY_ID_FOR_STREAMING_FOOTER;
import static com.linkedin.venice.streaming.StreamingConstants.STREAMING_FOOTER_SCHEMA_ID;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.read.protocol.response.streaming.StreamingFooterRecordV1;
import com.linkedin.venice.router.api.VeniceResponseAggregator;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.EmptyByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GenericProgressiveFutureListener;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This is the class to talk to {@link VeniceChunkedWriteHandler} to send back partial response.
 *
 * Here is the high-level idea about how Streaming works in Venice:
 * 1. When {@link com.linkedin.venice.router.api.VenicePathParser} recognizes the incoming request
 * support streaming by looking at the streaming header: {@link HttpConstants#VENICE_STREAMING}, it
 * will create a {@link VeniceChunkedResponse} associated with {@link VenicePath};
 * 2. After scattering, {@link com.linkedin.venice.router.api.VeniceDispatcher} will send the response
 * received from Venice SN to {@link VeniceChunkedResponse#write(ByteBuf)} to Venice Client;
 * 3. In the meantime, {@link com.linkedin.venice.router.api.VeniceDispatcher} will also return an
 * empty response to the following processing logic;
 * 4. When {@link VeniceResponseAggregator} gathers all the response, it will send a {@link SuccessfulStreamingResponse}
 * to the following Netty handler, which is {@link VeniceChunkedWriteHandler}, which will delegate
 * {@link VeniceChunkedResponse} to decide how to process every response;
 * 5. For {@link SuccessfulStreamingResponse}, {@link VeniceChunkedResponse} will just skip this response,
 * but send back {@link LastHttpContent} to indicate the end of the response, and for other error response,
 * {@link VeniceChunkedResponse} will send back a {@link LastHttpContent} containing a {@link StreamingFooterRecordV1}
 * to include all the details about the error;
 */
public class VeniceChunkedResponse {
  public static final ByteBuf EMPTY_BYTE_BUF = new EmptyByteBuf(ByteBufAllocator.DEFAULT);

  private static final Logger LOGGER = LogManager.getLogger(VeniceChunkedResponse.class);

  /**
   * The following objects are used to construct a footer record for streaming response
   * to include meta data info, which are only available after processing the full request.
   */
  private static final RecordSerializer<StreamingFooterRecordV1> STREAMING_FOOTER_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(StreamingFooterRecordV1.getClassSchema());
  private static final Map<CharSequence, CharSequence> EMPTY_MAP = new HashMap<>();
  private static final RecordSerializer<MultiGetResponseRecordV1> MULTI_GET_RESPONSE_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(MultiGetResponseRecordV1.getClassSchema());
  private static final RecordSerializer<ComputeResponseRecordV1> COMPUTE_RESPONSE_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(ComputeResponseRecordV1.getClassSchema());

  private final String storeName;
  private final RequestType requestType;
  private final RouterStats<AggRouterHttpRequestStats> routerStats;
  private final String clientComputeHeader;
  private final ChannelHandlerContext ctx;
  private final VeniceChunkedWriteHandler chunkedWriteHandler;
  private final ChannelProgressivePromise writeFuture;

  // Whether the response has already completed or not
  private boolean responseCompleteCalled = false;
  private final AtomicLong totalBytesReceived = new AtomicLong(0);
  private final Queue<Chunk> chunksToWrite = new ConcurrentLinkedQueue<Chunk>();
  private final Queue<Chunk> chunksAwaitingCallback = new ConcurrentLinkedQueue<Chunk>();

  /**
   * Initialized upon the first write. If null, then no writes occurred yet, and therefore the response metadata was
   * not sent either.
   *
   * In the case of streaming batch gets, we expect that all servers should return the same value for this.
   */
  private volatile CompressionStrategy responseCompression = null;

  protected static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  /**
   * This is the response header for streaming response.
   */
  private static class StreamingResponseMetadata extends DefaultHttpResponse {
    public StreamingResponseMetadata(Map<CharSequence, String> headers) {
      super(HttpVersion.HTTP_1_1, HttpResponseStatus.OK); // So far, we choose 200 for streaming response
      headers.forEach(headers()::set);
      // Add chunked transfer encoding header here
      HttpUtil.setTransferEncodingChunked(this, true);
      headers().set(HttpConstants.VENICE_STREAMING_RESPONSE, "1");
    }
  }

  public VeniceChunkedResponse(
      String storeName,
      RequestType requestType,
      ChannelHandlerContext ctx,
      VeniceChunkedWriteHandler handler,
      RouterStats<AggRouterHttpRequestStats> routerStats,
      String clientComputeHeader) {
    this.storeName = storeName;
    this.requestType = requestType;
    this.routerStats = routerStats;
    this.clientComputeHeader = clientComputeHeader;
    if (!requestType.equals(RequestType.MULTI_GET_STREAMING) && !requestType.equals(RequestType.COMPUTE_STREAMING)) {
      throw new VeniceException(
          "Unexpected request type for streaming: " + requestType + ", and currently only"
              + " the following types are supported: [" + RequestType.MULTI_GET_STREAMING + ", "
              + RequestType.COMPUTE_STREAMING + "]");
    }
    this.ctx = ctx;
    this.chunkedWriteHandler = handler;
    this.writeFuture = ctx.newProgressivePromise();
    this.chunkedWriteHandler.setWriteMessageCallback(new WriteMessageCallbackImpl());
  }

  private static class StreamingCallbackOnlyFreeResponseOnSuccess<T> implements StreamingCallback<T> {
    private final FullHttpResponse response;
    private final ChannelPromise promise;

    private StreamingCallbackOnlyFreeResponseOnSuccess(final FullHttpResponse response, final ChannelPromise promise) {
      this.response = response;
      this.promise = promise;
    }

    @Override
    public void onCompletion(T result, Exception exception) {
      if (exception != null) {
        /**
         * For failure scenario, we will allow {@link com.linkedin.alpini.netty4.handlers.AsyncFullHttpRequestHandler#channelRead0}
         * to release the message to avoid double free issue.
         */
        promise.setFailure(exception);
      } else {
        response.release();
        promise.setSuccess();
      }
    }
  }

  private class WriteMessageCallbackImpl implements VeniceChunkedWriteHandler.WriteMessageCallback {
    /**
     * This function is used to intercept all the write calls to {@link VeniceChunkedWriteHandler}, and
     * it will make decision about how to process the current write.
     *
     * In the high-level, there will be three kinds of messages to be writen here:
     *  1. Response meta data with 'OK' status;
     *  2. {@link ChunkDispenser} to dispense all the chunks;
     *  3. Final Response sent by {@link com.linkedin.alpini.netty4.handlers.AsyncFullHttpRequestHandler};
     *
     *  For 1st type, it is the response meta for streaming response, and its type will be {@link StreamingResponseMetadata},
     *  and the write should be forwarded to the downstream handler.
     *  For 2nd type, it is the holder for all the data chunks, and it will be handled specifically by {@link ChunkedWriteHandler}.
     *  For 3rd type, there are multiple possibilities:
     *  a. Error response sent before any response metadata/data: and it will be passed to the downstream handler.
     *  b. Full response to indicate that all the sub-responses are good and handled, and its type will be {@link SuccessfulStreamingResponse},
     *  and for this case, the {@link SuccessfulStreamingResponse} will be discarded, and a {@link LastHttpContent} will be sent instead.
     *  c. Error response to indicate that some sub-responses are not good, and its type will be a regular {@link FullHttpResponse},
     *  and for this case, the original {@link FullHttpResponse} will be discarded, and a {@link LastHttpContent}
     *  containing a footer record with error status/detail will be sent instead.
     */
    @Override
    public boolean whetherToSkipMessage(Object msg, ChannelPromise promise) {
      if (msg instanceof StreamingResponseMetadata) {
        // Streaming response metadata
        return false;
      } else if (msg instanceof ChunkDispenser) {
        // Chunk dispenser
        return false;
      } else if (msg instanceof SuccessfulStreamingResponse) {
        // Full response to indicate that all the sub responses are good and handled
        finish(new StreamingCallbackOnlyFreeResponseOnSuccess<>(((SuccessfulStreamingResponse) msg), promise));
        return true;
      } else if (msg instanceof FullHttpResponse) {
        if (VeniceChunkedResponse.this.responseCompression == null) {
          // Error response before sending out any data yet
          return false;
        }
        // Error response after sending out the streaming response metadata
        FullHttpResponse response = ((FullHttpResponse) msg);
        HttpResponseStatus status = response.status();
        if (!status.equals(HttpResponseStatus.OK)) {
          finishWithError(response, new StreamingCallbackOnlyFreeResponseOnSuccess<>(response, promise));
          return true;
        } else {
          // Defensive code
          throw new IllegalStateException(
              "Unexpected response status: " + status + ", and only non 200 status is expected here");
        }
      }
      throw new VeniceException("Unexpected message type received: " + (msg == null ? "null" : msg.getClass()));
    }
  }

  public CompletableFuture<Long> write(ByteBuf buffer) {
    return write(buffer, CompressionStrategy.NO_OP, null);
  }

  /**
   * This function is used to send a data chunk to Venice Client.
   * @param buffer
   * @param compression
   */
  public CompletableFuture<Long> write(ByteBuf buffer, CompressionStrategy compression) {
    return write(buffer, compression, null);
  }

  /**
   * This function is used to send a data chunk to Venice Client, and when it is completed, 'callback' will be invoked.
   * @param byteBuf
   * @param compression
   * @param callback
   * @return
   */
  public CompletableFuture<Long> write(
      ByteBuf byteBuf,
      CompressionStrategy compression,
      StreamingCallback<Long> callback) {
    boolean isFirstWrite = false;
    if (this.responseCompression == null) {
      synchronized (this) {
        if (this.responseCompression == null) {
          this.responseCompression = compression;
          isFirstWrite = true;
        }
      }
    }

    boolean isMultiGetStreaming = this.requestType.equals(RequestType.MULTI_GET_STREAMING);
    if (isMultiGetStreaming && !this.responseCompression.equals(compression)) {
      // Defensive code, not expected
      LOGGER.error(
          "Received inconsistent compression for the new write: {}, and previous compression: {}",
          compression,
          this.responseCompression);
      /**
       * Skip the write with wrong compression.
       * {@link VeniceResponseAggregator#buildStreamingResponse} will perform the same check
       * when all the responses returned by storage nodes are ready, and when the inconsistency happens,
       * {@link VeniceResponseAggregator} will return an error response instead to indicate the issue.
       */
      return CompletableFuture.completedFuture(0L);
    }

    /**
      We would like to send out response metadata as late as possible, so that the error happened earlier
      (such as request parsing error) could still be sent out with the right status code
     */
    if (isFirstWrite) {
      // Send out response metadata
      Map<CharSequence, String> headers =
          new HashMap<>(isMultiGetStreaming ? MULTI_GET_VALID_HEADER_MAP : COMPUTE_VALID_HEADER_MAP);
      if (this.clientComputeHeader != null) {
        headers.put(HttpConstants.VENICE_CLIENT_COMPUTE, clientComputeHeader);
      }
      headers.put(HttpConstants.VENICE_COMPRESSION_STRATEGY, Integer.toString(compression.getValue()));
      ChannelPromise writePromise = ctx.newPromise().addListener(new ResponseMetadataWriteListener());
      chunkedWriteHandler.write(ctx, new StreamingResponseMetadata(headers), writePromise);
      /**
       * {@link ChunkedWriteHandler#resumeTransfer()} invocation will try to flush more data to the client since more
       * data is available because of the previous write.
       */
      chunkedWriteHandler.resumeTransfer();
    }

    Chunk chunk = new Chunk(byteBuf, false, callback);

    if (!maybeAddChunk(chunk)) {
      // Chunk will be skipped
      return chunk.future;
    }

    if (!ctx.channel().isOpen()) {
      // When channel is not open any more, here needs to clean up all the added chunks.
      writeFuture.addListener(new CleanupCallback(new ClosedChannelException()));
    } else {
      chunkedWriteHandler.resumeTransfer();
    }

    return chunk.future;
  }

  /**
   * Synchronized way to add data chunks to {@link #chunksToWrite}, and it will try to avoid adding more chunks
   * after producing last chunk.
   *
   * @param chunk
   * @return
   */
  private synchronized boolean maybeAddChunk(Chunk chunk) {
    if (responseCompleteCalled) {
      // Skip adding more chunks since the last http content has been sent
      chunk.resolveChunk(null);
      return false;
    }
    chunksToWrite.add(chunk);
    if (chunk.isLast) {
      responseCompleteCalled = true;
    }
    return true;
  }

  private void reportResponseSize() {
    routerStats.getStatsByType(this.requestType).recordResponseSize(this.storeName, totalBytesReceived.get());
  }

  /**
   * Finish the response without any error
   */
  private void finish(StreamingCallback<Long> callback) {
    Chunk lastChunk = new Chunk(EMPTY_BYTE_BUF, true, callback);
    if (maybeAddChunk(lastChunk)) {
      reportResponseSize();
      chunkedWriteHandler.resumeTransfer();
    } else {
      LOGGER.error("Couldn't add last chunk to the chunk queue");
    }
  }

  /**
   * Finish the response with an error
    */
  private void finishWithError(FullHttpResponse errorResponse, StreamingCallback<Long> callback) {
    LOGGER.debug("Finishing the chunked transfer response with status: {}", errorResponse.status());

    StreamingFooterRecordV1 footerRecord = new StreamingFooterRecordV1();
    footerRecord.status = errorResponse.status().code();
    footerRecord.detail = errorResponse.content().nioBuffer();
    footerRecord.trailerHeaders = EMPTY_MAP;
    ByteBuffer footerByteBuffer = ByteBuffer.wrap(STREAMING_FOOTER_SERIALIZER.serialize(footerRecord));

    ByteBuf footerResponse;
    if (this.requestType.equals(RequestType.MULTI_GET_STREAMING)) {
      MultiGetResponseRecordV1 record = new MultiGetResponseRecordV1();
      record.keyIndex = KEY_ID_FOR_STREAMING_FOOTER;
      record.value = footerByteBuffer;
      record.schemaId = STREAMING_FOOTER_SCHEMA_ID;
      footerResponse = Unpooled.wrappedBuffer(MULTI_GET_RESPONSE_SERIALIZER.serialize(record));
    } else if (requestType.equals(RequestType.COMPUTE_STREAMING)) {
      ComputeResponseRecordV1 record = new ComputeResponseRecordV1();
      record.keyIndex = KEY_ID_FOR_STREAMING_FOOTER;
      record.value = footerByteBuffer;
      footerResponse = Unpooled.wrappedBuffer(COMPUTE_RESPONSE_SERIALIZER.serialize(record));
    } else {
      // not possible
      LOGGER.error("Received unsupported request type: {} for streaming response", this.requestType);
      return;
    }

    Chunk lastChunk = new Chunk(footerResponse, true, callback);
    if (maybeAddChunk(lastChunk)) {
      reportResponseSize();
      chunkedWriteHandler.resumeTransfer();
    } else {
      LOGGER.error("Couldn't add last chunk with error status: {} to the internal chunk queue", errorResponse.status());
    }
  }

  /**
   * Handle channel write failure, and this function will try to clean up all the data chunks left and close the channel.
   *
   * The reason to use `synchronized` since both {@link #maybeAddChunk(Chunk)} and this function could potentially change
   * {@link #responseCompleteCalled} variable.
   *
   * * @param cause
   * @param propagateErrorIfRequired
   */
  private synchronized void handleChannelWriteFailure(Throwable cause, boolean propagateErrorIfRequired) {
    String msg = "Encountered a throwable on channel write failure on channel: ";
    if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
      LOGGER.error("{}{}", msg, ctx.channel(), cause);
    } else {
      LOGGER.error("{}{}", msg, ctx.channel());
    }

    Exception exception;
    if (!(cause instanceof Exception)) {
      exception = new IllegalStateException("Encountered a Throwable - " + cause.getMessage());
      if (propagateErrorIfRequired) {
        // we can't ignore throwables - so we let Netty deal with it.
        ctx.fireExceptionCaught(cause);
      }
    } else {
      exception = (Exception) cause;
    }
    if (!responseCompleteCalled) {
      responseCompleteCalled = true;
      LOGGER.debug("Finished responding to current request on channel: {} with exception: ", ctx.channel(), exception);
    }
    if (!writeFuture.isDone()) {
      writeFuture.setFailure(exception);
    }
    // close the channel
    closeChannel();

    // clean up chunks left
    cleanupChunks(exception);
  }

  /**
   * Clean up all the chunks, and it includes the following steps:
   * 1. Clean up all the data chunks awaiting for callback;
   * 2. Clean up all the data chunks, which haven't been sent yet;
   * @param exception
   */
  private void cleanupChunks(Exception exception) {
    Chunk chunk = chunksAwaitingCallback.poll();
    while (chunk != null) {
      chunk.resolveChunk(exception);
      chunk = chunksAwaitingCallback.poll();
    }
    chunk = chunksToWrite.poll();
    while (chunk != null) {
      chunk.resolveChunk(exception);
      chunk = chunksToWrite.poll();
    }
  }

  private void closeChannel() {
    if (ctx.channel().isOpen()) {
      writeFuture.addListener(ChannelFutureListener.CLOSE);
    }
  }

  /**
   * Listener that will be attached to the write of a response metadata.
   */
  private class ResponseMetadataWriteListener implements GenericFutureListener<ChannelFuture> {
    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if (future.isSuccess()) {
        writeFuture.addListener(new ChunkCallbackListener());
        chunkedWriteHandler.write(ctx, new ChunkDispenser(), writeFuture);
        chunkedWriteHandler.resumeTransfer();
      } else {
        String msg = "Received exception when sending response metadata";
        if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
          LOGGER.error(msg, future.cause());
        } else {
          LOGGER.error(msg);
        }
        /**
         * We need to do some cleanup here:
         * 1. free all the chunks;
         * 2. trigger all the callbacks;
         */
        handleChannelWriteFailure(future.cause(), true);
      }
    }
  }

  /**
   * Invokes callback on any chunks that are eligible for callback.
   */
  private class ChunkCallbackListener implements GenericProgressiveFutureListener<ChannelProgressiveFuture> {
    /**
     * Uses {@code progress} to determine chunks whose callbacks need to be invoked.
     * @param future the {@link ChannelProgressiveFuture} that is being listened on.
     * @param progress the total number of bytes that have been written starting from the time writes were invoked via
     *                 {@link ChunkedWriteHandler}.
     * @param total the total number of bytes that need to be written i.e. the target number. This is not relevant to
     *              {@link ChunkedWriteHandler} because there is no target number. All calls to this function except
     *              the very last one will have a negative {@code total}.
     */
    @Override
    public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) {
      /**
       * The netty version being used doesn't seem to provide the progress correctly when H2 is enabled, so we are not going
       * to release any chunks here, but until the full write is complete.
       *
       * If we release the chunks early while the underlying write is still in progress, Router would end up
       * with such exception:
       * io.netty.util.IllegalReferenceCountException: refCnt: 0
       *         at io.netty.buffer.AbstractByteBuf.ensureAccessible(AbstractByteBuf.java:1489) ~[io.netty.netty-all-4.1.52.Final.jar:4.1.52.Final]
       *         at io.netty.buffer.AbstractByteBuf.checkReadableBytes0(AbstractByteBuf.java:1475) ~[io.netty.netty-all-4.1.52.Final.jar:4.1.52.Final]
       *         at io.netty.buffer.AbstractByteBuf.checkReadableBytes(AbstractByteBuf.java:1463) ~[io.netty.netty-all-4.1.52.Final.jar:4.1.52.Final]
       *         at io.netty.buffer.AbstractByteBuf.readRetainedSlice(AbstractByteBuf.java:888) ~[io.netty.netty-all-4.1.52.Final.jar:4.1.52.Final]
       *         at io.netty.handler.codec.http2.DefaultHttp2FrameWriter.writeData(DefaultHttp2FrameWriter.java:158) [io.netty.netty-all-4.1.52.Final.jar:4.1.52.Final]
       *         at io.netty.handler.codec.http2.Http2OutboundFrameLogger.writeData(Http2OutboundFrameLogger.java:44) [io.netty.netty-all-4.1.52.Final.jar:4.1.52.Final]
       *         at io.netty.handler.codec.http2.DefaultHttp2ConnectionEncoder$FlowControlledData.write(DefaultHttp2ConnectionEncoder.java:508) [io.netty.netty-all-4.1.52.Final.jar:4.1.52.Final]
       */
    }

    /**
     * Called once the write is complete i.e. either all chunks that were needed to be written were written or there
     * was an error  writing the chunks.
     * @param future the {@link ChannelProgressiveFuture} that is being listened on.
     */
    @Override
    public void operationComplete(ChannelProgressiveFuture future) {
      if (!future.isSuccess()) {
        handleChannelWriteFailure(future.cause(), true);
      } else {
        // Need to clean up all the chunks when complete
        cleanupChunks(null);
      }
    }
  }

  /**
   * This class is used to clean up all the data left when error happens.
   */
  private class CleanupCallback implements GenericFutureListener<ChannelFuture> {
    private final Exception exception;

    /**
     * Instantiate a CleanupCallback with an exception to return once cleanup is activated.
     * @param exception the {@link Exception} to return as a part of the callback. Can be {@code null}. This can be
     *                  overriden if the channel write ended in failure.
     */
    CleanupCallback(Exception exception) {
      this.exception = exception;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      Throwable cause = future.cause() == null ? exception : future.cause();
      if (cause != null) {
        // There was an issue when writing data to the channel
        handleChannelWriteFailure(cause, false);
      } else {
        // The write was successful
        cleanupChunks(null);
      }
    }
  }

  /** Data chunk */
  private class Chunk {
    final CompletableFuture<Long> future = new CompletableFuture<>();
    final StreamingCallback<Long> callback;

    /** data buffer */
    final ByteBuf buffer;
    /** whether this is the last chunk */
    final boolean isLast;
    /** bytes to be writen for this chunk */
    final long bytesToBeWritten;

    public Chunk(ByteBuf buffer, boolean isLast, StreamingCallback<Long> streamingCallback) {
      this.buffer = buffer;
      this.isLast = isLast;
      this.callback = streamingCallback;

      this.bytesToBeWritten = buffer.readableBytes();
      totalBytesReceived.addAndGet(bytesToBeWritten);
    }

    /**
     * Marks a chunk as handled and invokes the callback and future that accompanied this chunk of data. Once a chunk is
     * resolved, the data inside it is considered void.
     * @param exception the reason for chunk handling failure.
     */
    void resolveChunk(Exception exception) {
      long bytesWritten = 0;
      if (exception == null) {
        bytesWritten = bytesToBeWritten;
        future.complete(bytesToBeWritten);
      } else {
        future.completeExceptionally(exception);
      }
      if (callback != null) {
        callback.onCompletion(bytesWritten, exception);
      }
      // The internal buffer will be freed when the chunk has been resolved.
      if (buffer.refCnt() > 0) {
        /**
         *  In the regular case, the buffer could be already freed by the downstream handler.
         *  If the chunk is skipped because of any reason, the buffer needs to be freed explicitly here.
          */
        ReferenceCountUtil.release(buffer);
      }
    }
  }

  /**
   * This Object is used to send the buffered chunks in {@link #chunksToWrite} to {@link VeniceChunkedWriteHandler}.
   */
  private class ChunkDispenser implements ChunkedInput<HttpContent> {
    private boolean sentLastChunk = false;
    private final AtomicInteger progress = new AtomicInteger(0);

    @Override
    public boolean isEndOfInput() throws Exception {
      return sentLastChunk;
    }

    @Override
    public void close() throws Exception {
      // do nothing
    }

    @Override
    public HttpContent readChunk(ChannelHandlerContext ctx) throws Exception {
      return readChunk(ctx.alloc());
    }

    /**
     * This function will be invoked by {@link ChunkedWriteHandler#resumeTransfer()} to poll data chunk from {@link #chunksToWrite}.
     * @param allocator
     * @return
     * @throws Exception
     */
    @Override
    public HttpContent readChunk(ByteBufAllocator allocator) throws Exception {
      HttpContent content = null;
      Chunk chunk = chunksToWrite.poll();
      if (chunk != null) {
        progress.addAndGet(chunk.buffer.readableBytes());
        chunksAwaitingCallback.add(chunk);
        /**
         * Retain the buffer before passing to Netty. Netty will release the buffer after the write
         * completes (success or failure), and we also release it in resolveChunk(). Without retain(),
         * this would cause IllegalReferenceCountException when client disconnects abruptly and both
         * Netty cleanup and resolveChunk() try to release the same buffer.
         */
        if (chunk.isLast) {
          content = new DefaultLastHttpContent(chunk.buffer.retain());
          sentLastChunk = true;
        } else {
          content = new DefaultHttpContent(chunk.buffer.retain());
        }
      }
      return content;
    }

    @Override
    public long length() {
      // unknown
      return -1;
    }

    @Override
    public long progress() {
      return progress.get();
    }
  }
}
