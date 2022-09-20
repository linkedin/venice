package com.linkedin.venice.router.streaming;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.handler.stream.ChunkedFile;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.nio.channels.ClosedChannelException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * This class was copied from {@link ChunkedWriteHandler}, and the only change we made here is to
 * using {@link ConcurrentLinkedQueue} for {@link #queue} since {@link #resumeTransfer()} or {@link #discard(Throwable)}
 * could happen in any sequence with any parallelism because of scatter/gather logic in Venice Router.
 * TODO: sync up with Netty team to see whether we could incorporate this change into Netty code base or not.
 *
 * Original Javadoc starts from here:
 * A {@link ChannelHandler} that adds support for writing a large data stream
 * asynchronously neither spending a lot of memory nor getting
 * {@link OutOfMemoryError}.  Large data streaming such as file
 * transfer requires complicated state management in a {@link ChannelHandler}
 * implementation.  {@link ChunkedWriteHandler} manages such complicated states
 * so that you can send a large data stream without difficulties.
 * <p>
 * To use {@link ChunkedWriteHandler} in your application, you have to insert
 * a new {@link ChunkedWriteHandler} instance:
 * <pre>
 * {@link ChannelPipeline} p = ...;
 * p.addLast("streamer", <b>new {@link ChunkedWriteHandler}()</b>);
 * p.addLast("handler", new MyHandler());
 * </pre>
 * Once inserted, you can write a {@link ChunkedInput} so that the
 * {@link ChunkedWriteHandler} can pick it up and fetch the content of the
 * stream chunk by chunk and write the fetched chunk downstream:
 * <pre>
 * {@link Channel} ch = ...;
 * ch.write(new {@link ChunkedFile}(new File("video.mkv"));
 * </pre>
 *
 * <h3>Sending a stream which generates a chunk intermittently</h3>
 *
 * Some {@link ChunkedInput} generates a chunk on a certain event or timing.
 * Such {@link ChunkedInput} implementation often returns {@code null} on
 * {@link ChunkedInput#readChunk(ChannelHandlerContext)}, resulting in the indefinitely suspended
 * transfer.  To resume the transfer when a new chunk is available, you have to
 * call {@link #resumeTransfer()}.
 */
public class NettyChunkedWriteHandler extends ChannelDuplexHandler {
  private static final InternalLogger LOGGER =
      InternalLoggerFactory.getInstance(io.netty.handler.stream.ChunkedWriteHandler.class);

  /**
   * Using {@link ConcurrentLinkedQueue} below to allow {@link #doFlush(ChannelHandlerContext)} to be executed
   * in parallel since {@link #resumeTransfer()} or {@link #discard(Throwable)} could happen in any sequence.
   */
  private final Queue<PendingWrite> queue = new ConcurrentLinkedQueue<>();
  private volatile ChannelHandlerContext ctx;
  private PendingWrite currentWrite;

  public NettyChunkedWriteHandler() {
  }

  /**
   * @deprecated use {@link #NettyChunkedWriteHandler()}
   */
  @Deprecated
  public NettyChunkedWriteHandler(int maxPendingWrites) {
    if (maxPendingWrites <= 0) {
      throw new IllegalArgumentException("maxPendingWrites: " + maxPendingWrites + " (expected: > 0)");
    }
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    this.ctx = ctx;
  }

  /**
   * Continues to fetch the chunks from the input.
   */
  public void resumeTransfer() {
    final ChannelHandlerContext ctx = this.ctx;
    if (ctx == null) {
      return;
    }
    if (ctx.executor().inEventLoop()) {
      try {
        doFlush(ctx);
      } catch (Exception e) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("Unexpected exception while sending chunks.", e);
        }
      }
    } else {
      // let the transfer resume on the next event loop round
      ctx.executor().execute(new Runnable() {
        @Override
        public void run() {
          try {
            doFlush(ctx);
          } catch (Exception e) {
            if (LOGGER.isWarnEnabled()) {
              LOGGER.warn("Unexpected exception while sending chunks.", e);
            }
          }
        }
      });
    }
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    queue.add(new PendingWrite(msg, promise));
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    doFlush(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    doFlush(ctx);
    ctx.fireChannelInactive();
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
    if (ctx.channel().isWritable()) {
      // channel is writable again try to continue flushing
      doFlush(ctx);
    }
    ctx.fireChannelWritabilityChanged();
  }

  private void discard(Throwable cause) {
    for (;;) {
      PendingWrite currentWrite = this.currentWrite;

      if (this.currentWrite == null) {
        currentWrite = queue.poll();
      } else {
        this.currentWrite = null;
      }

      if (currentWrite == null) {
        break;
      }
      Object message = currentWrite.msg;
      if (message instanceof ChunkedInput) {
        ChunkedInput<?> in = (ChunkedInput<?>) message;
        try {
          if (!in.isEndOfInput()) {
            if (cause == null) {
              cause = new ClosedChannelException();
            }
            currentWrite.fail(cause);
          } else {
            currentWrite.success(in.length());
          }
          closeInput(in);
        } catch (Exception e) {
          currentWrite.fail(e);
          LOGGER.warn(ChunkedInput.class.getSimpleName() + ".isEndOfInput() failed", e);
          closeInput(in);
        }
      } else {
        if (cause == null) {
          cause = new ClosedChannelException();
        }
        currentWrite.fail(cause);
      }
    }
  }

  private void doFlush(final ChannelHandlerContext ctx) throws Exception {
    final Channel channel = ctx.channel();
    if (!channel.isActive()) {
      discard(null);
      return;
    }

    boolean requiresFlush = true;
    ByteBufAllocator allocator = ctx.alloc();
    while (channel.isWritable()) {
      if (currentWrite == null) {
        currentWrite = queue.poll();
      }

      if (currentWrite == null) {
        break;
      }

      if (currentWrite.promise.isDone()) {
        // This might happen e.g. in the case when a write operation
        // failed, but there're still unconsumed chunks left.
        // Most chunked input sources would stop generating chunks
        // and report end of input, but this doesn't work with any
        // source wrapped in HttpChunkedInput.
        // Note, that we're not trying to release the message/chunks
        // as this had to be done already by someone who resolved the
        // promise (using ChunkedInput.close method).
        // See https://github.com/netty/netty/issues/8700.
        this.currentWrite = null;
        continue;
      }

      final PendingWrite currentWrite = this.currentWrite;
      final Object pendingMessage = currentWrite.msg;

      if (pendingMessage instanceof ChunkedInput) {
        final ChunkedInput<?> chunks = (ChunkedInput<?>) pendingMessage;
        boolean endOfInput;
        boolean suspend;
        Object message = null;
        try {
          message = chunks.readChunk(allocator);
          endOfInput = chunks.isEndOfInput();

          if (message == null) {
            // No need to suspend when reached at the end.
            suspend = !endOfInput;
          } else {
            suspend = false;
          }
        } catch (final Throwable t) {
          this.currentWrite = null;

          if (message != null) {
            ReferenceCountUtil.release(message);
          }

          currentWrite.fail(t);
          closeInput(chunks);
          break;
        }

        if (suspend) {
          // ChunkedInput.nextChunk() returned null and it has
          // not reached at the end of input. Let's wait until
          // more chunks arrive. Nothing to write or notify.
          break;
        }

        if (message == null) {
          // If message is null write an empty ByteBuf.
          // See https://github.com/netty/netty/issues/1671
          message = Unpooled.EMPTY_BUFFER;
        }

        ChannelFuture f = ctx.write(message);
        if (endOfInput) {
          this.currentWrite = null;

          // Register a listener which will close the input once the write is complete.
          // This is needed because the Chunk may have some resource bound that can not
          // be closed before its not written.
          //
          // See https://github.com/netty/netty/issues/303
          f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {

              if (!future.isSuccess()) {
                currentWrite.fail(future.cause());
              } else {
                // read state of the input in local variables before closing it
                currentWrite.progress(chunks.progress(), chunks.length());
                currentWrite.success(chunks.length());
              }
              closeInput(chunks);
            }
          });
        } else if (channel.isWritable()) {
          f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              if (!future.isSuccess()) {
                closeInput(chunks);
                currentWrite.fail(future.cause());
              } else {
                currentWrite.progress(chunks.progress(), chunks.length());
              }
            }
          });
        } else {
          f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              if (!future.isSuccess()) {
                closeInput(chunks);
                currentWrite.fail(future.cause());
              } else {
                currentWrite.progress(chunks.progress(), chunks.length());
                if (channel.isWritable()) {
                  resumeTransfer();
                }
              }
            }
          });
        }
        // Flush each chunk to conserve memory
        ctx.flush();
        requiresFlush = false;
      } else {
        ctx.write(pendingMessage, currentWrite.promise);
        this.currentWrite = null;
        requiresFlush = true;
      }

      if (!channel.isActive()) {
        discard(new ClosedChannelException());
        break;
      }
    }

    if (requiresFlush) {
      ctx.flush();
    }
  }

  static void closeInput(ChunkedInput<?> chunks) {
    try {
      chunks.close();
    } catch (Throwable t) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn("Failed to close a chunked input.", t);
      }
    }
  }

  private static final class PendingWrite {
    final Object msg;
    final ChannelPromise promise;

    PendingWrite(Object msg, ChannelPromise promise) {
      this.msg = msg;
      this.promise = promise;
    }

    void fail(Throwable cause) {
      ReferenceCountUtil.release(msg);
      promise.tryFailure(cause);
    }

    void success(long total) {
      if (promise.isDone()) {
        // No need to notify the progress or fulfill the promise because it's done already.
        return;
      }

      if (promise instanceof ChannelProgressivePromise) {
        // Now we know what the total is.
        ((ChannelProgressivePromise) promise).tryProgress(total, total);
      }

      promise.trySuccess();
    }

    void progress(long progress, long total) {
      if (promise instanceof ChannelProgressivePromise) {
        ((ChannelProgressivePromise) promise).tryProgress(progress, total);
      }
    }
  }
}
