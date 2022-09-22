package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.concurrency.Lazy;
import com.linkedin.alpini.base.concurrency.NamedThreadFactory;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutor;
import java.nio.channels.ClosedChannelException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by acurtis on 5/4/17.
 */
@ChannelHandler.Sharable
public class BackgroundChannelHandler extends ChannelInitializer<Channel> {
  private static final Logger LOG = LogManager.getLogger(BackgroundChannelHandler.class);

  private static final Thread.UncaughtExceptionHandler UNCAUGHT_EXCEPTION_HANDLER =
      (t, e) -> LOG.warn("Uncaught Exception in thread {}", t.getName(), e);

  private static final ThreadFactory THREAD_FACTORY =
      new NamedThreadFactory(Executors.defaultThreadFactory(), "BackgroundChannelHandler") {
        @Override
        public Thread newThread(@Nonnull Runnable runnable) {
          Thread thread = super.newThread(runnable);
          thread.setUncaughtExceptionHandler(UNCAUGHT_EXCEPTION_HANDLER);
          return thread;
        }
      };

  private static final ThreadLocal<Executor> WORKER_EXECUTOR =
      ThreadLocal.withInitial(() -> Executors.newSingleThreadExecutor(THREAD_FACTORY));

  private final Supplier<ChannelHandler[]> _channelHandlers;

  public BackgroundChannelHandler(ChannelHandler... channelHandler) {
    this(makeSupplier(channelHandler.clone()));
  }

  public BackgroundChannelHandler(@Nonnull Supplier<ChannelHandler[]> channelHandlerSupplier) {
    _channelHandlers = Objects.requireNonNull(channelHandlerSupplier, "channelHandlerSupplier");
  }

  private static Supplier<ChannelHandler[]> makeSupplier(ChannelHandler[] channelHandlers) {
    return () -> channelHandlers;
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    ch.pipeline().replace(this, "background-handler", new Handler());
  }

  class Handler extends ChannelDuplexHandler {
    private EmbeddedChannel _embeddedChannel;
    private Supplier<Executor> _workerExecutor = Lazy.of(WORKER_EXECUTOR::get);

    private Executor workerExecutor() {
      return _workerExecutor.get();
    }

    private EmbeddedChannel checkOpen() throws ClosedChannelException {
      if (_embeddedChannel == null || !_embeddedChannel.isOpen()) {
        throw new ClosedChannelException();
      }
      return _embeddedChannel;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      _embeddedChannel = new EmbeddedChannel(
          ctx.channel().id(),
          ctx.channel().metadata().hasDisconnect(),
          ctx.channel().config(),
          _channelHandlers.get()) {
        @Override
        protected void doWrite(ChannelOutboundBuffer in) throws Exception {
          EventExecutor executor = ctx.executor();
          Runnable task = () -> {
            for (int remain = in.size(); remain > 1; remain--) {
              try {
                ctx.write(ReferenceCountUtil.retain(in.current()));
                in.remove();
              } catch (Throwable ex) {
                in.remove(ex);
              }
            }
            if (!in.isEmpty()) {
              try {
                ctx.writeAndFlush(ReferenceCountUtil.retain(in.current())).sync();
                in.remove();
              } catch (Throwable ex) {
                in.remove(ex);
              }
            }
          };
          if (executor.inEventLoop()) {
            task.run();
          } else {
            CompletableFuture.runAsync(task, executor).join();
          }
        }

        @Override
        protected void handleInboundMessage(Object msg) {
          ctx.fireChannelRead(msg);
        }
      };
      super.handlerAdded(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      EmbeddedChannel embeddedChannel = checkOpen();
      if (StreamSupport.stream(embeddedChannel.pipeline().spliterator(), false)
          .map(Map.Entry::getValue)
          .filter(handler -> handler instanceof ChannelInboundHandler)
          .findFirst()
          .isPresent()) {
        CompletableFuture.completedFuture(ReferenceCountUtil.retain(msg))
            .thenApplyAsync(embeddedChannel::writeOneInbound, workerExecutor())
            .exceptionally(ex -> ctx.fireExceptionCaught(ex).newSucceededFuture());
      } else {
        super.channelRead(ctx, msg);
      }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
      EmbeddedChannel embeddedChannel = checkOpen();
      if (StreamSupport.stream(embeddedChannel.pipeline().spliterator(), false)
          .map(Map.Entry::getValue)
          .filter(handler -> handler instanceof ChannelInboundHandler)
          .findFirst()
          .isPresent()) {
        CompletableFuture.runAsync(embeddedChannel::flushInbound, workerExecutor())
            .thenRun(ctx::fireChannelReadComplete)
            .exceptionally(ex -> {
              ctx.fireExceptionCaught(ex);
              return null;
            });
      } else {
        super.channelReadComplete(ctx);
      }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      EmbeddedChannel embeddedChannel = checkOpen();
      if (StreamSupport.stream(embeddedChannel.pipeline().spliterator(), false)
          .map(Map.Entry::getValue)
          .filter(handler -> handler instanceof ChannelInboundHandler)
          .findFirst()
          .isPresent()) {
        CompletableFuture.runAsync(() -> embeddedChannel.pipeline().fireUserEventTriggered(evt), workerExecutor())
            .thenRun(() -> ctx.fireUserEventTriggered(evt))
            .exceptionally(ex -> {
              ctx.fireExceptionCaught(ex);
              return null;
            });
      } else {
        super.userEventTriggered(ctx, evt);
      }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
      EmbeddedChannel embeddedChannel = checkOpen();
      CompletableFuture.supplyAsync(embeddedChannel::close, workerExecutor())
          .thenApply(f -> f.addListener(closeFuture -> {
            if (!f.isSuccess()) {
              ctx.fireExceptionCaught(f.cause());
            }
            ctx.close(promise);
          }))
          .exceptionally(ex -> {
            ctx.fireExceptionCaught(ex);
            ctx.close(promise);
            return null;
          });
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      EmbeddedChannel embeddedChannel = checkOpen();
      if (StreamSupport.stream(embeddedChannel.pipeline().spliterator(), false)
          .map(Map.Entry::getValue)
          .filter(handler -> handler instanceof ChannelOutboundHandler)
          .findFirst()
          .isPresent()) {
        CompletableFuture.completedFuture(ReferenceCountUtil.retain(msg))
            .thenApplyAsync(embeddedChannel::writeOneOutbound, workerExecutor())
            .exceptionally(ctx::newFailedFuture)
            .thenAccept(embeddedFuture -> {
              embeddedFuture.addListener(future -> {
                if (future.isSuccess()) {
                  promise.setSuccess();
                } else {
                  promise.setFailure(future.cause());
                }
              });
              embeddedChannel.flushOutbound();
            });
      } else {
        super.write(ctx, msg, promise);
      }
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
      EmbeddedChannel embeddedChannel = checkOpen();
      if (StreamSupport.stream(embeddedChannel.pipeline().spliterator(), false)
          .map(Map.Entry::getValue)
          .filter(handler -> handler instanceof ChannelOutboundHandler)
          .findFirst()
          .isPresent()) {
        CompletableFuture.runAsync(embeddedChannel::flush, workerExecutor())
            .thenRun(embeddedChannel::flushOutbound)
            .exceptionally(ex -> {
              ctx.fireExceptionCaught(ex);
              return null;
            })
            .thenRun(ctx::flush);
      } else {
        super.flush(ctx);
      }
    }
  }
}
