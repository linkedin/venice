package com.linkedin.alpini.netty4.misc;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import java.util.function.Consumer;
import javax.annotation.Nonnull;


/**
 * The purpose of this class is to execute tasks in series without overlap, even if the
 * {@link #executeTask(Consumer, ChannelFutureListener)} method is called before the completion
 * if the previous tasks.
 *
 * The next task is not started until after the provided {@link ChannelFutureListener} returns.
 *
 * If the task fails to complete by completing the {@link ChannelPromise}, the serializer becomes blocked.
 *
 * It may be possible to add a PhantomReference to detect when the {@linkplain ChannelPromise} completion
 * has been lost and to enact some kind of recovery.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class ChannelTaskSerializer {
  private final ChannelHandlerContext _ctx;

  private ChannelFuture _serializer;

  public ChannelTaskSerializer(@Nonnull ChannelHandlerContext ctx) {
    _ctx = ctx;
    _serializer = ctx.newSucceededFuture();
  }

  public void executeTask(@Nonnull Consumer<ChannelPromise> task, @Nonnull ChannelFutureListener listener) {
    EventExecutor executor = _ctx.executor();
    if (executor.inEventLoop()) {
      ChannelPromise newSerializer = _ctx.newPromise();
      ChannelFuture oldSerializer = _serializer;
      _serializer = newSerializer;
      oldSerializer.addListener(f -> {
        ChannelPromise completion = _ctx.newPromise();
        try {
          completion.setUncancellable();
          task.accept(completion);
        } catch (Throwable ex) {
          completion.setFailure(ex);
        }
        // PhantomReference to detect lost completion objects?
        completion.addListener((ChannelFutureListener) c -> {
          try {
            listener.operationComplete(c);
          } finally {
            newSerializer.setSuccess();
          }
        });
      });
    } else {
      executor.execute(() -> executeTask(task, listener));
    }
  }
}
