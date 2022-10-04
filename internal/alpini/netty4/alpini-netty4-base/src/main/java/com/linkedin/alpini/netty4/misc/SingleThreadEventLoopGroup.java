package com.linkedin.alpini.netty4.misc;

import io.netty.channel.EventLoop;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.util.concurrent.EventExecutorChooserFactory;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;


/**
 * Created by acurtis on 3/30/17.
 */
public class SingleThreadEventLoopGroup extends MultithreadEventLoopGroup {
  public SingleThreadEventLoopGroup(@Nonnull EventLoop eventLoop) {
    super(1, eventLoop, EVENT_EXECUTOR_CHOOSER_FACTORY);
  }

  @Override
  protected EventLoop newChild(Executor executor, Object... args) throws Exception {
    return (EventLoop) executor;
  }

  private static final EventExecutorChooserFactory EVENT_EXECUTOR_CHOOSER_FACTORY = executors -> () -> executors[0];
}
