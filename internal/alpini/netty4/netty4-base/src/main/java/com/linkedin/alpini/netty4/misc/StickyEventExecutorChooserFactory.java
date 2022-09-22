package com.linkedin.alpini.netty4.misc;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorChooserFactory;
import java.util.Arrays;
import java.util.Optional;


/**
 * Created by acurtis on 12/12/16.
 */
public class StickyEventExecutorChooserFactory implements EventExecutorChooserFactory {
  public static final EventExecutorChooserFactory INSTANCE = new StickyEventExecutorChooserFactory();

  @Override
  public EventExecutorChooser newChooser(EventExecutor[] executors) {
    EventExecutorChooser randomChooser = RandomEventExecutorChooserFactory.INSTANCE.newChooser(executors);
    if (executors.length <= 1) {
      return randomChooser;
    } else {
      ThreadLocal<Optional<EventExecutor>> cachedExecutor = ThreadLocal.withInitial(() -> {
        Thread currentThread = Thread.currentThread();
        return Arrays.stream(executors).filter(e -> e.inEventLoop(currentThread)).findFirst();
      });

      return () -> cachedExecutor.get().orElseGet(randomChooser::next);
    }
  }
}
