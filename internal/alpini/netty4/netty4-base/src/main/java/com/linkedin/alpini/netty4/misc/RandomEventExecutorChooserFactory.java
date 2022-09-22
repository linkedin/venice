package com.linkedin.alpini.netty4.misc;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorChooserFactory;
import java.util.concurrent.ThreadLocalRandom;


/**
 * Created by acurtis on 12/12/16.
 */
public class RandomEventExecutorChooserFactory implements EventExecutorChooserFactory {
  public static final EventExecutorChooserFactory INSTANCE = new RandomEventExecutorChooserFactory();

  @Override
  public EventExecutorChooser newChooser(EventExecutor[] executors) {
    if (executors.length < 1) {
      throw new IllegalArgumentException();
    } else if (executors.length == 1) {
      return () -> executors[0];
    } else {
      return () -> executors[ThreadLocalRandom.current().nextInt(executors.length)];
    }
  }
}
