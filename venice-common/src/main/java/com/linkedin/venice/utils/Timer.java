package com.linkedin.venice.utils;

import java.util.function.Consumer;


public interface Timer extends AutoCloseable {
  static Timer run(Consumer<Double> timeConsumer) {
    return new TimerImpl(timeConsumer);
  }

  // Override the close method to change the throws signature.
  @Override
  void close();
}

class TimerImpl implements Timer {
  private final Consumer<Double> timeInMsConsumer;
  private final long startTimeInNs = System.nanoTime();

  public TimerImpl(Consumer<Double> timeInMsConsumer) {
    this.timeInMsConsumer = timeInMsConsumer;
  }

  @Override
  public void close() {
    timeInMsConsumer.accept(LatencyUtils.getLatencyInMS(startTimeInNs));
  }
}
