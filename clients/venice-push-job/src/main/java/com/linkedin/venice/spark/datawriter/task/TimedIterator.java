package com.linkedin.venice.spark.datawriter.task;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;


/**
 * Wraps an {@code Iterator<Row>} and records wall-clock timing when the iterator is exhausted.
 * Timing is recorded once, on the first call to {@link #hasNext()} that returns false.
 */
public class TimedIterator implements Iterator<Row> {
  private final Iterator<Row> delegate;
  private final LongAccumulator timeNsAccumulator;
  private final long startNs;
  private boolean timingRecorded = false;

  public TimedIterator(Iterator<Row> delegate, LongAccumulator timeNsAccumulator) {
    this.delegate = delegate;
    this.timeNsAccumulator = timeNsAccumulator;
    this.startNs = System.nanoTime();
  }

  @Override
  public boolean hasNext() {
    boolean has = delegate.hasNext();
    if (!has && !timingRecorded) {
      timingRecorded = true;
      timeNsAccumulator.add(System.nanoTime() - startNs);
    }
    return has;
  }

  @Override
  public Row next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return delegate.next();
  }
}
