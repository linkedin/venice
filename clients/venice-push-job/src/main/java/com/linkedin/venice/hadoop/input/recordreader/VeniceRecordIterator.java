package com.linkedin.venice.hadoop.input.recordreader;

import java.io.Closeable;


public interface VeniceRecordIterator extends Closeable {
  /**
   * Proceed to next record, returns false if there is no more records.
   */
  boolean next();

  /**
   * Return the current key. This method should return same value until `next` is called.
   */
  byte[] getCurrentKey();

  /**
   * Return the current value. This method should return same value until `next` is called.
   */
  byte[] getCurrentValue();
}
