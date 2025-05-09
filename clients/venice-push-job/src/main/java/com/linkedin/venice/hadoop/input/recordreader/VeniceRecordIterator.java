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

  /**
   * Return the timestamp associated with the current record. This method will either return the timestamp of the record
   * or -1L if a timestamp was not specified with the given record
   * @return
   */
  long getTimeStamp();
}
