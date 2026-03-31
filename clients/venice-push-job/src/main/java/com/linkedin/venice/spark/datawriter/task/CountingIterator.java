package com.linkedin.venice.spark.datawriter.task;

import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RMD_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.VALUE_COLUMN_NAME;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;


/**
 * Wraps an {@code Iterator<Row>} to count records and accumulate byte sizes as records pass through.
 * Used to instrument VPJ Spark pipeline stages with per-stage I/O tracking.
 *
 * <p>Byte size per record is computed as: {@code key.length + (value != null ? value.length : 0)
 * + (rmd != null ? rmd.length : 0)}. This matches the three BinaryType columns in
 * {@link com.linkedin.venice.spark.SparkConstants#DEFAULT_SCHEMA}.
 *
 * <p>Overhead is minimal: one accumulator increment + one byte size computation per record.
 */
public class CountingIterator implements Iterator<Row> {
  private final Iterator<Row> delegate;
  private final LongAccumulator recordCounter;
  private final LongAccumulator byteCounter;

  public CountingIterator(Iterator<Row> delegate, LongAccumulator recordCounter, LongAccumulator byteCounter) {
    this.delegate = delegate;
    this.recordCounter = recordCounter;
    this.byteCounter = byteCounter;
  }

  @Override
  public boolean hasNext() {
    return delegate.hasNext();
  }

  @Override
  public Row next() {
    if (!delegate.hasNext()) {
      throw new NoSuchElementException();
    }
    Row row = delegate.next();
    recordCounter.add(1);
    byteCounter.add(computeByteSize(row));
    return row;
  }

  /**
   * Compute the byte size of a row by summing key, value, and rmd column lengths.
   * Handles null values gracefully (value and rmd are nullable in Venice schemas).
   * Falls back to field index access if column name lookup fails (for schemas that
   * don't use the standard column names).
   */
  static long computeByteSize(Row row) {
    long size = 0;
    try {
      byte[] key = row.getAs(KEY_COLUMN_NAME);
      if (key != null) {
        size += key.length;
      }
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      // Schema doesn't have named key column; try index 0
      Object key = row.get(0);
      if (key instanceof byte[]) {
        size += ((byte[]) key).length;
      }
    }

    try {
      byte[] value = row.getAs(VALUE_COLUMN_NAME);
      if (value != null) {
        size += value.length;
      }
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      if (row.length() > 1) {
        Object value = row.get(1);
        if (value instanceof byte[]) {
          size += ((byte[]) value).length;
        }
      }
    }

    try {
      byte[] rmd = row.getAs(RMD_COLUMN_NAME);
      if (rmd != null) {
        size += rmd.length;
      }
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      if (row.length() > 2) {
        Object rmd = row.get(2);
        if (rmd instanceof byte[]) {
          size += ((byte[]) rmd).length;
        }
      }
    }

    return size;
  }
}
