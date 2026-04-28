package com.linkedin.venice.spark.datawriter.task;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
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
  private static final Logger LOGGER = LogManager.getLogger(CountingIterator.class);

  private final Iterator<Row> delegate;
  private final LongAccumulator recordCounter;
  private final LongAccumulator byteCounter;
  private final int keyIdx;
  private final int valueIdx;
  private final int rmdIdx;

  /**
   * @param schema optional schema to resolve column indices once at construction time.
   *               If null, byte size tracking returns 0 (record counting still works).
   * @param keyColumnName column name for key (e.g., "key")
   * @param valueColumnName column name for value (e.g., "value")
   * @param rmdColumnName column name for RMD (e.g., "rmd" or "__replication_metadata_payload__").
   *                      If null, RMD bytes are not counted.
   */
  public CountingIterator(
      Iterator<Row> delegate,
      LongAccumulator recordCounter,
      LongAccumulator byteCounter,
      StructType schema,
      String keyColumnName,
      String valueColumnName,
      String rmdColumnName) {
    this.delegate = delegate;
    this.recordCounter = recordCounter;
    this.byteCounter = byteCounter;
    this.keyIdx = resolveFieldIndex(schema, keyColumnName);
    this.valueIdx = resolveFieldIndex(schema, valueColumnName);
    this.rmdIdx = resolveFieldIndex(schema, rmdColumnName);
  }

  private static int resolveFieldIndex(StructType schema, String fieldName) {
    if (schema == null || fieldName == null) {
      return -1;
    }
    try {
      return schema.fieldIndex(fieldName);
    } catch (IllegalArgumentException e) {
      return -1;
    }
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
   *
   * <p>Column indices are resolved once at construction time from the schema. Returns 0
   * if the key or value column is not present in the schema.
   */
  long computeByteSize(Row row) {
    if (keyIdx < 0 || valueIdx < 0) {
      return 0;
    }

    long size = 0;
    size += getBinaryFieldLength(row, keyIdx);
    size += getBinaryFieldLength(row, valueIdx);
    if (rmdIdx >= 0) {
      size += getBinaryFieldLength(row, rmdIdx);
    }
    return size;
  }

  /**
   * Static helper for computing byte size from a Row using pre-resolved column indices.
   * Used in groupByKey.flatMapGroups lambdas where a CountingIterator instance is not available.
   *
   * @param indices variable-length list of column indices to sum. Negative indices are skipped.
   */
  public static long computeByteSizeByIndices(Row row, int... indices) {
    long size = 0;
    for (int idx: indices) {
      if (idx >= 0) {
        size += getBinaryFieldLength(row, idx);
      }
    }
    return size;
  }

  private static long getBinaryFieldLength(Row row, int index) {
    try {
      if (index >= row.size() || row.isNullAt(index)) {
        return 0;
      }
      Object val = row.get(index);
      if (val instanceof byte[]) {
        return ((byte[]) val).length;
      }
      // Fallback: try getAs which may handle type conversion
      byte[] bytes = row.getAs(index);
      return bytes != null ? bytes.length : 0;
    } catch (Exception e) {
      LOGGER
          .error("getBinaryFieldLength failed for index {} on row class {}", index, row.getClass().getSimpleName(), e);
      return 0;
    }
  }

}
