package com.linkedin.venice.hadoop.task.datawriter;

/**
 * Immutable carrier passed to {@link ExternalStorageWriter#batchPut(java.util.List)}. Holds one record's
 * serialized key and a value blob whose layout matches Venice's on-disk format in RocksDB:
 *
 * <pre>
 *   value = [4-byte big-endian value schema id] [compressed Avro value bytes]
 * </pre>
 *
 * <p>Implementations can either store the value as-is (an external reader uses the same logical reassembly
 * that Venice clients do — read first 4 bytes for the schema id, decompress the rest with the schema) or
 * split out the schema id by parsing the prefix locally. Either way, a reader pulling from the external
 * sink will see the same logical bytes a Venice client would observe after RocksDB's chunked-value
 * reassembly: the chunking that Venice does for Kafka transport is invisible at this seam.
 *
 * <p>Byte arrays are passed through by reference for efficiency and must not be mutated by the implementation.
 */
public final class ExternalStorageRecord {
  /** Length of the 4-byte big-endian schema id prefix on every value. */
  public static final int SCHEMA_ID_PREFIX_LENGTH = Integer.BYTES;

  private final byte[] key;
  private final byte[] value;

  public ExternalStorageRecord(byte[] key, byte[] value) {
    this.key = key;
    this.value = value;
  }

  public byte[] getKey() {
    return key;
  }

  /**
   * Returns the value with the 4-byte big-endian schema id prefix in front of the Avro payload. The schema id
   * can be parsed from the first {@link #SCHEMA_ID_PREFIX_LENGTH} bytes.
   */
  public byte[] getValue() {
    return value;
  }
}
