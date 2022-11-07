package com.linkedin.venice.schema.rmd;

/**
 * Type of the replication metadata timestamp. Note that replication metadata could contain more than just "timestamps".
 */
public enum RmdTimestampType {
  VALUE_LEVEL_TIMESTAMP(0), // Value level timestamp.
  PER_FIELD_TIMESTAMP(1); // If a value is an Avro Record and it has per-record-field timestamp(s).

  private final int val;

  RmdTimestampType(int val) {
    this.val = val;
  }
}
