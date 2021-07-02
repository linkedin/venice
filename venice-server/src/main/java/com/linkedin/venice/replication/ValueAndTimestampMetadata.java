package com.linkedin.venice.replication;

import org.apache.avro.generic.GenericRecord;


/**
 * Wrapper class to hold a pair of {@link GenericRecord}, including a value and its corresponding
 * timestamp metadata.
 */
public class ValueAndTimestampMetadata {
  private GenericRecord value;
  private GenericRecord timestampMetadata;

  public ValueAndTimestampMetadata(GenericRecord value, GenericRecord timestampMetadata) {
    this.value = value;
    this.timestampMetadata = timestampMetadata;
  }

  public GenericRecord getValue() {
    return value;
  }

  public void setValue(GenericRecord value) {
    this.value = value;
  }

  public GenericRecord getTimestampMetadata() {
    return timestampMetadata;
  }

  public void setTimestampMetadata(GenericRecord timestampMetadata) {
    this.timestampMetadata = timestampMetadata;
  }
}
