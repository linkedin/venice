package com.linkedin.venice.samza;

/**
 * A value object holding the Avro-serialized key and value bytes along with the resolved schema IDs
 * and logical timestamp needed to write a record to Venice. Obtain an instance via
 * {@link VeniceSystemProducer#prepareRecord(Object, Object)}.
 */
public class SerializedRecord {
  private final byte[] serializedKey;
  private final byte[] serializedValue; // null signals a delete
  private final int valueSchemaId;
  private final int derivedSchemaId; // -1 for a plain PUT
  private final long logicalTimestamp;

  public SerializedRecord(
      byte[] serializedKey,
      byte[] serializedValue,
      int valueSchemaId,
      int derivedSchemaId,
      long logicalTimestamp) {
    this.serializedKey = serializedKey;
    this.serializedValue = serializedValue;
    this.valueSchemaId = valueSchemaId;
    this.derivedSchemaId = derivedSchemaId;
    this.logicalTimestamp = logicalTimestamp;
  }

  public byte[] getSerializedKey() {
    return serializedKey;
  }

  /** Returns the serialized value bytes, or {@code null} if this record represents a delete. */
  public byte[] getSerializedValue() {
    return serializedValue;
  }

  public int getValueSchemaId() {
    return valueSchemaId;
  }

  public int getDerivedSchemaId() {
    return derivedSchemaId;
  }

  public long getLogicalTimestamp() {
    return logicalTimestamp;
  }
}
