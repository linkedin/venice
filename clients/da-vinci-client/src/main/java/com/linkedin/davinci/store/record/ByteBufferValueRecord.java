package com.linkedin.davinci.store.record;

/**
 * This class encapsulates a value from venice storage accompanied by the schema
 * id that was used to serialize the value.
 *
 * TODO: This class should probably be superseded by {@link ValueRecord}. Unfortunately,
 * MANY interfaces in the ingestion path rely on the Bytebuffer interface, where ValueRecord relies on ByteBuf. Until
 * we rectify that, this is our stand in.
 */
public final class ByteBufferValueRecord<T> {
  private final T value;
  private final int writerSchemaId;

  /**
   */
  public ByteBufferValueRecord(T value, int writerSchemaId) {
    this.value = value;
    this.writerSchemaId = writerSchemaId;
  }

  public T value() {
    return value;
  }

  public int writerSchemaId() {
    return writerSchemaId;
  }

  @Override
  public String toString() {
    return "ByteBufferValueRecord[" + "value=" + value + ", " + "writerSchemaId=" + writerSchemaId + ']';
  }
}
