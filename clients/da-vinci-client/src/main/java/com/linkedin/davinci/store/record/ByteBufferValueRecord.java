package com.linkedin.davinci.store.record;

/**
 * This class encapsulates a value from venice storage accompanied by the schema
 * id that was used to serialize the value.
 *
 * TODO: This class should probably be superseded by {@link com.linkedin.davinci.store.record.ValueRecord}. Unfortunately,
 * MANY interfaces in the ingestion path rely on the Bytebuffer interface, where ValueRecord relies on ByteBuf. Until
 * we rectify that, this is our stand in.
 */
public class ByteBufferValueRecord<T> {
  T value;
  int writerSchemaId = -1;

  public ByteBufferValueRecord(T value, int writerSchemaId) {
    this.value = value;
    this.writerSchemaId = writerSchemaId;
  }

  public T getValue() {
    return value;
  }

  public int getWriterSchemaId() {
    return writerSchemaId;
  }
}
