package com.linkedin.venice.serialization;

import com.linkedin.venice.serializer.RecordDeserializer;
import java.nio.ByteBuffer;


public class RawBytesStoreDeserializerCache implements StoreDeserializerCache<ByteBuffer> {
  private static final StoreDeserializerCache INSTANCE = new RawBytesStoreDeserializerCache();

  @Override
  public RecordDeserializer<ByteBuffer> getDeserializer(int writerSchemaId, int readerSchemaId) {
    return IdentityRecordDeserializer.getInstance();
  }

  @Override
  public RecordDeserializer<ByteBuffer> getDeserializer(int writerSchemaId) {
    throw new UnsupportedOperationException(
        "getDeserializer by only writeSchemaId is not supported by " + this.getClass().getSimpleName());
  }

  public static StoreDeserializerCache<ByteBuffer> getInstance() {
    return INSTANCE;
  }
}
