package com.linkedin.venice.serialization;

import com.linkedin.venice.serializer.RecordDeserializer;


public interface StoreDeserializerCache<T> {
  RecordDeserializer<T> getDeserializer(int writerSchemaId, int readerSchemaId);
}
