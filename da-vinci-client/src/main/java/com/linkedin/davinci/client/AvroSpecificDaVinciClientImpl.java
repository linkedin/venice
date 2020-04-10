package com.linkedin.davinci.client;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.storage.chunking.AbstractAvroChunkingAdapter;
import com.linkedin.venice.storage.chunking.SpecificRecordChunkingAdapter;
import org.apache.avro.specific.SpecificRecord;

public class AvroSpecificDaVinciClientImpl<K, V extends SpecificRecord> extends AvroGenericDaVinciClientImpl<K, V> {
  private final SpecificRecordChunkingAdapter<V> chunkingAdapter;

  public AvroSpecificDaVinciClientImpl(DaVinciConfig daVinciConfig, ClientConfig clientConfig) {
    super(daVinciConfig, clientConfig);
    Class<V> valueClass = clientConfig.getSpecificValueClass();

    if (useFastAvro) {
      FastSerializerDeserializerFactory.verifyWhetherFastSpecificDeserializerWorks(valueClass);
    }

    this.chunkingAdapter = new SpecificRecordChunkingAdapter<>(valueClass);
  }

  @Override
  protected AbstractAvroChunkingAdapter<V> getChunkingAdapter() {
    return chunkingAdapter;
  }
}
