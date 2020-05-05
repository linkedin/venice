package com.linkedin.davinci.client;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.storage.chunking.AbstractAvroChunkingAdapter;
import com.linkedin.venice.storage.chunking.SpecificRecordChunkingAdapter;
import com.linkedin.venice.utils.VeniceProperties;

import org.apache.avro.specific.SpecificRecord;

public class AvroSpecificDaVinciClientImpl<K, V extends SpecificRecord> extends AvroGenericDaVinciClientImpl<K, V> {
  private final SpecificRecordChunkingAdapter<V> chunkingAdapter;

  public AvroSpecificDaVinciClientImpl(
      DaVinciConfig daVinciConfig,
      ClientConfig clientConfig,
      VeniceProperties backendConfig) {
    super(daVinciConfig, clientConfig, backendConfig);
    Class<V> valueClass = clientConfig.getSpecificValueClass();

    if (clientConfig.isUseFastAvro()) {
      FastSerializerDeserializerFactory.verifyWhetherFastSpecificDeserializerWorks(valueClass);
    }

    this.chunkingAdapter = new SpecificRecordChunkingAdapter<>(valueClass);
  }

  @Override
  protected AbstractAvroChunkingAdapter<V> getChunkingAdapter() {
    return chunkingAdapter;
  }
}
