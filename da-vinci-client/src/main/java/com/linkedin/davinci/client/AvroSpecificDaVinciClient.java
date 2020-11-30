package com.linkedin.davinci.client;

import com.linkedin.davinci.storage.chunking.AbstractAvroChunkingAdapter;
import com.linkedin.davinci.storage.chunking.SpecificRecordChunkingAdapter;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.utils.VeniceProperties;

import org.apache.avro.specific.SpecificRecord;

public class AvroSpecificDaVinciClient<K, V extends SpecificRecord> extends AvroGenericDaVinciClient<K, V> {
  private final SpecificRecordChunkingAdapter<V> chunkingAdapter;

  public AvroSpecificDaVinciClient(
      DaVinciConfig daVinciConfig,
      ClientConfig clientConfig,
      VeniceProperties backendConfig,
      String instanceName) {
    super(daVinciConfig, clientConfig, backendConfig, instanceName);

    Class<V> valueClass = clientConfig.getSpecificValueClass();
    FastSerializerDeserializerFactory.verifyWhetherFastSpecificDeserializerWorks(valueClass);
    this.chunkingAdapter = new SpecificRecordChunkingAdapter<>(valueClass);
  }

  @Override
  protected AbstractAvroChunkingAdapter<V> getChunkingAdapter() {
    return chunkingAdapter;
  }
}
