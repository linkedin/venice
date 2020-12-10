package com.linkedin.davinci.client;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.utils.VeniceProperties;

import com.linkedin.davinci.storage.chunking.AbstractAvroChunkingAdapter;
import com.linkedin.davinci.storage.chunking.SpecificRecordChunkingAdapter;

import org.apache.avro.specific.SpecificRecord;

import java.util.Optional;
import java.util.Set;

public class AvroSpecificDaVinciClient<K, V extends SpecificRecord> extends AvroGenericDaVinciClient<K, V> {
  private final SpecificRecordChunkingAdapter<V> chunkingAdapter;

  public AvroSpecificDaVinciClient(
      DaVinciConfig daVinciConfig,
      ClientConfig clientConfig,
      VeniceProperties backendConfig,
      Optional<Set<String>> managedClients) {
    super(daVinciConfig, clientConfig, backendConfig, managedClients);

    Class<V> valueClass = clientConfig.getSpecificValueClass();
    FastSerializerDeserializerFactory.verifyWhetherFastSpecificDeserializerWorks(valueClass);
    this.chunkingAdapter = new SpecificRecordChunkingAdapter<>(valueClass);
  }

  @Override
  protected AbstractAvroChunkingAdapter<V> getChunkingAdapter() {
    return chunkingAdapter;
  }
}
