package com.linkedin.davinci.client;

import com.linkedin.davinci.storage.chunking.AbstractAvroChunkingAdapter;
import com.linkedin.davinci.storage.chunking.SpecificRecordChunkingAdapter;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.specific.SpecificRecord;


public class AvroSpecificDaVinciClient<K, V extends SpecificRecord> extends AvroGenericDaVinciClient<K, V> {
  private final SpecificRecordChunkingAdapter<V> chunkingAdapter;

  public AvroSpecificDaVinciClient(
      DaVinciConfig daVinciConfig,
      ClientConfig clientConfig,
      VeniceProperties backendConfig,
      Optional<Set<String>> managedClients,
      ICProvider icProvider) {
    super(daVinciConfig, clientConfig, backendConfig, managedClients, icProvider);

    Class<V> valueClass = clientConfig.getSpecificValueClass();
    FastSerializerDeserializerFactory.verifyWhetherFastSpecificDeserializerWorks(valueClass);
    this.chunkingAdapter = new SpecificRecordChunkingAdapter<>(valueClass);
  }

  @Override
  protected AbstractAvroChunkingAdapter<V> getAvroChunkingAdapter() {
    return chunkingAdapter;
  }
}
