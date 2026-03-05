package com.linkedin.davinci.client;

import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.storage.chunking.SpecificRecordChunkingAdapter;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.avro.specific.SpecificRecord;


public class AvroSpecificSeekableDaVinciClient<K, V extends SpecificRecord> extends AvroGenericDaVinciClient<K, V>
    implements SeekableDaVinciClient<K, V> {
  public AvroSpecificSeekableDaVinciClient(
      DaVinciConfig daVinciConfig,
      ClientConfig clientConfig,
      VeniceProperties backendConfig,
      Optional<Set<String>> managedClients,
      ICProvider icProvider,
      Executor readChunkExecutorForLargeRequest) {
    super(
        daVinciConfig,
        clientConfig,
        backendConfig,
        managedClients,
        icProvider,
        new SpecificRecordChunkingAdapter<>(),
        () -> {
          Class<V> valueClass = clientConfig.getSpecificValueClass();
          FastSerializerDeserializerFactory.verifyWhetherFastSpecificDeserializerWorks(valueClass);
        },
        readChunkExecutorForLargeRequest,
        null);
  }

  @Override
  public CompletableFuture<Void> seekToTimestamps(Map<Integer, Long> timestamps) {
    return super.seekToTimestamps(timestamps);
  }

  @Override
  public CompletableFuture<Void> seekToTimestamp(Long timestamp) {
    return super.seekToTimestamps(timestamp);
  }

  @Override
  public CompletableFuture<Void> seekToBeginningOfPush(Set<Integer> partitions) {
    return super.seekToBeginningOfPush(partitions);
  }

  @Override
  public CompletableFuture<Void> seekToCheckpoint(Set<VeniceChangeCoordinate> checkpoints) {
    return super.seekToCheckpoint(checkpoints);
  }

  @Override
  public CompletableFuture<Void> seekToTail() {
    throw new VeniceClientException("seekToTail is not supported yet");
  }

  @Override
  public CompletableFuture<Void> seekToTail(Set<Integer> partitions) {
    throw new VeniceClientException("seekToTail is not supported yet");
  }
}
