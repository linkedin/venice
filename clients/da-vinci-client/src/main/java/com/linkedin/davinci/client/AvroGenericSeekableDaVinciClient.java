package com.linkedin.davinci.client;

import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.storage.chunking.GenericChunkingAdapter;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;


public class AvroGenericSeekableDaVinciClient<K, V> extends AvroGenericDaVinciClient<K, V>
    implements SeekableDaVinciClient<K, V> {
  public AvroGenericSeekableDaVinciClient(
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
        GenericChunkingAdapter.INSTANCE,
        () -> {},
        readChunkExecutorForLargeRequest,
        null);
  }

  @Override
  public CompletableFuture<Void> seekToTimestamps(Map<Integer, Long> timestamps) {
    return null;
  }

  @Override
  public CompletableFuture<Void> seekToTimestamp(Long timestamp) {
    return null;
  }

  @Override
  public CompletableFuture<Void> seekToBeginningOfPush(Set<Integer> partitions) {
    return super.subscribe(partitions);
  }

  @Override
  public CompletableFuture<Void> seekToCheckpoint(Set<VeniceChangeCoordinate> checkpoints) {
    return null;
  }

  @Override
  public CompletableFuture<Void> seekToTail() {
    return null;
  }

  @Override
  public CompletableFuture<Void> seekToTail(Set<Integer> partitions) {
    return null;
  }
}
