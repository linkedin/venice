package com.linkedin.davinci.client;

import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


public interface SeekableDaVinciClient<K, V> extends DaVinciClient<K, V> {
  CompletableFuture<Void> seekToTimestamps(Map<Integer, Long> timestamps);

  CompletableFuture<Void> seekToTimestamp(Long timestamp);

  CompletableFuture<Void> seekToBeginningOfPush(Set<Integer> partitions);

  CompletableFuture<Void> seekToCheckpoint(Set<VeniceChangeCoordinate> checkpoints);

  CompletableFuture<Void> seekToTail();

  CompletableFuture<Void> seekToTail(Set<Integer> partitions);
}
