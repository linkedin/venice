package com.linkedin.davinci.client;

import com.linkedin.venice.views.ChangeCaptureView;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


public interface SeekableDaVinciClient<K, V> extends DaVinciClient<K, V> {
  CompletableFuture<Void> seekToTimestamps(Map<Integer, Long> timestamps);

  CompletableFuture<Void> seekToTimestamp(Long timestamp);

  CompletableFuture<Void> seekToCheckpoint(Set<ChangeCaptureView> checkpoints);

  CompletableFuture<Void> seekToTail();

  CompletableFuture<Void> seekToTail(Set<Integer> partitions);
}
