package com.linkedin.venice.client.store.deserialization;

import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.stats.Reporter;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.serializer.RecordDeserializer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;


/**
 * This API controls the behavior of the user payload deserialization phase of the
 * batch get response handling.
 */
public abstract class BatchGetDeserializer<K, V> {
  protected final Executor deserializationExecutor;
  protected final ClientConfig clientConfig;

  BatchGetDeserializer(final Executor deserializationExecutor, ClientConfig clientConfig) {
    this.deserializationExecutor = deserializationExecutor;
    this.clientConfig = clientConfig;
  }

  public abstract void deserialize(
      CompletableFuture<Map<K, V>> valueFuture,
      Iterable<MultiGetResponseRecordV1> records,
      List<K> keyList,
      Function<Integer, RecordDeserializer<V>> recordDeserializerGetter,
      Reporter responseDeserializationComplete,
      Optional<ClientStats> stats,
      long preResponseEnvelopeDeserialization);
}
