package com.linkedin.venice.client.store.deserialization;

import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.stats.Reporter;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.serializer.RecordDeserializer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;


/**
 * This {@link BatchGetDeserializer} does nothing, and always returns an empty list.
 *
 * The intent is to use this serializer in order to stress the Venice backend services
 * with more requests, while doing as little work as possible in the client. This can
 * also be used to test the transport-only portion of the client, without any
 * deserialization costs.
 */
public class BlackHoleDeserializer<K, V> extends BatchGetDeserializer<K, V> {
  public BlackHoleDeserializer(Executor deserializationExecutor, ClientConfig clientConfig) {
    super(deserializationExecutor, clientConfig);
  }

  @Override
  public void deserialize(
      final CompletableFuture<Map<K, V>> valueFuture,
      final Iterable<MultiGetResponseRecordV1> records,
      final List<K> keyList,
      final Function<Integer, RecordDeserializer<V>> recordDeserializerGetter,
      final Reporter responseDeserializationComplete,
      final Optional<ClientStats> stats,
      final long preResponseEnvelopeDeserialization) {
    valueFuture.complete(new HashMap<>(1));
    responseDeserializationComplete.report();
  }
}
