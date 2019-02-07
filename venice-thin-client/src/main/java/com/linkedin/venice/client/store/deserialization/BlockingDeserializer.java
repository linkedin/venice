package com.linkedin.venice.client.store.deserialization;

import com.linkedin.venice.client.exceptions.VeniceClientException;
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
 * This {@link BatchGetDeserializer} does not do anything asynchronously. It simply deserializes
 * all records sequentially on the same thread that called it.
 */
public class BlockingDeserializer<K, V> extends BatchGetDeserializer<K, V> {
  public BlockingDeserializer(Executor deserializationExecutor, ClientConfig clientConfig) {
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
    Map<K, V> resultMap = new HashMap<>(keyList.size());
    for (MultiGetResponseRecordV1 record : records) {
      int keyIdx = record.keyIndex;
      if (keyIdx >= keyList.size() || keyIdx < 0) {
        valueFuture.completeExceptionally(new VeniceClientException("Key index: " + keyIdx + " doesn't have a corresponding key"));
      }
      int recordSchemaId = record.schemaId;
      RecordDeserializer<V> dataDeserializer = recordDeserializerGetter.apply(recordSchemaId);
      V value = dataDeserializer.deserialize(record.value);
      resultMap.put(keyList.get(keyIdx), value);
    }
    valueFuture.complete(resultMap);
    responseDeserializationComplete.report();
  }
}
