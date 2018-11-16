package com.linkedin.venice.client.store.deserialization;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.stats.Reporter;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * This {@link BatchGetDeserializer} forks off one new future for each record.
 */
public class OneFuturePerRecordDeserializer<K, V> extends BatchGetDeserializer<K, V> {
  OneFuturePerRecordDeserializer(Executor deserializationExecutor, ClientConfig clientConfig) {
    super(deserializationExecutor, clientConfig);
  }

  @Override
  public void deserialize(
      CompletableFuture<Map<K, V>> valueFuture,
      Iterable<MultiGetResponseRecordV1> records,
      List<K> keyList,
      Function<Integer, RecordDeserializer<V>> recordDeserializerGetter,
      Reporter responseDeserializationComplete,
      Optional<ClientStats> stats,
      long preResponseEnvelopeDeserialization) {
    List<CompletableFuture<Void>> futureList = new LinkedList<>();
    Map<K, V> resultMap = new ConcurrentHashMap<>(keyList.size());
    AtomicLong earliestPreResponseRecordsDeserialization = new AtomicLong(Long.MAX_VALUE);
    long preResponseRecordsDeserializationSubmision = System.nanoTime();
    for (MultiGetResponseRecordV1 record : records) {
      futureList.add(CompletableFuture.runAsync(() -> {
        stats.ifPresent(clientStats -> clientStats.recordResponseRecordsDeserializationSubmissionToStartTime(
            LatencyUtils.getLatencyInMS(preResponseRecordsDeserializationSubmision)));
        long preResponseRecordsDeserialization = System.nanoTime();

        int keyIdx = record.keyIndex;
        if (keyIdx >= keyList.size() || keyIdx < 0) {
          valueFuture.completeExceptionally(new VeniceClientException("Key index: " + keyIdx + " doesn't have a corresponding key"));
        }
        int recordSchemaId = record.schemaId;
        byte[] serializedData = record.value.array();
        RecordDeserializer<V> dataDeserializer = recordDeserializerGetter.apply(recordSchemaId);
        V value = dataDeserializer.deserialize(serializedData);
        resultMap.put(keyList.get(keyIdx), value);
        earliestPreResponseRecordsDeserialization.accumulateAndGet(preResponseRecordsDeserialization,
            (left, right) -> Math.min(left, right));
      }, deserializationExecutor));
    }
    stats.ifPresent(clientStats -> clientStats.recordResponseEnvelopeDeserializationTime(LatencyUtils.getLatencyInMS(preResponseEnvelopeDeserialization)));
    CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()])).handle((v, throwable1) -> {
      if (null != throwable1) {
        valueFuture.completeExceptionally(throwable1);
      }
      valueFuture.complete(resultMap);
      responseDeserializationComplete.report();
      stats.ifPresent(clientStats -> clientStats.recordResponseRecordsDeserializationTime(LatencyUtils.getLatencyInMS(earliestPreResponseRecordsDeserialization.get())));
      return null;
    });
  }
}
