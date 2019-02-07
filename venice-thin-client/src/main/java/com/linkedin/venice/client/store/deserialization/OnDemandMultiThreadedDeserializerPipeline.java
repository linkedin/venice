package com.linkedin.venice.client.store.deserialization;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.stats.Reporter;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.LatencyUtils;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

/**
 * This {@link BatchGetDeserializer} will spin up a number of short-lived futures for each request,
 * depending on the amount of keys in the request. Each future consumes from a transient blocking queue
 * created for the lifetime of the request.
 */
public class OnDemandMultiThreadedDeserializerPipeline<K, V> extends BatchGetDeserializer<K, V> {
  private static final int END_OF_COLLECTION_SENTINEL_VALUE = -1;

  public OnDemandMultiThreadedDeserializerPipeline(Executor deserializationExecutor, ClientConfig clientConfig) {
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
    Map<K, V> resultMap = new ConcurrentHashMap<>(keyList.size());
    BlockingQueue<MultiGetResponseRecordV1> queue = new LinkedBlockingQueue<>();

    int numberOfRecordsDeserializationThread = Math.max(keyList.size() / clientConfig.getOnDemandDeserializerNumberOfRecordsPerThread(), 1);

    CompletableFuture[] recordsDeserializationFutures = new CompletableFuture[numberOfRecordsDeserializationThread];
    long preResponseRecordsDeserializationSubmission = System.nanoTime();
    for (int futureIdx = 0; futureIdx < numberOfRecordsDeserializationThread; futureIdx++) {
      recordsDeserializationFutures[futureIdx] = CompletableFuture.runAsync(
          () -> {
            stats.ifPresent(clientStats -> clientStats.recordResponseRecordsDeserializationSubmissionToStartTime(
                LatencyUtils.getLatencyInMS(preResponseRecordsDeserializationSubmission)));
            long preResponseRecordsDeserialization = System.nanoTime();
            while (true) {
              try {
                MultiGetResponseRecordV1 record = queue.take();
                int keyIdx = record.keyIndex;
                if (keyIdx == END_OF_COLLECTION_SENTINEL_VALUE) {
                  if (record.schemaId == numberOfRecordsDeserializationThread) {
                    valueFuture.complete(resultMap);
                    responseDeserializationComplete.report();
                    stats.ifPresent(clientStats -> clientStats.recordResponseRecordsDeserializationTime(LatencyUtils.getLatencyInMS(preResponseRecordsDeserialization)));
                  }
                  break;
                } else if (keyIdx >= keyList.size() || keyIdx < 0) {
                  valueFuture.completeExceptionally(new VeniceClientException("Key index: " + keyIdx + " doesn't have a corresponding key"));
                }
                int recordSchemaId = record.schemaId;
                RecordDeserializer<V> dataDeserializer = recordDeserializerGetter.apply(recordSchemaId);
                V value = dataDeserializer.deserialize(record.value);
                resultMap.put(keyList.get(keyIdx), value);
              } catch (InterruptedException e) {
                valueFuture.completeExceptionally(e);
                break;
              } catch (Exception e) {
                valueFuture.completeExceptionally(new VeniceClientException("Unexpected exception during record deserialization!", e));
                break;
              }
            }
          }, deserializationExecutor
      );
    }

    for (MultiGetResponseRecordV1 record : records) {
      queue.add(record);
    }
    stats.ifPresent(clientStats -> clientStats.recordResponseEnvelopeDeserializationTime(LatencyUtils.getLatencyInMS(preResponseEnvelopeDeserialization)));

    /**
     * Here, we want to send one sentinel record for each deserialization future we spun up, in order to ensure that
     * all futures terminate as soon as we know that there won't be any more items to process in the queue. Since
     * an item taken from the queue cannot be seen by the other futures, and each future will break after a single
     * sentinel record, then we need exactly one sentinel record per future.
     *
     * This mechanism also guarantees that all records have been processed, since by the time a future gets to a
     * sentinel record, it is certain that its previous iteration of the loop, and therefore, it fully finished
     * deserializing and putting in the map the previous record it pulled from the queue. If there was just a single
     * sentinel record, it could open up race conditions where one future pulled the second to last item, then got
     * its thread paused (because of context switching) and then another future pulled the sentinel record and
     * completed the future.
     */
    for (int futureIdx = 0; futureIdx < numberOfRecordsDeserializationThread; futureIdx++) {
      MultiGetResponseRecordV1 sentinelRecord = new MultiGetResponseRecordV1();
      sentinelRecord.keyIndex = END_OF_COLLECTION_SENTINEL_VALUE;
      sentinelRecord.value = ByteBuffer.allocate(0);
      sentinelRecord.schemaId = futureIdx + 1;
      queue.add(sentinelRecord);
    }
  }
}
