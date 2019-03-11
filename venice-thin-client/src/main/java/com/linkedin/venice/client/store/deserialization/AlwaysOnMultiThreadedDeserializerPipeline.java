package com.linkedin.venice.client.store.deserialization;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.stats.Reporter;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;


/**
 * This {@link BatchDeserializer} maintains a fixed number of long-running threads, which are
 * used to deserialize records originating from any request. Its threads will be added exactly
 * once per instance of {@link Executor} passed to it, in order to avoid starving it of threads
 * if it it is re-used across clients.
 */
public class AlwaysOnMultiThreadedDeserializerPipeline<E, K, V> extends BatchDeserializer<E, K, V> {
  /**
   * This map is used to ensure that each {@link Executor} only gets threads added to it once.
   */
  private static final Map<Executor, BlockingQueue<RecordContainer>> ALREADY_SEEDED_EXECUTORS = new HashMap<>();

  private final BlockingQueue<RecordContainer> queue;

  public AlwaysOnMultiThreadedDeserializerPipeline(Executor deserializationExecutor, ClientConfig clientConfig) {
    super(deserializationExecutor, clientConfig);

    synchronized (AlwaysOnMultiThreadedDeserializerPipeline.class) {
      if (!ALREADY_SEEDED_EXECUTORS.containsKey(deserializationExecutor)) {
        BlockingQueue<RecordContainer> queueDedicatedToExecutor =
            new LinkedBlockingQueue<>(this.clientConfig.getAlwaysOnDeserializerQueueCapacity());
        ALREADY_SEEDED_EXECUTORS.put(deserializationExecutor, queueDedicatedToExecutor);
        for (int i = 0; i < this.clientConfig.getAlwaysOnDeserializerNumberOfThreads(); i++) {
          deserializationExecutor.execute(() -> {
            Thread.currentThread().setName(Thread.currentThread().getName() + "-" + this.getClass().getSimpleName());
            while (true) {
              RecordContainer<E, K, V> recordContainer;
              try {
                recordContainer = queueDedicatedToExecutor.take();
                if (null == recordContainer) {
                  continue;
                }
              } catch (InterruptedException e) {
                break;
              }
              try {
                recordContainer.resultsMap.initFirstRecordedTimeStamp(() -> System.nanoTime());
                recordContainer.envelopeProcessor.accept(recordContainer.resultsMap, recordContainer.envelope);
              } catch (Exception e) {
                recordContainer.resultsMap.valueFuture.completeExceptionally(
                    new VeniceClientException("Unexpected exception during record deserialization!", e));
              }
            }
          });
        }
      }
    }
    this.queue = ALREADY_SEEDED_EXECUTORS.get(deserializationExecutor);
  }

  @Override
  public void deserialize(
      CompletableFuture<Map<K, V>> valueFuture,
      Iterable<E> envelopes,
      List<K> keyList,
      BiConsumer<Map<K, V>, E> envelopeProcessor,
      Reporter responseDeserializationComplete,
      Optional<ClientStats> stats,
      long preResponseEnvelopeDeserialization) {

    long recordsDeserializationStartTime = System.nanoTime();
    Consumer<Long> endReporter = preResponseRecordsDeserialization -> {
      responseDeserializationComplete.report();
      stats.ifPresent(clientStats -> clientStats.recordResponseRecordsDeserializationTime(
          LatencyUtils.getLatencyInMS(preResponseRecordsDeserialization)));
      stats.ifPresent(clientStats -> clientStats.recordResponseRecordsDeserializationSubmissionToStartTime(
          LatencyUtils.convertLatencyFromNSToMS(preResponseRecordsDeserialization - recordsDeserializationStartTime)));
    };
    FuturisticMap<K, V> resultMap = new FuturisticMap<>(valueFuture, endReporter, keyList.size());
    int recordCount = 0;
    for (E envelope : envelopes) {
      recordCount++;
      try {
        queue.put(new RecordContainer(envelope, resultMap, envelopeProcessor));
      } catch (InterruptedException e) {
        valueFuture.completeExceptionally(new VeniceException("Interrupted during deserialization!", e));
      }
    }
    stats.ifPresent(clientStats -> clientStats.recordResponseEnvelopeDeserializationTime(
        LatencyUtils.getLatencyInMS(preResponseEnvelopeDeserialization)));
    resultMap.setTargetSize(recordCount);
  }

  private static class RecordContainer<E, K, V> {
    private final E envelope;
    private final FuturisticMap<K, V> resultsMap;
    private final BiConsumer<Map<K, V>, E> envelopeProcessor;

    RecordContainer(
        E envelope,
        FuturisticMap<K, V> resultsMap,
        BiConsumer<Map<K, V>, E> envelopeProcessor) {
      this.envelope = envelope;
      this.resultsMap = resultsMap;
      this.envelopeProcessor = envelopeProcessor;
    }
  }

  /**
   * This map traveled back in time to come see us in a DeLorean.
   *
   * When filled up with a sufficient amount of entries, it will send itself back to the future again.
   */
  private static class FuturisticMap<K, V> extends ConcurrentHashMap<K, V> {
    private static final int NOT_INITIALIZED = -1;
    private final AtomicInteger size = new AtomicInteger(0);
    private final CompletableFuture<Map<K, V>> valueFuture;
    private final Consumer<Long> endReporter;
    private int targetSize = NOT_INITIALIZED;
    private long firstRecordTimeStampNS = NOT_INITIALIZED;

    FuturisticMap(CompletableFuture<Map<K, V>> valueFuture, Consumer<Long> endReporter, int capacity) {
      super(capacity);
      this.valueFuture = valueFuture;
      this.endReporter = endReporter;
    }

    @Override
    public int size() {
      return size.get();
    }

    @Override
    public boolean isEmpty() {
      return size() == 0;
    }

    @Override
    public void clear() {
      super.clear();
      size.set(0);
    }

    @Override
    public V put(K key, V value) {
      V previous = super.put(key, value);
      if (null == previous) {
        incrementSizeAndCheckTarget();
      }
      return previous;
    }

    @Override
    public V putIfAbsent(K key, V value) {
      V previous = super.putIfAbsent(key, value);
      if (null == previous) {
        incrementSizeAndCheckTarget();
      }
      return previous;
    }

    @Override
    public V remove(Object key) {
      V previous = super.remove(key);
      if (null != previous) {
        size.getAndDecrement();
      }
      return previous;
    }

    @Override
    public boolean remove(Object key, Object value) {
      boolean removed = super.remove(key, value);
      if (removed) {
        size.getAndDecrement();
      }
      return removed;
    }

    private void setTargetSize(int targetSize) {
      this.targetSize = targetSize;
      checkTarget(size.get());
    }

    /**
     * Used to keep track of the first time a given map has been seen by one of the deserialization threads.
     */
    private void initFirstRecordedTimeStamp(Supplier<Long> timeStampProvider) {
      if (NOT_INITIALIZED == firstRecordTimeStampNS) {
        synchronized (this) {
          if (NOT_INITIALIZED == firstRecordTimeStampNS) {
            firstRecordTimeStampNS = timeStampProvider.get();
          }
        }
      }
    }

    private void incrementSizeAndCheckTarget() {
      checkTarget(size.incrementAndGet());
    }

    private void checkTarget(int currentSize) {
      if (currentSize == targetSize) {
        valueFuture.complete(this);
        endReporter.accept(firstRecordTimeStampNS);
      }
    }
  }
}
