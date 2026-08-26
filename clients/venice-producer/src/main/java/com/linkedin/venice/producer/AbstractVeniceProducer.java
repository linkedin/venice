package com.linkedin.venice.producer;

import static com.linkedin.venice.ConfigKeys.CLIENT_PRODUCER_CALLBACK_QUEUE_CAPACITY;
import static com.linkedin.venice.ConfigKeys.CLIENT_PRODUCER_CALLBACK_THREAD_COUNT;
import static com.linkedin.venice.ConfigKeys.CLIENT_PRODUCER_WORKER_COUNT;
import static com.linkedin.venice.ConfigKeys.CLIENT_PRODUCER_WORKER_QUEUE_CAPACITY;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_OVER_SSL;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.utils.LatencyUtils.convertNSToMS;
import static com.linkedin.venice.utils.LatencyUtils.getElapsedTimeFromNSToMS;
import static com.linkedin.venice.writer.VeniceWriter.APP_DEFAULT_LOGICAL_TS;

import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.SchemaPresenceChecker;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterHook;
import com.linkedin.venice.writer.VeniceWriterOptions;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Async Venice producer with configurable parallelism.
 *
 * <p>Flow: {@code asyncPut(key)} → Worker[partition % workers] → serialize → veniceWriter.put() →
 * PubSub callback → Callback executor → complete future</p>
 *
 * <p>Both worker pool and callback executor are optional (set count=0 for inline execution).
 * Per-key ordering is preserved since same key always routes to same worker.</p>
 *
 * @see VeniceProducer
 * @see PartitionedProducerExecutor
 */
public abstract class AbstractVeniceProducer<K, V> implements VeniceProducer<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(AbstractVeniceProducer.class);
  private static final DurableWrite DURABLE_WRITE = new DurableWrite();
  /** Sentinel value indicating key serialization failed; use reference equality (==) to check. */
  private static final byte[] SERIALIZATION_FAILED = new byte[0];

  // Default configuration values
  private static final int DEFAULT_WORKER_COUNT = 4;
  private static final int DEFAULT_WORKER_QUEUE_CAPACITY = 100000;
  private static final int DEFAULT_CALLBACK_THREAD_COUNT = 0; // Disabled by default
  private static final int DEFAULT_CALLBACK_QUEUE_CAPACITY = 100000;

  private VeniceProperties producerConfigs;
  private boolean configured = false;
  private volatile boolean closed = false;
  private VeniceProducerMetrics producerMetrics;

  private SchemaReader schemaReader;
  private PartitionedProducerExecutor asyncDispatcher;
  private VeniceWriter<byte[], byte[], byte[]> veniceWriter;
  private VenicePartitioner partitioner;
  private int partitionCount;
  private final Object deferredCompletionMonitor = new Object();
  private final ThreadLocal<Integer> deferredCompletionDepth = new ThreadLocal<>();
  private int pendingDeferredCompletions;
  private Throwable deferredCompletionFailure;

  private RecordSerializer<Object> keySerializer;
  protected boolean needsPartitionRouting;

  private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
  private static final Schema INT_SCHEMA = Schema.create(Schema.Type.INT);
  private static final Schema LONG_SCHEMA = Schema.create(Schema.Type.LONG);
  private static final Schema FLOAT_SCHEMA = Schema.create(Schema.Type.FLOAT);
  private static final Schema DOUBLE_SCHEMA = Schema.create(Schema.Type.DOUBLE);
  private static final Schema BYTES_SCHEMA = Schema.create(Schema.Type.BYTES);
  private static final Schema BOOL_SCHEMA = Schema.create(Schema.Type.BOOLEAN);

  protected void configure(
      String storeName,
      VeniceProperties producerConfigs,
      MetricsRepository metricsRepository,
      SchemaReader schemaReader,
      SchemaReader kmeSchemaReader) {
    this.configured = true;
    this.producerConfigs = producerConfigs;
    this.schemaReader = schemaReader;

    if (kmeSchemaReader != null) {
      new SchemaPresenceChecker(kmeSchemaReader, AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE)
          .verifySchemaVersionPresentOrExit();
      LOGGER.info("Successfully verified the latest protocols at runtime are valid in Venice backend.");
    }

    this.producerMetrics = new VeniceProducerMetrics(metricsRepository, storeName);

    // Create the async dispatcher for partition-based record dispatch and callbacks
    this.asyncDispatcher = createDispatcher(storeName, producerConfigs, metricsRepository);
    this.producerMetrics.registerQueueMetrics(this.asyncDispatcher);

    this.keySerializer = getSerializer(schemaReader.getKeySchema());

    VersionCreationResponse versionCreationResponse = requestTopic();
    this.partitionCount = versionCreationResponse.getPartitions();

    // Cache routing decision: only need partition routing when both multiple partitions AND multiple workers exist.
    // With a single partition or a single worker, all requests go to the same destination regardless of routing.
    this.needsPartitionRouting = partitionCount > 1 && asyncDispatcher.getWorkerCount() > 1;

    // Initialize partitioner from response
    Properties partitionerProperties = new Properties();
    partitionerProperties.putAll(versionCreationResponse.getPartitionerParams());
    this.partitioner = PartitionUtils.getVenicePartitioner(
        versionCreationResponse.getPartitionerClass(),
        new VeniceProperties(partitionerProperties));

    this.veniceWriter = getVeniceWriter(versionCreationResponse);
  }

  /**
   * Factory method for creating the dispatcher. Can be overridden for testing.
   *
   * <p>Execution modes based on configuration:</p>
   * <ul>
   *   <li>{@code workerCount = 0, callbackThreadCount = 0}: Fully inline - all tasks execute on caller thread</li>
   *   <li>{@code workerCount > 0, callbackThreadCount = 0}: Parallel workers, callbacks on PubSub thread</li>
   *   <li>{@code workerCount = 0, callbackThreadCount > 0}: Inline preprocessing, callbacks on dedicated threads</li>
   *   <li>{@code workerCount > 0, callbackThreadCount > 0}: Full async - parallel workers + callback isolation</li>
   * </ul>
   *
   * <p>Set {@code workerCount = 0} to disable worker threads and execute all preprocessing
   * inline on the caller thread. This is useful when the caller is already managing
   * concurrency externally or wants minimal overhead.</p>
   *
   * @see PartitionedProducerExecutor
   */
  protected PartitionedProducerExecutor createDispatcher(
      String storeName,
      VeniceProperties configs,
      MetricsRepository metricsRepository) {
    return new PartitionedProducerExecutor(
        configs.getInt(CLIENT_PRODUCER_WORKER_COUNT, DEFAULT_WORKER_COUNT),
        configs.getInt(CLIENT_PRODUCER_WORKER_QUEUE_CAPACITY, DEFAULT_WORKER_QUEUE_CAPACITY),
        configs.getInt(CLIENT_PRODUCER_CALLBACK_THREAD_COUNT, DEFAULT_CALLBACK_THREAD_COUNT),
        configs.getInt(CLIENT_PRODUCER_CALLBACK_QUEUE_CAPACITY, DEFAULT_CALLBACK_QUEUE_CAPACITY),
        storeName,
        metricsRepository);
  }

  private VeniceWriter<byte[], byte[], byte[]> getVeniceWriter(VersionCreationResponse versionCreationResponse) {
    Properties writerProps = producerConfigs.getPropertiesCopy();

    if (versionCreationResponse.isEnableSSL()) {
      writerProps.put(KAFKA_OVER_SSL, "true");
      writerProps.put(SSL_KAFKA_BOOTSTRAP_SERVERS, versionCreationResponse.getKafkaBootstrapServers());
      writerProps.put(PUBSUB_BROKER_ADDRESS, versionCreationResponse.getKafkaBootstrapServers());
    } else {
      writerProps.put(KAFKA_BOOTSTRAP_SERVERS, versionCreationResponse.getKafkaBootstrapServers());
      writerProps.put(PUBSUB_BROKER_ADDRESS, versionCreationResponse.getKafkaBootstrapServers());
    }

    return getVeniceWriter(versionCreationResponse, writerProps);
  }

  private VeniceWriter<byte[], byte[], byte[]> getVeniceWriter(
      VersionCreationResponse versionCreationResponse,
      Properties veniceWriterProperties) {
    VeniceWriterOptions.Builder optionsBuilder =
        new VeniceWriterOptions.Builder(versionCreationResponse.getKafkaTopic()).setPartitioner(partitioner)
            .setPartitionCount(partitionCount)
            .setChunkingEnabled(false)
            .setWriterHook(getWriterHook());

    // Extract concurrent producer configs from properties
    String producerCountProp = veniceWriterProperties.getProperty(VeniceWriter.PRODUCER_COUNT);
    if (producerCountProp != null) {
      optionsBuilder.setProducerCount(Integer.parseInt(producerCountProp));
    }
    String producerThreadCountProp = veniceWriterProperties.getProperty(VeniceWriter.PRODUCER_THREAD_COUNT);
    if (producerThreadCountProp != null) {
      optionsBuilder.setProducerThreadCount(Integer.parseInt(producerThreadCountProp));
    }
    String producerQueueSizeProp = veniceWriterProperties.getProperty(VeniceWriter.PRODUCER_QUEUE_SIZE);
    if (producerQueueSizeProp != null) {
      optionsBuilder.setProducerQueueSize(Integer.parseInt(producerQueueSizeProp));
    }

    return constructVeniceWriter(veniceWriterProperties, optionsBuilder.build());
  }

  // Visible for testing
  protected VeniceWriter<byte[], byte[], byte[]> constructVeniceWriter(
      Properties properties,
      VeniceWriterOptions writerOptions) {
    return new VeniceWriterFactory(properties).createVeniceWriter(writerOptions);
  }

  protected RecordSerializer<Object> getSerializer(Schema schema) {
    return FastSerializerDeserializerFactory.getFastAvroGenericSerializer(schema);
  }

  /**
   * Optional hook invoked by the core writer before each PubSub produce.
   *
   * <p>Subclasses can supply quota or admission control without duplicating the producer dispatch implementation.</p>
   */
  protected VeniceWriterHook getWriterHook() {
    return null;
  }

  private static Schema getSchemaFromObject(Object object) {
    if (object instanceof GenericContainer) {
      GenericContainer obj = (GenericContainer) object;
      return obj.getSchema();
    } else if (object instanceof CharSequence) { // convenience option.
      return STRING_SCHEMA;
    } else if (object instanceof Integer) {
      return INT_SCHEMA;
    } else if (object instanceof Long) {
      return LONG_SCHEMA;
    } else if (object instanceof Double) {
      return DOUBLE_SCHEMA;
    } else if (object instanceof Float) {
      return FLOAT_SCHEMA;
    } else if (object instanceof byte[] || object instanceof ByteBuffer) {
      return BYTES_SCHEMA;
    } else if (object instanceof Boolean) {
      return BOOL_SCHEMA;
    } else {
      throw new VeniceException(
          "Venice Producer only supports Avro objects, and primitives, found object of class: "
              + object.getClass().toString());
    }
  }

  /**
   * Gets key bytes, serializing if not already done during routing.
   */
  private byte[] getKeyBytes(K key, byte[] serializedKeyBytes) throws Exception {
    return serializedKeyBytes != null ? serializedKeyBytes : keySerializer.serialize(key);
  }

  /**
   * Serializes the key for partition routing if needed.
   *
   * @return null if no routing needed, {@link #SERIALIZATION_FAILED} on error (use == to check),
   *         or the serialized key bytes on success
   */
  private byte[] serializeKeyForRouting(K key, CompletableFuture<DurableWrite> durableWriteFuture) {
    if (!needsPartitionRouting) {
      return null;
    }
    try {
      return keySerializer.serialize(key);
    } catch (Exception e) {
      producerMetrics.recordFailedRequest();
      durableWriteFuture.completeExceptionally(new VeniceException("Key serialization failed", e));
      return SERIALIZATION_FAILED;
    }
  }

  @Override
  public CompletableFuture<DurableWrite> asyncPut(K key, V value) {
    return asyncPutInternal(APP_DEFAULT_LOGICAL_TS, key, value);
  }

  @Override
  public CompletableFuture<DurableWrite> asyncPut(long logicalTime, K key, V value) {
    if (logicalTime < 0) {
      return getFutureCompletedExceptionally("Logical time must be a non-negative value. Got: " + logicalTime);
    }

    return asyncPutInternal(logicalTime, key, value);
  }

  private CompletableFuture<DurableWrite> asyncPutInternal(long logicalTime, K key, V value) {
    String error = validateProducer();
    if (!StringUtils.isEmpty(error)) {
      return getFutureCompletedExceptionally(error);
    }
    producerMetrics.recordPutRequest();
    // Capture submission time at method entry for consistent end-to-end latency measurement
    long submissionTimeNanos = System.nanoTime();
    CompletableFuture<DurableWrite> durableWriteFuture = new CompletableFuture<>();

    byte[] serializedKeyBytes = serializeKeyForRouting(key, durableWriteFuture);
    if (serializedKeyBytes == SERIALIZATION_FAILED) {
      return durableWriteFuture;
    }
    int partition = serializedKeyBytes != null ? partitioner.getPartitionId(serializedKeyBytes, partitionCount) : 0;
    try {
      asyncDispatcher.submit(partition, () -> {
        long preprocessStartNanos = System.nanoTime();
        producerMetrics.recordQueueWaitLatency(convertNSToMS(preprocessStartNanos - submissionTimeNanos));
        try {
          byte[] keyBytes = getKeyBytes(key, serializedKeyBytes);
          Schema valueSchema = getSchemaFromObject(value);
          int valueSchemaId;
          try {
            valueSchemaId = schemaReader.getValueSchemaId(valueSchema);
          } catch (Exception e) {
            throw new VeniceException(
                "Could not find a registered schema id for schema: " + valueSchema
                    + ". This might be transient if the schema has been registered recently.",
                e);
          }

          byte[] valueBytes = getSerializer(valueSchema).serialize(value);
          long preprocessEndNanos = System.nanoTime();
          producerMetrics.recordPreprocessingLatency(convertNSToMS(preprocessEndNanos - preprocessStartNanos));
          veniceWriter.put(
              keyBytes,
              valueBytes,
              valueSchemaId,
              logicalTime,
              getPubSubProducerCallback(
                  durableWriteFuture,
                  submissionTimeNanos,
                  preprocessEndNanos,
                  "Failed to write the PUT record to the PubSub system"));
          producerMetrics.recordCallerToPubSubBufferLatency(getElapsedTimeFromNSToMS(submissionTimeNanos));
        } catch (Exception e) {
          completeDurableWriteFutureExceptionally(durableWriteFuture, e);
        }
      }, rejection -> completeRejectedWriteFutureExceptionally(durableWriteFuture, rejection));
    } catch (RejectedExecutionException ignored) {
      // The executor invokes the rejection callback for immediate and queued shutdown rejection.
    }

    return durableWriteFuture;
  }

  @Override
  public CompletableFuture<DurableWrite> asyncDelete(K key) {
    return asyncDeleteInternal(APP_DEFAULT_LOGICAL_TS, key);
  }

  @Override
  public CompletableFuture<DurableWrite> asyncDelete(long logicalTime, K key) {
    if (logicalTime < 0) {
      return getFutureCompletedExceptionally("Logical time must be a non-negative value. Got: " + logicalTime);
    }

    return asyncDeleteInternal(logicalTime, key);
  }

  private CompletableFuture<DurableWrite> asyncDeleteInternal(long logicalTime, K key) {
    String error = validateProducer();
    if (!StringUtils.isEmpty(error)) {
      return getFutureCompletedExceptionally(error);
    }

    producerMetrics.recordDeleteRequest();
    // Capture submission time at method entry for consistent end-to-end latency measurement
    long submissionTimeNanos = System.nanoTime();
    CompletableFuture<DurableWrite> durableWriteFuture = new CompletableFuture<>();

    byte[] serializedKeyBytes = serializeKeyForRouting(key, durableWriteFuture);
    if (serializedKeyBytes == SERIALIZATION_FAILED) {
      return durableWriteFuture;
    }
    int partition = serializedKeyBytes != null ? partitioner.getPartitionId(serializedKeyBytes, partitionCount) : 0;
    try {
      asyncDispatcher.submit(partition, () -> {
        long preprocessStartNanos = System.nanoTime();
        producerMetrics.recordQueueWaitLatency(convertNSToMS(preprocessStartNanos - submissionTimeNanos));
        try {
          byte[] keyBytes = getKeyBytes(key, serializedKeyBytes);
          long preprocessEndNanos = System.nanoTime();
          producerMetrics.recordPreprocessingLatency(convertNSToMS(preprocessEndNanos - preprocessStartNanos));
          veniceWriter.delete(
              keyBytes,
              logicalTime,
              getPubSubProducerCallback(
                  durableWriteFuture,
                  submissionTimeNanos,
                  preprocessEndNanos,
                  "Failed to write the DELETE record to the PubSub system"));
          producerMetrics.recordCallerToPubSubBufferLatency(getElapsedTimeFromNSToMS(submissionTimeNanos));
        } catch (Exception e) {
          completeDurableWriteFutureExceptionally(durableWriteFuture, e);
        }
      }, rejection -> completeRejectedWriteFutureExceptionally(durableWriteFuture, rejection));
    } catch (RejectedExecutionException ignored) {
      // The executor invokes the rejection callback for immediate and queued shutdown rejection.
    }

    return durableWriteFuture;
  }

  @Override
  public CompletableFuture<DurableWrite> asyncUpdate(K key, Consumer<UpdateBuilder> updateFunction) {
    return asyncUpdateInternal(APP_DEFAULT_LOGICAL_TS, key, updateFunction);
  }

  @Override
  public CompletableFuture<DurableWrite> asyncUpdate(long logicalTime, K key, Consumer<UpdateBuilder> updateFunction) {
    if (logicalTime < 0) {
      return getFutureCompletedExceptionally("Logical time must be a non-negative value. Got: " + logicalTime);
    }

    return asyncUpdateInternal(logicalTime, key, updateFunction);
  }

  private CompletableFuture<DurableWrite> asyncUpdateInternal(
      long logicalTime,
      K key,
      Consumer<UpdateBuilder> updateFunction) {
    String error = validateProducer();
    if (!StringUtils.isEmpty(error)) {
      return getFutureCompletedExceptionally(error);
    }

    producerMetrics.recordUpdateRequest();
    // Capture submission time at method entry for consistent end-to-end latency measurement
    long submissionTimeNanos = System.nanoTime();
    CompletableFuture<DurableWrite> durableWriteFuture = new CompletableFuture<>();

    byte[] serializedKeyBytes = serializeKeyForRouting(key, durableWriteFuture);
    if (serializedKeyBytes == SERIALIZATION_FAILED) {
      return durableWriteFuture;
    }
    int partition = serializedKeyBytes != null ? partitioner.getPartitionId(serializedKeyBytes, partitionCount) : 0;
    try {
      asyncDispatcher.submit(partition, () -> {
        long preprocessStartNanos = System.nanoTime();
        producerMetrics.recordQueueWaitLatency(convertNSToMS(preprocessStartNanos - submissionTimeNanos));
        try {
          byte[] keyBytes = getKeyBytes(key, serializedKeyBytes);
          // Caching to avoid race conditions during processing of the function
          DerivedSchemaEntry updateSchemaEntry = schemaReader.getLatestUpdateSchema();

          if (updateSchemaEntry == null) {
            throw new VeniceException(
                "Update schema not found. Check if partial update is enabled for the store. This error"
                    + " might also be transient if partial update has been enabled recently.");
          }

          Schema updateSchema = updateSchemaEntry.getSchema();

          if (updateSchemaEntry.getValueSchemaID() == SchemaData.INVALID_VALUE_SCHEMA_ID
              || updateSchemaEntry.getId() == SchemaData.INVALID_VALUE_SCHEMA_ID) {
            throw new VeniceException(
                "Could not find a registered schema id for schema: " + updateSchema
                    + ". This might be transient if the schema has been registered recently.");
          }

          UpdateBuilder updateBuilder = new UpdateBuilderImpl(updateSchema);
          updateFunction.accept(updateBuilder);
          GenericRecord updateRecord = updateBuilder.build();
          byte[] updateBytes = getSerializer(updateSchema).serialize(updateRecord);

          long preprocessEndNanos = System.nanoTime();
          producerMetrics.recordPreprocessingLatency(convertNSToMS(preprocessEndNanos - preprocessStartNanos));
          veniceWriter.update(
              keyBytes,
              updateBytes,
              updateSchemaEntry.getValueSchemaID(),
              updateSchemaEntry.getId(),
              getPubSubProducerCallback(
                  durableWriteFuture,
                  submissionTimeNanos,
                  preprocessEndNanos,
                  "Failed to write the UPDATE record to the PubSub system"),
              logicalTime);
          producerMetrics.recordCallerToPubSubBufferLatency(getElapsedTimeFromNSToMS(submissionTimeNanos));
        } catch (Exception e) {
          completeDurableWriteFutureExceptionally(durableWriteFuture, e);
        }
      }, rejection -> completeRejectedWriteFutureExceptionally(durableWriteFuture, rejection));
    } catch (RejectedExecutionException ignored) {
      // The executor invokes the rejection callback for immediate and queued shutdown rejection.
    }

    return durableWriteFuture;
  }

  /**
   * Create a callback for the PubSub producer that completes the durable write future.
   *
   * @param durableWriteFuture the future to complete
   * @param submissionTimeNanos submission time in nanoseconds (from System.nanoTime())
   * @param preprocessEndNanos preprocessing end time in nanoseconds (from System.nanoTime())
   * @param errorMessage error message to use if the operation fails
   */
  private PubSubProducerCallback getPubSubProducerCallback(
      CompletableFuture<DurableWrite> durableWriteFuture,
      long submissionTimeNanos,
      long preprocessEndNanos,
      String errorMessage) {
    final AtomicBoolean callbackTriggered = new AtomicBoolean();
    return (PubSubProduceResult produceResult, Exception exception) -> {
      boolean firstInvocation = callbackTriggered.compareAndSet(false, true);
      // We do not expect this to be triggered multiple times, but we still handle the case for defensive reasons
      if (!firstInvocation) {
        return;
      }

      long pubSubAckTimeNanos = System.nanoTime();
      if (exception != null) {
        producerMetrics.recordFailedRequest();
      } else {
        producerMetrics.recordSuccessfulRequestWithLatency(convertNSToMS(pubSubAckTimeNanos - preprocessEndNanos));
      }

      // Complete user future on the callback executor, inline, or handed off when invoked by a worker.
      try {
        executeCallbackOrDeferFromWorker(() -> {
          if (exception == null) {
            durableWriteFuture.complete(DURABLE_WRITE);
            producerMetrics.recordEndToEndLatency(getElapsedTimeFromNSToMS(submissionTimeNanos));
          } else {
            durableWriteFuture.completeExceptionally(new VeniceException(errorMessage, exception));
          }
        },
            rejection -> scheduleDeferredCompletion(
                () -> durableWriteFuture.completeExceptionally(
                    new VeniceException("Callback executor rejected during shutdown", rejection))));
      } catch (RejectedExecutionException ignored) {
        // Rejection completion is deferred so PubSub and shutdown threads never run user continuations.
      }
    };
  }

  /**
   * Complete durable write future exceptionally with proper metrics recording.
   * Note: End-to-end latency is not recorded for failed requests.
   *
   * @param durableWriteFuture the future to complete exceptionally
   * @param exception the exception that caused the failure
   */
  private void completeDurableWriteFutureExceptionally(
      CompletableFuture<DurableWrite> durableWriteFuture,
      Throwable exception) {
    try {
      executeCallbackOrDeferFromWorker(() -> {
        String errorMessage = "Write operation failed: " + exception.getMessage();
        if (durableWriteFuture.completeExceptionally(new VeniceException(errorMessage, exception))) {
          producerMetrics.recordFailedRequest();
        }
      }, rejection -> scheduleDeferredCompletion(() -> {
        String errorMessage = "Write operation failed (callback rejected): " + exception.getMessage();
        if (durableWriteFuture.completeExceptionally(new VeniceException(errorMessage, exception))) {
          producerMetrics.recordFailedRequest();
        }
      }));
    } catch (RejectedExecutionException ignored) {
      // Rejection completion is deferred so worker and shutdown threads never run user continuations.
    }
  }

  private void executeCallbackOrDeferFromWorker(Runnable completion, Consumer<Throwable> rejectionCallback) {
    if (!asyncDispatcher.isCallbackExecutorEnabled() && asyncDispatcher.isCurrentThreadExecutingWorker()) {
      scheduleDeferredCompletion(completion);
      return;
    }
    asyncDispatcher.executeCallback(completion, rejectionCallback);
  }

  private void completeRejectedWriteFutureExceptionally(
      CompletableFuture<DurableWrite> durableWriteFuture,
      Throwable rejection) {
    scheduleDeferredCompletion(() -> {
      String errorMessage = "Write operation failed: " + rejection.getMessage();
      if (durableWriteFuture.completeExceptionally(new VeniceException(errorMessage, rejection))) {
        producerMetrics.recordFailedRequest();
      }
    });
  }

  private void scheduleDeferredCompletion(Runnable completion) {
    scheduleDeferredCompletion(completion, ForkJoinPool.commonPool());
  }

  void scheduleDeferredCompletion(Runnable completion, Executor completionExecutor) {
    synchronized (deferredCompletionMonitor) {
      pendingDeferredCompletions++;
    }
    Runnable trackedCompletion = () -> {
      Integer previousDepth = deferredCompletionDepth.get();
      deferredCompletionDepth.set(previousDepth == null ? 1 : previousDepth + 1);
      Throwable failure = null;
      try {
        completion.run();
      } catch (Throwable throwable) {
        failure = throwable;
      } finally {
        synchronized (deferredCompletionMonitor) {
          if (failure != null && deferredCompletionFailure == null) {
            deferredCompletionFailure = failure;
          }
          pendingDeferredCompletions--;
          deferredCompletionMonitor.notifyAll();
        }
        if (previousDepth == null) {
          deferredCompletionDepth.remove();
        } else {
          deferredCompletionDepth.set(previousDepth);
        }
      }
    };
    try {
      completionExecutor.execute(trackedCompletion);
    } catch (RejectedExecutionException ignored) {
      trackedCompletion.run();
    }
  }

  /**
   * This function should return a {@link VersionCreationResponse} to determine the PubSub topic and the characteristics
   * that the producer should follow.
   */
  protected abstract VersionCreationResponse requestTopic();

  private String validateProducer() {
    if (closed) {
      return "Producer is already closed. New requests are not accepted.";
    }

    if (!configured) {
      return "Producer is not configured. Please call `configure`.";
    }

    return StringUtils.EMPTY;
  }

  private <D> CompletableFuture<D> getFutureCompletedExceptionally(String exceptionMessage) {
    CompletableFuture<D> future = new CompletableFuture<>();
    future.completeExceptionally(new VeniceException(exceptionMessage));
    return future;
  }

  @Override
  public void close() throws IOException {
    closed = true;
    boolean restoreInterrupt = Thread.interrupted();
    try {
      boolean workersTerminated = true;
      if (asyncDispatcher != null) {
        // Stop worker admission first, but keep callback delivery alive while the writer flushes and closes.
        asyncDispatcher.shutdownWorkers();
        TerminationResult gracefulTermination = awaitTerminationUninterruptibly(true, 60, TimeUnit.SECONDS);
        restoreInterrupt |= gracefulTermination.interrupted;
        workersTerminated = gracefulTermination.terminated;
        if (!workersTerminated) {
          LOGGER.warn("Async dispatcher did not terminate gracefully, forcing shutdown");
          asyncDispatcher.shutdownWorkersNow();
          TerminationResult forcedTermination = awaitTerminationUninterruptibly(true, 60, TimeUnit.SECONDS);
          restoreInterrupt |= forcedTermination.interrupted;
          workersTerminated = forcedTermination.terminated;
        }
      }

      if (!workersTerminated) {
        throw new IOException("Venice producer workers did not terminate; refusing to close the active writer");
      }

      Utils.closeQuietlyWithErrorLogged(veniceWriter);
      restoreInterrupt |= Thread.interrupted();

      if (asyncDispatcher != null) {
        asyncDispatcher.shutdownCallbacks();
        if (!asyncDispatcher.isCurrentThreadExecutingCallback()) {
          TerminationResult gracefulTermination = awaitTerminationUninterruptibly(false, 60, TimeUnit.SECONDS);
          restoreInterrupt |= gracefulTermination.interrupted;
          boolean callbacksTerminated = gracefulTermination.terminated;
          if (!callbacksTerminated) {
            LOGGER.warn("Async callback dispatcher did not terminate gracefully, forcing shutdown");
            asyncDispatcher.shutdownCallbacksNow();
            TerminationResult forcedTermination = awaitTerminationUninterruptibly(false, 60, TimeUnit.SECONDS);
            restoreInterrupt |= forcedTermination.interrupted;
            callbacksTerminated = forcedTermination.terminated;
          }
          if (!callbacksTerminated) {
            throw new IOException("Venice producer callbacks did not terminate after forced shutdown");
          }
        }
      }

      // Pending completions include the tracked wrappers executing on this thread. Those wrappers cannot finish until
      // close() returns, so exempt only their current nesting depth while draining every other completion.
      Integer depth = deferredCompletionDepth.get();
      int currentThreadCompletionDepth = depth == null ? 0 : depth;
      if (!awaitDeferredCompletionsUninterruptibly(currentThreadCompletionDepth, 60, TimeUnit.SECONDS)) {
        throw new IOException("Venice producer deferred completions did not terminate");
      }
    } finally {
      restoreInterrupt |= Thread.interrupted();
      if (restoreInterrupt) {
        Thread.currentThread().interrupt();
      }
    }
  }

  protected boolean isClosed() {
    return closed;
  }

  private TerminationResult awaitTerminationUninterruptibly(boolean workers, long timeout, TimeUnit unit) {
    long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
    boolean interrupted = false;
    while (true) {
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        return new TerminationResult(false, interrupted);
      }
      try {
        return new TerminationResult(
            workers
                ? asyncDispatcher.awaitWorkerTermination(remainingNanos, TimeUnit.NANOSECONDS)
                : asyncDispatcher.awaitCallbackTermination(remainingNanos, TimeUnit.NANOSECONDS),
            interrupted);
      } catch (InterruptedException exception) {
        interrupted = true;
      }
    }
  }

  private boolean awaitDeferredCompletionsUninterruptibly(int currentThreadCompletionDepth, long timeout, TimeUnit unit)
      throws IOException {
    long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
    boolean interrupted = false;
    try {
      while (true) {
        synchronized (deferredCompletionMonitor) {
          if (pendingDeferredCompletions <= currentThreadCompletionDepth) {
            if (deferredCompletionFailure != null) {
              throw new IOException(
                  "Failed while draining Venice producer deferred completions",
                  deferredCompletionFailure);
            }
            return true;
          }
          if (deadlineNanos - System.nanoTime() <= 0) {
            return false;
          }
        }
        try {
          ForkJoinPool.managedBlock(new DeferredCompletionBlocker(currentThreadCompletionDepth, deadlineNanos));
        } catch (InterruptedException exception) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private final class DeferredCompletionBlocker implements ForkJoinPool.ManagedBlocker {
    private final int currentThreadCompletionDepth;
    private final long deadlineNanos;

    private DeferredCompletionBlocker(int currentThreadCompletionDepth, long deadlineNanos) {
      this.currentThreadCompletionDepth = currentThreadCompletionDepth;
      this.deadlineNanos = deadlineNanos;
    }

    @Override
    public boolean block() throws InterruptedException {
      synchronized (deferredCompletionMonitor) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (pendingDeferredCompletions > currentThreadCompletionDepth && remainingNanos > 0) {
          TimeUnit.NANOSECONDS.timedWait(deferredCompletionMonitor, remainingNanos);
        }
        return isReleasableLocked();
      }
    }

    @Override
    public boolean isReleasable() {
      synchronized (deferredCompletionMonitor) {
        return isReleasableLocked();
      }
    }

    private boolean isReleasableLocked() {
      return pendingDeferredCompletions <= currentThreadCompletionDepth || deadlineNanos - System.nanoTime() <= 0;
    }
  }

  private static final class TerminationResult {
    private final boolean terminated;
    private final boolean interrupted;

    private TerminationResult(boolean terminated, boolean interrupted) {
      this.terminated = terminated;
      this.interrupted = interrupted;
    }
  }
}
