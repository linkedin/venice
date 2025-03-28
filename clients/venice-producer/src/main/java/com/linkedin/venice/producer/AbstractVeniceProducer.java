package com.linkedin.venice.producer;

import static com.linkedin.venice.ConfigKeys.CLIENT_PRODUCER_THREAD_NUM;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_OVER_SSL;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS;
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
import com.linkedin.venice.stats.ThreadPoolStats;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.BlockingQueueType;
import com.linkedin.venice.utils.concurrent.ThreadPoolFactory;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
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
 * A generic implementation of the {@link VeniceProducer} interface
 *
 * @see VeniceProducer
 */
public abstract class AbstractVeniceProducer<K, V> implements VeniceProducer<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(AbstractVeniceProducer.class);
  private static final DurableWrite DURABLE_WRITE = new DurableWrite();

  private VeniceProperties producerConfigs;
  private boolean configured = false;
  private boolean closed = false;
  private VeniceProducerMetrics producerMetrics;

  private SchemaReader schemaReader;
  private ThreadPoolExecutor producerExecutor;
  private VeniceWriter<byte[], byte[], byte[]> veniceWriter;

  private RecordSerializer<Object> keySerializer;
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

    this.producerExecutor = ThreadPoolFactory.createThreadPool(
        producerConfigs.getInt(CLIENT_PRODUCER_THREAD_NUM, 10),
        "ClientProducer",
        Integer.MAX_VALUE,
        BlockingQueueType.LINKED_BLOCKING_QUEUE);
    if (metricsRepository != null) {
      new ThreadPoolStats(metricsRepository, producerExecutor, "client_producer_thread_pool");
    }
    this.keySerializer = getSerializer(schemaReader.getKeySchema());

    VersionCreationResponse versionCreationResponse = requestTopic();
    this.veniceWriter = getVeniceWriter(versionCreationResponse);
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
    Integer partitionCount = versionCreationResponse.getPartitions();
    Properties partitionerProperties = new Properties();
    partitionerProperties.putAll(versionCreationResponse.getPartitionerParams());
    VenicePartitioner venicePartitioner = PartitionUtils.getVenicePartitioner(
        versionCreationResponse.getPartitionerClass(),
        new VeniceProperties(partitionerProperties));
    return constructVeniceWriter(
        veniceWriterProperties,
        new VeniceWriterOptions.Builder(versionCreationResponse.getKafkaTopic()).setPartitioner(venicePartitioner)
            .setPartitionCount(partitionCount)
            .setChunkingEnabled(false)
            .build());
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
    return CompletableFuture.supplyAsync(() -> {
      Schema valueSchema;
      try {
        valueSchema = getSchemaFromObject(value);
      } catch (Exception e) {
        producerMetrics.recordFailedRequest();
        throw e;
      }
      // Might block
      int valueSchemaId;
      Exception schemaReadException = null;
      try {
        valueSchemaId = schemaReader.getValueSchemaId(valueSchema);
      } catch (Exception e) {
        valueSchemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
        schemaReadException = e;
      }
      if (valueSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID) {
        producerMetrics.recordFailedRequest();
        throw new VeniceException(
            "Could not find a registered schema id for schema: " + valueSchema
                + ". This might be transient if the schema has been registered recently.",
            schemaReadException);
      }
      final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
      final Instant sendStartTime = Instant.now();
      final PubSubProducerCallback callback = getPubSubProducerCallback(
          sendStartTime,
          completableFuture,
          "Failed to write the requested data to the PubSub system");

      byte[] keyBytes = keySerializer.serialize(key);
      byte[] valueBytes = getSerializer(valueSchema).serialize(value);

      try {
        veniceWriter.put(keyBytes, valueBytes, valueSchemaId, logicalTime, callback);
      } catch (Exception e) {
        callback.onCompletion(null, e);
        throw e;
      }

      try {
        completableFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new VeniceException(e);
      }

      return DURABLE_WRITE;
    }, producerExecutor);
  }

  private PubSubProducerCallback getPubSubProducerCallback(
      Instant sendStartTime,
      CompletableFuture<Void> completableFuture,
      String errorMessage) {
    final AtomicBoolean callbackTriggered = new AtomicBoolean();
    final PubSubProducerCallback callback = (PubSubProduceResult produceResult, Exception exception) -> {
      boolean firstInvocation = callbackTriggered.compareAndSet(false, true);
      // We do not expect this to be triggered multiple times, but we still handle the case for defensive reasons
      if (!firstInvocation) {
        return;
      }

      Duration sendDuration = Duration.between(sendStartTime, Instant.now());
      if (exception == null) {
        producerMetrics.recordSuccessfulRequestWithLatency(sendDuration.toMillis());
        completableFuture.complete(null);
      } else {
        producerMetrics.recordFailedRequest();
        LOGGER.error(errorMessage, exception);
        completableFuture.completeExceptionally(exception);
      }
    };
    return callback;
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
    return CompletableFuture.supplyAsync(() -> {
      final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
      final Instant sendStartTime = Instant.now();
      final PubSubProducerCallback callback = getPubSubProducerCallback(
          sendStartTime,
          completableFuture,
          "Failed to write the delete operation to the PubSub system");

      byte[] keyBytes = keySerializer.serialize(key);

      try {
        veniceWriter.delete(keyBytes, logicalTime, callback);
      } catch (Exception e) {
        callback.onCompletion(null, e);
        throw e;
      }

      try {
        completableFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new VeniceException(e);
      }

      return DURABLE_WRITE;
    }, producerExecutor);
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
    return CompletableFuture.supplyAsync(() -> {
      // Caching to avoid race conditions during processing of the function
      DerivedSchemaEntry updateSchemaEntry = schemaReader.getLatestUpdateSchema();

      if (updateSchemaEntry == null) {
        producerMetrics.recordFailedRequest();
        throw new VeniceException(
            "Update schema not found. Check if partial update is enabled for the store. This error"
                + " might also be transient if partial update has been enabled recently.");
      }

      Schema updateSchema = updateSchemaEntry.getSchema();

      if (updateSchemaEntry.getValueSchemaID() == SchemaData.INVALID_VALUE_SCHEMA_ID
          || updateSchemaEntry.getId() == SchemaData.INVALID_VALUE_SCHEMA_ID) {
        producerMetrics.recordFailedRequest();
        throw new VeniceException(
            "Could not find a registered schema id for schema: " + updateSchema
                + ". This might be transient if the schema has been registered recently.");
      }

      UpdateBuilder updateBuilder = new UpdateBuilderImpl(updateSchema);
      updateFunction.accept(updateBuilder);
      GenericRecord updateRecord = updateBuilder.build();

      final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
      final Instant sendStartTime = Instant.now();
      final AtomicBoolean callbackTriggered = new AtomicBoolean();
      final PubSubProducerCallback callback = getPubSubProducerCallback(
          sendStartTime,
          completableFuture,
          "Failed to write the partial update record to the PubSub system");

      byte[] keyBytes = keySerializer.serialize(key);
      byte[] updateBytes = getSerializer(updateSchema).serialize(updateRecord);

      try {
        veniceWriter.update(
            keyBytes,
            updateBytes,
            updateSchemaEntry.getValueSchemaID(),
            updateSchemaEntry.getId(),
            callback,
            logicalTime);
      } catch (Exception e) {
        if (!callbackTriggered.get()) {
          callback.onCompletion(null, e);
        }
        throw e;
      }

      try {
        completableFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new VeniceException(e);
      }

      return DURABLE_WRITE;
    }, producerExecutor);
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
    if (producerExecutor != null) {
      producerExecutor.shutdownNow();
      try {
        producerExecutor.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOGGER.warn("Caught InterruptedException while closing the Venice producer ExecutorService", e);
      }
    }

    Utils.closeQuietlyWithErrorLogged(veniceWriter);
  }

  protected boolean isClosed() {
    return closed;
  }
}
