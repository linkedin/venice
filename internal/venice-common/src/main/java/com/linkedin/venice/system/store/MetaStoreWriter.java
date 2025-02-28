package com.linkedin.venice.system.store;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.systemstore.schemas.StoreKeySchemas;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StoreValueSchema;
import com.linkedin.venice.systemstore.schemas.StoreValueSchemas;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceResourceCloseResult;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is to write metadata: store properties/key schema/value schemas/replica statuses to meta system store.
 * So far, only child fabric should write to it and in the future, we may want to support the write from parent fabric,
 * which can be used for the fabric buildup.
 *
 * TODO: we may need to consider to close the corresponding VeniceWriter when the store is deleted.
 */
public class MetaStoreWriter implements Closeable {
  public static final String KEY_STRING_STORE_NAME = "KEY_STORE_NAME";
  public static final String KEY_STRING_CLUSTER_NAME = "KEY_CLUSTER_NAME";
  public static final String KEY_STRING_VERSION_NUMBER = "KEY_VERSION_NUMBER";
  public static final String KEY_STRING_PARTITION_ID = "KEY_PARTITION_ID";
  public static final String KEY_STRING_SCHEMA_ID = "KEY_SCHEMA_ID";
  private static final Logger LOGGER = LogManager.getLogger(MetaStoreWriter.class);

  private final Map<String, VeniceWriter> metaStoreWriterMap = new VeniceConcurrentHashMap<>();
  private final Map<String, ReentrantLock> metaStoreWriterLockMap = new VeniceConcurrentHashMap<>();
  private final TopicManager topicManager;
  private final VeniceWriterFactory writerFactory;
  private final Schema derivedComputeSchema;
  private final HelixReadOnlyZKSharedSchemaRepository zkSharedSchemaRepository;
  private int derivedComputeSchemaId = -1;

  private final PubSubTopicRepository pubSubTopicRepository;
  private final long closeTimeoutMs;
  private final int numOfConcurrentVwCloseOps;

  public MetaStoreWriter(
      TopicManager topicManager,
      VeniceWriterFactory writerFactory,
      HelixReadOnlyZKSharedSchemaRepository schemaRepo,
      PubSubTopicRepository pubSubTopicRepository,
      long closeTimeoutMs,
      int numOfConcurrentVwCloseOps) {
    /**
     * TODO: get the write compute schema from the constructor so that this class does not use {@link WriteComputeSchemaConverter}
     */
    this(
        topicManager,
        writerFactory,
        schemaRepo,
        WriteComputeSchemaConverter.getInstance()
            .convertFromValueRecordSchema(
                AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema()),
        pubSubTopicRepository,
        closeTimeoutMs,
        numOfConcurrentVwCloseOps);
  }

  MetaStoreWriter(
      TopicManager topicManager,
      VeniceWriterFactory writerFactory,
      HelixReadOnlyZKSharedSchemaRepository schemaRepo,
      Schema derivedComputeSchema,
      PubSubTopicRepository pubSubTopicRepository,
      long closeTimeoutMs,
      int numOfConcurrentVwCloseOps) {
    this.topicManager = topicManager;
    this.writerFactory = writerFactory;
    this.derivedComputeSchema = derivedComputeSchema;
    this.zkSharedSchemaRepository = schemaRepo;
    this.pubSubTopicRepository = pubSubTopicRepository;
    this.closeTimeoutMs = closeTimeoutMs;
    this.numOfConcurrentVwCloseOps = numOfConcurrentVwCloseOps;
  }

  /**
   * This function should be invoked for any store metadata change.
   */
  public void writeStoreProperties(String clusterName, Store store) {
    String storeName = store.getName();
    if (!(store instanceof ZKStore)) {
      throw new IllegalArgumentException(
          "Param 'store' must be an instance of 'ZKStore' for store name: " + storeName + ", but received: "
              + store.getClass());
    }
    write(storeName, MetaStoreDataType.STORE_PROPERTIES, () -> new HashMap<String, String>() {
      {
        put(KEY_STRING_STORE_NAME, storeName);
        put(KEY_STRING_CLUSTER_NAME, clusterName);
      }
    }, () -> {
      StoreMetaValue value = new StoreMetaValue();
      value.storeProperties = ((ZKStore) store).dataModel();
      return value;
    });
  }

  public void writeStoreKeySchemas(String storeName, Collection<SchemaEntry> keySchemas) {
    write(storeName, MetaStoreDataType.STORE_KEY_SCHEMAS, () -> new HashMap<String, String>() {
      {
        put(KEY_STRING_STORE_NAME, storeName);
      }
    }, () -> {
      StoreMetaValue value = new StoreMetaValue();
      StoreKeySchemas storeKeySchemas = new StoreKeySchemas();
      storeKeySchemas.keySchemaMap = buildSchemaMap(keySchemas);
      value.storeKeySchemas = storeKeySchemas;
      return value;
    });
  }

  /**
   * This function should be invoked for any value schema changes, and the {@param valueSchemas} should
   * contain all the value schemas since this operation will be a full PUT.
   */
  public void writeStoreValueSchemas(String storeName, Collection<SchemaEntry> valueSchemas) {
    writeStoreValueSchemasIndividually(storeName, valueSchemas);

    write(storeName, MetaStoreDataType.STORE_VALUE_SCHEMAS, () -> {
      Map<String, String> keyMap = new HashMap<>(1);
      keyMap.put(KEY_STRING_STORE_NAME, storeName);
      return keyMap;
    }, () -> {
      StoreMetaValue value = new StoreMetaValue();
      StoreValueSchemas storeValueSchemas = new StoreValueSchemas();
      storeValueSchemas.valueSchemaMap = buildSchemaIdOnlyMap(valueSchemas);
      value.storeValueSchemas = storeValueSchemas;
      return value;
    });
  }

  public void writeHeartbeat(String storeName, long heartbeatTimestamp) {
    write(
        storeName,
        MetaStoreDataType.HEARTBEAT,
        () -> Collections.singletonMap(KEY_STRING_STORE_NAME, storeName),
        StoreMetaValue::new,
        heartbeatTimestamp);
  }

  /**
   * Improved version of writeStoreValueSchemas. Instead of writing all value schemas into one K/V pair we write it to
   * a different key space where each K/V pair only represents one version of the value schema. This allows us to store
   * many versions of a large value schema.
   */
  private void writeStoreValueSchemasIndividually(String storeName, Collection<SchemaEntry> valueSchemas) {
    for (SchemaEntry schemaEntry: valueSchemas) {
      write(storeName, MetaStoreDataType.STORE_VALUE_SCHEMA, () -> {
        Map<String, String> keyMap = new HashMap<>(2);
        keyMap.put(KEY_STRING_STORE_NAME, storeName);
        keyMap.put(KEY_STRING_SCHEMA_ID, Integer.toString(schemaEntry.getId()));
        return keyMap;
      }, () -> {
        StoreMetaValue value = new StoreMetaValue();
        StoreValueSchema storeValueSchema = new StoreValueSchema();
        storeValueSchema.valueSchema = schemaEntry.getSchema().toString();
        value.storeValueSchema = storeValueSchema;
        return value;
      });
    }
  }

  public void writeInUseValueSchema(String storeName, int versionNumber, int valueSchemaId) {
    update(storeName, MetaStoreDataType.VALUE_SCHEMAS_WRITTEN_PER_STORE_VERSION, () -> {
      Map<String, String> map = new HashMap<>(2);
      map.put(KEY_STRING_STORE_NAME, storeName);
      map.put(KEY_STRING_VERSION_NUMBER, Integer.toString(versionNumber));
      return map;
    }, () -> {
      // Construct an update
      UpdateBuilder updateBuilder = new UpdateBuilderImpl(this.derivedComputeSchema);
      updateBuilder.setNewFieldValue("timestamp", System.currentTimeMillis());
      updateBuilder
          .setElementsToAddToListField("storeValueSchemaIdsWrittenPerStoreVersion", Arrays.asList(valueSchemaId));
      return updateBuilder.build();
    });
  }

  /**
   * Write {@link com.linkedin.venice.meta.StoreConfig} equivalent to the meta system store. This is still only invoked
   * by child controllers only.
   */
  public void writeStoreClusterConfig(StoreConfig storeConfig) {
    write(storeConfig.getStoreName(), MetaStoreDataType.STORE_CLUSTER_CONFIG, () -> new HashMap<String, String>() {
      {
        put(KEY_STRING_STORE_NAME, storeConfig.getStoreName());
      }
    }, () -> {
      StoreMetaValue value = new StoreMetaValue();
      value.storeClusterConfig = storeConfig.dataModel();
      return value;
    });
  }

  /**
   * This function should be used only for store deletion scenario.
   * @param metaStoreName
   */
  public void removeMetaStoreWriter(String metaStoreName) {
    VeniceWriter writer = getMetaStoreWriterMap().remove(metaStoreName);
    if (writer != null) {
      /**
       * Free the internal resource without sending any extra messages since the store is going to be deleted.
       */
      closeVeniceWriter(metaStoreName, writer, true);
      LOGGER.info("Removed the venice writer for meta store: {}", metaStoreName);
    }
  }

  public VeniceWriter getMetaStoreWriter(String metaStoreName) {
    return metaStoreWriterMap.get(metaStoreName);
  }

  private void write(
      String storeName,
      MetaStoreDataType dataType,
      Supplier<Map<String, String>> keyStringSupplier,
      Supplier<StoreMetaValue> valueSupplier) {
    write(storeName, dataType, keyStringSupplier, valueSupplier, System.currentTimeMillis());
  }

  private void write(
      String storeName,
      MetaStoreDataType dataType,
      Supplier<Map<String, String>> keyStringSupplier,
      Supplier<StoreMetaValue> valueSupplier,
      long timestamp) {
    String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    StoreMetaKey key = dataType.getStoreMetaKey(keyStringSupplier.get());
    StoreMetaValue value = valueSupplier.get();
    value.timestamp = timestamp;
    writeMessageWithRetry(
        metaStoreName,
        vw -> vw.put(key, value, AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.currentProtocolVersion.get()));
  }

  private void update(
      String storeName,
      MetaStoreDataType dataType,
      Supplier<Map<String, String>> keyStringSupplier,
      Supplier<GenericRecord> updateSupplier) {
    String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    if (derivedComputeSchemaId == -1) {
      /**
       * Fetch the derived compute schema id on demand for integration test since the meta system store is being created
       * during cluster initialization.
       */
      GeneratedSchemaID derivedSchemaId = getSchemaRepository()
          .getDerivedSchemaId(VeniceSystemStoreType.META_STORE.getZkSharedStoreName(), derivedComputeSchema.toString());
      if (!derivedSchemaId.isValid()) {
        throw new VeniceException(
            "The derived compute schema for meta system store hasn't been registered to Venice yet");
      }
      this.derivedComputeSchemaId = derivedSchemaId.getGeneratedSchemaVersion();
    }
    StoreMetaKey key = dataType.getStoreMetaKey(keyStringSupplier.get());
    GenericRecord update = updateSupplier.get();
    writeMessageWithRetry(metaStoreName, vw -> {
      vw.update(
          key,
          update,
          AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.currentProtocolVersion.get(),
          derivedComputeSchemaId,
          null);
    });
  }

  HelixReadOnlySchemaRepository getSchemaRepository() {
    return zkSharedSchemaRepository;
  }

  void writeMessageWithRetry(String metaStoreName, Consumer<VeniceWriter> writerConsumer) {
    ReentrantLock lock = getOrCreateMetaStoreWriterLock(metaStoreName);
    int messageProduceRetryCount = 0;
    int maxMessageProduceRetryCount = 3;
    VeniceWriter writer = null;
    boolean messageProduced = false;
    while (!messageProduced) {
      lock.lock();
      try {
        writer = getOrCreateMetaStoreWriter(metaStoreName);
        writerConsumer.accept(writer);
        writer.flush();
        messageProduced = true;
      } catch (Exception e) {
        messageProduceRetryCount++;
        if (messageProduceRetryCount < maxMessageProduceRetryCount) {
          LOGGER.warn(
              "Caught exception while trying to write message, will restart Venice Writer and retry {}/{}",
              messageProduceRetryCount,
              maxMessageProduceRetryCount);
          // Defensive coding to make sure close Venice Writer logic won't throw another exception.
          try {
            removeMetaStoreWriter(metaStoreName);
          } catch (Exception ex) {
            LOGGER.warn("Caught exception while trying to close Venice writer", e);
          }
        } else {
          LOGGER.error("Fail to write message after {} attempts.", maxMessageProduceRetryCount, e);
          break;
        }
      } finally {
        lock.unlock();
      }
    }
  }

  ReentrantLock getOrCreateMetaStoreWriterLock(String metaStoreName) {
    return metaStoreWriterLockMap.computeIfAbsent(metaStoreName, k -> new ReentrantLock());
  }

  Map<String, VeniceWriter> getMetaStoreWriterMap() {
    return metaStoreWriterMap;
  }

  VeniceWriter getOrCreateMetaStoreWriter(String metaStoreName) {
    return metaStoreWriterMap.computeIfAbsent(metaStoreName, k -> {
      PubSubTopic rtTopic = pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic(metaStoreName));
      if (!topicManager.containsTopicAndAllPartitionsAreOnline(rtTopic)) {
        throw new VeniceException("Realtime topic: " + rtTopic + " doesn't exist or some partitions are not online");
      }

      VeniceWriterOptions options = new VeniceWriterOptions.Builder(rtTopic.getName())
          .setKeyPayloadSerializer(
              new VeniceAvroKafkaSerializer(
                  AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE_KEY.getCurrentProtocolVersionSchema()))
          .setValuePayloadSerializer(
              new VeniceAvroKafkaSerializer(
                  AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema()))
          .setWriteComputePayloadSerializer(new VeniceAvroKafkaSerializer(derivedComputeSchema))
          .setChunkingEnabled(false)
          .setPartitionCount(1)
          .build();

      return writerFactory.createVeniceWriter(options);
    });
  }

  private Map<CharSequence, CharSequence> buildSchemaMap(Collection<SchemaEntry> schemas) {
    return schemas.stream()
        .collect(Collectors.toMap(s -> (Integer.toString(s.getId())), s -> s.getSchema().toString()));
  }

  private Map<CharSequence, CharSequence> buildSchemaIdOnlyMap(Collection<SchemaEntry> schemas) {
    return schemas.stream().collect(Collectors.toMap(s -> (Integer.toString(s.getId())), s -> ""));
  }

  /**
   * When {@param skipTopicCheck} is enabled, this function will skip the RT topic existence check and
   * close the internal Kafka producer directly without sending out any EOS messages.
   * Otherwise, it will perform the regular topic existence check to decide whether EOS should be sent or not.
   */
  private void closeVeniceWriter(String metaStoreName, VeniceWriter veniceWriter, boolean skipTopicCheck) {
    if (skipTopicCheck) {
      veniceWriter.close(false);
      return;
    }
    /**
     * Check whether the RT topic exists or not before closing Venice Writer since closing VeniceWriter will try
     * to write a Control Message to the RT topic, and it could hang if the topic doesn't exist.
     * This check is a best-effort since the race condition is still there between topic check and closing VeniceWriter.
     */
    PubSubTopic rtTopic = pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic(metaStoreName));
    if (!topicManager.containsTopicAndAllPartitionsAreOnline(rtTopic)) {
      LOGGER.info(
          "RT topic: {} for meta system store: {} doesn't exist, will only close the internal producer without sending END_OF_SEGMENT control messages",
          rtTopic,
          metaStoreName);
      veniceWriter.close(false);
    } else {
      veniceWriter.close();
    }
  }

  /**
   * If numOfConcurrentVwCloseOps is set to -1, then all the VeniceWriters will be closed asynchronously and concurrently.
   * If numOfConcurrentVwCloseOps is set to a positive number, then the VeniceWriters will be closed with a bounded concurrency until timeout.
   * Once timeout is reached, the remaining VeniceWriters will be closed asynchronously and concurrently.
   */
  @Override
  public synchronized void close() throws IOException {
    long startTime = System.currentTimeMillis();
    LOGGER.info(
        "Closing MetaStoreWriter - numOfVeniceWriters: {} permits: {} timeoutInMs: {}",
        metaStoreWriterMap.size(),
        numOfConcurrentVwCloseOps,
        closeTimeoutMs);
    // iterate through the map and close all the VeniceWriters
    List<CompletableFuture<VeniceResourceCloseResult>> closeFutures = new ArrayList<>(metaStoreWriterMap.size());
    List<VeniceWriter> writersToClose = new ArrayList<>(metaStoreWriterMap.values());
    metaStoreWriterMap.clear();
    // permit for the VeniceWriters to be closed asynchronously
    int permits = numOfConcurrentVwCloseOps == -1 ? writersToClose.size() : numOfConcurrentVwCloseOps;
    Semaphore semaphore = new Semaphore(permits);
    long deadline = startTime + closeTimeoutMs;
    for (VeniceWriter veniceWriter: writersToClose) {
      boolean acquired = false;
      try {
        acquired = semaphore.tryAcquire(Math.max(0, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Interrupted while acquiring semaphore", e);
      }
      CompletableFuture<VeniceResourceCloseResult> closeFuture = veniceWriter.closeAsync(true);
      closeFutures.add(closeFuture);
      if (acquired) {
        // release the semaphore when the future is completed
        closeFuture.whenComplete((result, throwable) -> semaphore.release());
      }
    }

    // wait for all the VeniceWriters to be closed with a bounded timeout
    try {
      CompletableFuture.allOf(closeFutures.toArray(new CompletableFuture[0]))
          .get(Math.max(1000, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while closing VeniceWriters", e);
    }

    // collect the close results
    EnumMap<VeniceResourceCloseResult, Integer> closeResultMap = new EnumMap<>(VeniceResourceCloseResult.class);
    for (CompletableFuture<VeniceResourceCloseResult> future: closeFutures) {
      if (!future.isDone()) {
        closeResultMap.compute(VeniceResourceCloseResult.UNKNOWN, (key, value) -> value == null ? 1 : value + 1);
        continue;
      }
      // for the completed future, get the close result and increment the counter
      try {
        closeResultMap.compute(future.get(), (key, value) -> value == null ? 1 : value + 1);
      } catch (Exception e) {
        LOGGER.warn("Caught exception while getting VeniceResourceCloseResult", e);
        closeResultMap.compute(VeniceResourceCloseResult.FAILED, (key, value) -> value == null ? 1 : value + 1);
      }
    }

    LOGGER.info(
        "Closed MetaStoreWriter in {} ms - numbOfVeniceWriters: {} veniceWriterCloseResult: {}",
        System.currentTimeMillis() - startTime,
        writersToClose.size(),
        closeResultMap);
  }
}
