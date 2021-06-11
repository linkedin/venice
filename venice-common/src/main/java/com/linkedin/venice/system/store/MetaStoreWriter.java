package com.linkedin.venice.system.store;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.ReplicaStatus;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.WriteComputeSchemaAdapter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.systemstore.schemas.StoreKeySchemas;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StoreReplicaStatus;
import com.linkedin.venice.systemstore.schemas.StoreValueSchemas;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import static com.linkedin.venice.schema.WriteComputeSchemaAdapter.*;


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

  private static final Logger LOGGER = Logger.getLogger(MetaStoreWriter.class);


  private final Map<String, VeniceWriter> metaStoreWriterMap = new VeniceConcurrentHashMap<>();
  private final TopicManager topicManager;
  private final VeniceWriterFactory writerFactory;
  private final Schema derivedComputeSchema;
  private final HelixReadOnlyZKSharedSchemaRepository zkSharedSchemaRepository;
  private int derivedComputeSchemaId = -1;

  public MetaStoreWriter(TopicManager topicManager, VeniceWriterFactory writerFactory, HelixReadOnlyZKSharedSchemaRepository schemaRepo) {
    this.topicManager = topicManager;
    this.writerFactory = writerFactory;
    this.derivedComputeSchema = WriteComputeSchemaAdapter.parse(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema());
    this.zkSharedSchemaRepository = schemaRepo;
  }

  /**
   * This function should be invoked for any store metadata change.
   */
  public void writeStoreProperties(String clusterName, Store store) {
    String storeName = store.getName();
    if (!(store instanceof ZKStore)) {
      throw new IllegalArgumentException("Param 'store' must be an instance of 'ZKStore' for store name: " + storeName + ", but received: " + store.getClass());
    }
    write(storeName, MetaStoreDataType.STORE_PROPERTIES,
        () -> new HashMap<String, String>() {{ put(KEY_STRING_STORE_NAME, storeName); put(KEY_STRING_CLUSTER_NAME, clusterName);}},
        () -> {
          StoreMetaValue value = new StoreMetaValue();
          value.storeProperties = ((ZKStore) store).dataModel();
          return value;
        });
  }

  public void writeStoreKeySchemas(String storeName, Collection<SchemaEntry> keySchemas) {
    write(storeName, MetaStoreDataType.STORE_KEY_SCHEMAS,
        () -> new HashMap<String, String>() {{ put(KEY_STRING_STORE_NAME, storeName);}},
        () -> {
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
    write(storeName, MetaStoreDataType.STORE_VALUE_SCHEMAS,
        () -> new HashMap<String, String>() {{ put(KEY_STRING_STORE_NAME, storeName);}},
        () -> {
          StoreMetaValue value = new StoreMetaValue();
          StoreValueSchemas storeValueSchemas = new StoreValueSchemas();
          storeValueSchemas.valueSchemaMap = buildSchemaMap(valueSchemas);
          value.storeValueSchemas = storeValueSchemas;
          return value;
        });
  }

  /**
   * This function is used to produce a snapshot for replica statuses.
   */
  public void writeStoreReplicaStatuses(String clusterName, String storeName, int version, int partitionId,
      Collection<ReplicaStatus> replicaStatuses) {
    write(storeName, MetaStoreDataType.STORE_REPLICA_STATUSES,
        () -> new HashMap<String, String>() {{
          put(KEY_STRING_STORE_NAME, storeName);
          put(KEY_STRING_CLUSTER_NAME, clusterName);
          put(KEY_STRING_VERSION_NUMBER, Integer.toString(version));
          put(KEY_STRING_PARTITION_ID, Integer.toString(partitionId));
        }},
        () -> {
          StoreMetaValue value = new StoreMetaValue();
          Map<CharSequence, StoreReplicaStatus> replicaMap = new HashMap<>();
          for (ReplicaStatus replicaStatus : replicaStatuses) {
            StoreReplicaStatus storeReplicaStatus = new StoreReplicaStatus();
            storeReplicaStatus.status = replicaStatus.getCurrentStatus().getValue();
            replicaMap.put(Instance.fromNodeId(replicaStatus.getInstanceId()).getUrl(true), storeReplicaStatus);
          }
          value.storeReplicaStatuses = replicaMap;
          return value;
        });
  }

  /**
   * This function should be invoked during ingestion when execution status changes.
   */
  public void writeStoreReplicaStatus(String clusterName, String storeName, int version, int partitionId,
      Instance instance, ExecutionStatus executionStatus) {
    update(storeName, MetaStoreDataType.STORE_REPLICA_STATUSES,
        () -> new HashMap<String, String>() {{
          put(KEY_STRING_STORE_NAME, storeName);
          put(KEY_STRING_CLUSTER_NAME, clusterName);
          put(KEY_STRING_VERSION_NUMBER, Integer.toString(version));
          put(KEY_STRING_PARTITION_ID, Integer.toString(partitionId));
        }},
        () -> {
          // Construct an update
          GenericRecord update = new GenericData.Record(derivedComputeSchema.getTypes().get(0));
          update.put("timestamp", System.currentTimeMillis());
          GenericRecord storeReplicaStatusesUpdate = new GenericData.Record(derivedComputeSchema.getTypes().get(0).getField("storeReplicaStatuses").schema().getTypes().get(2));
          Map<CharSequence, StoreReplicaStatus> instanceStatusMap = new HashMap<>();
          StoreReplicaStatus replicaStatus = new StoreReplicaStatus();
          replicaStatus.status = executionStatus.getValue();
          instanceStatusMap.put(instance.getUrl(true), replicaStatus);
          storeReplicaStatusesUpdate.put(MAP_UNION, instanceStatusMap);
          storeReplicaStatusesUpdate.put(MAP_DIFF, Collections.emptyList());
          update.put("storeReplicaStatuses", storeReplicaStatusesUpdate);
          return update;
        });
  }

  /**
   * Write {@link com.linkedin.venice.meta.StoreConfig} equivalent to the meta system store. This is still only invoked
   * by child controllers only.
   */
  public void writeStoreClusterConfig(StoreConfig storeConfig) {
    write(storeConfig.getStoreName(), MetaStoreDataType.STORE_CLUSTER_CONFIG,
        () -> new HashMap<String, String>() {{
          put(KEY_STRING_STORE_NAME, storeConfig.getStoreName());
        }},
        () -> {
          StoreMetaValue value = new StoreMetaValue();
          value.storeClusterConfig = storeConfig.dataModel();
          return value;
        });
  }

  /**
   * This function should be invoked when Venice SN is dropping any partition replica.
   */
  public void deleteStoreReplicaStatus(String clusterName, String storeName, int version, int partitionId, Instance instance) {
    update(storeName, MetaStoreDataType.STORE_REPLICA_STATUSES,
        () -> new HashMap<String, String>() {{
          put(KEY_STRING_STORE_NAME, storeName);
          put(KEY_STRING_CLUSTER_NAME, clusterName);
          put(KEY_STRING_VERSION_NUMBER, Integer.toString(version));
          put(KEY_STRING_PARTITION_ID, Integer.toString(partitionId));
        }},
        () -> {
          // Construct an update
          GenericRecord update = new GenericData.Record(derivedComputeSchema.getTypes().get(0));
          update.put("timestamp", System.currentTimeMillis());
          GenericRecord storeReplicaStatusesUpdate = new GenericData.Record(derivedComputeSchema.getTypes().get(0).getField("storeReplicaStatuses").schema().getTypes().get(2));
          List<CharSequence> deletedReplicas = new ArrayList<>();
          deletedReplicas.add(instance.getUrl(true));
          storeReplicaStatusesUpdate.put(MAP_UNION, Collections.emptyMap());
          storeReplicaStatusesUpdate.put(MAP_DIFF, deletedReplicas);
          update.put("storeReplicaStatuses", storeReplicaStatusesUpdate);
          return update;
        });
  }

  /**
   * Clean up deprecated replica statuses.
   * Currently, it is being used when cleaning up a deprecated version topic, where it guarantees all the
   * partition replicas have been removed from Venice Cluster in {@literal TopicCleanupService}.
   */
  public void deleteStoreReplicaStatus(String clusterName, String storeName, int version, int partitionId) {
    String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    VeniceWriter writer = prepareToWrite(metaStoreName);
    StoreMetaKey key = MetaStoreDataType.STORE_REPLICA_STATUSES.getStoreMetaKey(new HashMap<String, String>() {{
      put(KEY_STRING_STORE_NAME, storeName);
      put(KEY_STRING_CLUSTER_NAME, clusterName);
      put(KEY_STRING_VERSION_NUMBER, Integer.toString(version));
      put(KEY_STRING_PARTITION_ID, Integer.toString(partitionId));
    }});
    writer.delete(key, null);
    writer.flush();
  }

  /**
   * This function should be used only for store deletion scenario.
   * @param metaStoreName
   */
  public void removeMetaStoreWriter(String metaStoreName) {
    VeniceWriter writer = metaStoreWriterMap.get(metaStoreName);
    if (writer != null) {
      /**
       * Free the internal resource without sending any extra messages since the store is going to be deleted.
       */
      closeVeniceWriter(metaStoreName, writer, true);
      metaStoreWriterMap.remove(metaStoreName);
      LOGGER.info("Removed the venice writer for meta store: " + metaStoreName);
    }
  }

  public VeniceWriter getMetaStoreWriter(String metaStoreName) {
    return metaStoreWriterMap.get(metaStoreName);
  }

  private void write(String storeName, MetaStoreDataType dataType,
      Supplier<Map<String, String>> keyStringSupplier, Supplier<StoreMetaValue> valueSupplier) {
    String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    VeniceWriter writer = prepareToWrite(metaStoreName);
    StoreMetaKey key = dataType.getStoreMetaKey(keyStringSupplier.get());
    StoreMetaValue value = valueSupplier.get();
    value.timestamp = System.currentTimeMillis();
    writer.put(key, value, AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.currentProtocolVersion.get());
    writer.flush();
  }

  private void update(String storeName, MetaStoreDataType dataType,
      Supplier<Map<String, String>> keyStringSupplier, Supplier<GenericRecord> updateSupplier) {
    String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    VeniceWriter writer = prepareToWrite(metaStoreName);
    if (derivedComputeSchemaId == -1) {
      /**
       * Fetch the derived compute schema id on demand for integration test since the meta system store is being created
       * during cluster initialization.
       */
      Pair<Integer, Integer> derivedSchemaId =
          zkSharedSchemaRepository.getDerivedSchemaId(VeniceSystemStoreType.META_STORE.getZkSharedStoreName(), derivedComputeSchema.toString());
      if (derivedSchemaId.getFirst() == SchemaData.INVALID_VALUE_SCHEMA_ID) {
        throw new VeniceException("The derived compute schema for meta system store hasn't been registered to Venice yet");
      }
      this.derivedComputeSchemaId = derivedSchemaId.getSecond();
    }
    StoreMetaKey key = dataType.getStoreMetaKey(keyStringSupplier.get());
    GenericRecord update = updateSupplier.get();
    writer.update(key, update, AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.currentProtocolVersion.get(), derivedComputeSchemaId, null);
    writer.flush();
  }

  private VeniceWriter prepareToWrite(String metaStoreName) {
    String rtTopic = Version.composeRealTimeTopic(metaStoreName);
    if (!topicManager.containsTopicAndAllPartitionsAreOnline(rtTopic)) {
      throw new VeniceException("Realtime topic: " + rtTopic + " doesn't exist or some partitions are not online");
    }

    return metaStoreWriterMap.computeIfAbsent(metaStoreName, k ->
        writerFactory.createVeniceWriter(rtTopic, new VeniceAvroKafkaSerializer(
                AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE_KEY.getCurrentProtocolVersionSchema()),
            new VeniceAvroKafkaSerializer(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema()),
            new VeniceAvroKafkaSerializer(derivedComputeSchema), Optional.empty(), new SystemTime()));
  }

  private Map<CharSequence, CharSequence> buildSchemaMap(Collection<SchemaEntry> schemas) {
    return schemas.stream().collect(Collectors.toMap(s -> (Integer.toString(s.getId())), s -> s.getSchema().toString()));
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
     *
     * This check is a best-effort since the race condition is still there between topic check and closing VeniceWriter.
     */
    String rtTopic = Version.composeRealTimeTopic(metaStoreName);
    if (!topicManager.containsTopicAndAllPartitionsAreOnline(rtTopic)) {
      LOGGER.info("RT topic: " + rtTopic + " for meta system store: " + metaStoreName + " doesn't exist, so here "
          + " will only close the internal producer without sending END_OF_SEGMENT control messages");
      veniceWriter.close(false);
    } else {
      veniceWriter.close();
    }
  }

  @Override
  public void close() throws IOException {
    metaStoreWriterMap.forEach((metaStoreName, veniceWriter) -> closeVeniceWriter(metaStoreName, veniceWriter, false));
  }
}
