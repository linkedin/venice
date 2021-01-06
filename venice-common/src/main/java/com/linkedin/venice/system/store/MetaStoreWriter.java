package com.linkedin.venice.system.store;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.pushmonitor.ReplicaStatus;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.systemstore.schemas.StoreKeySchemas;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StoreReplicaStatus;
import com.linkedin.venice.systemstore.schemas.StoreValueSchemas;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;


/**
 * This class is to write metadata: store properties/key schema/value schemas/replica statuses to meta system store.
 * So far, only child fabric should write to it and in the future, we may want to support the write from parent fabric,
 * which can be used for the fabric buildup.
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

  public MetaStoreWriter(TopicManager topicManager, VeniceWriterFactory writerFactory) {
    this.topicManager = topicManager;
    this.writerFactory = writerFactory;
  }

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

  public void writeStoreKeySchemas(String clusterName, String storeName, Collection<SchemaEntry> keySchemas) {
    write(storeName, MetaStoreDataType.STORE_KEY_SCHEMAS,
        () -> new HashMap<String, String>() {{ put(KEY_STRING_STORE_NAME, storeName); put(KEY_STRING_CLUSTER_NAME, clusterName);}},
        () -> {
          StoreMetaValue value = new StoreMetaValue();
          StoreKeySchemas storeKeySchemas = new StoreKeySchemas();
          storeKeySchemas.keySchemaMap = buildSchemaMap(keySchemas);
          value.storeKeySchemas = storeKeySchemas;
          return value;
        });
  }

  public void writeStoreValueSchemas(String clusterName, String storeName, Collection<SchemaEntry> valueSchemas) {
    write(storeName, MetaStoreDataType.STORE_VALUE_SCHEMAS,
        () -> new HashMap<String, String>() {{ put(KEY_STRING_STORE_NAME, storeName); put(KEY_STRING_CLUSTER_NAME, clusterName);}},
        () -> {
          StoreMetaValue value = new StoreMetaValue();
          StoreValueSchemas storeValueSchemas = new StoreValueSchemas();
          storeValueSchemas.valueSchemaMap = buildSchemaMap(valueSchemas);
          value.storeValueSchemas = storeValueSchemas;
          return value;
        });
  }

  public void writeStoreReplicaStatuses(String clusterName, String storeName, int version, int partitionId, Collection<ReplicaStatus> replicaStatuses) {
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

  private VeniceWriter prepareToWrite(String metaStoreName) {
    String rtTopic = Version.composeRealTimeTopic(metaStoreName);
    if (!topicManager.containsTopicAndAllPartitionsAreOnline(rtTopic)) {
      throw new VeniceException("Realtime topic: " + rtTopic + " doesn't exist or some partitions are not online");
    }
    return metaStoreWriterMap.computeIfAbsent(metaStoreName, k ->
        writerFactory.createVeniceWriter(rtTopic, new VeniceAvroKafkaSerializer(
                AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE_KEY.getCurrentProtocolVersionSchema()),
            new VeniceAvroKafkaSerializer(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema())));
  }

  private Map<CharSequence, CharSequence> buildSchemaMap(Collection<SchemaEntry> schemas) {
    return schemas.stream().collect(Collectors.toMap(s -> (Integer.toString(s.getId())), s->s.getSchema().toString()));
  }

  @Override
  public void close() throws IOException {
    metaStoreWriterMap.forEach((k, v) -> IOUtils.closeQuietly(v));
  }
}
