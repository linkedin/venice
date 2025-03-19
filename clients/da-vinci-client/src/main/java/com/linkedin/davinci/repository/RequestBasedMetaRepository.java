package com.linkedin.davinci.repository;

import static com.linkedin.venice.client.store.ClientConfig.defaultGenericClientConfig;

import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.client.store.AvroGenericStoreClientImpl;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.D2ServiceDiscovery;
import com.linkedin.venice.client.store.InternalAvroStoreClient;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.metadata.payload.StorePropertiesPayloadRecord;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.systemstore.schemas.StoreClusterConfig;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;


public class RequestBasedMetaRepository extends NativeMetadataRepository {

  // cluster -> client
  private final VeniceConcurrentHashMap<String, D2TransportClient> d2TransportClientMap =
      new VeniceConcurrentHashMap<>();

  // storeName -> T
  VeniceConcurrentHashMap<String, SchemaData> storeSchemaMap = new VeniceConcurrentHashMap<>();

  private final D2TransportClient d2DiscoveryTransportClient;
  private D2ServiceDiscovery d2ServiceDiscovery;

  // Schema Readers
  RouterBackedSchemaReader storePropertiesSchemaReader;
  RouterBackedSchemaReader storeMetaValueSchemaReader;

  // Deserializers
  VeniceConcurrentHashMap<Integer, RecordDeserializer<StorePropertiesPayloadRecord>> storePropertiesDeserializers =
      new VeniceConcurrentHashMap<>();
  VeniceConcurrentHashMap<Integer, RecordDeserializer<StoreMetaValue>> storeMetaValueDeserializers =
      new VeniceConcurrentHashMap<>();

  public RequestBasedMetaRepository(ClientConfig clientConfig, VeniceProperties backendConfig) {
    super(clientConfig, backendConfig);

    // D2 Transport Client
    this.d2ServiceDiscovery = new D2ServiceDiscovery();
    this.d2DiscoveryTransportClient =
        new D2TransportClient(clientConfig.getD2ServiceName(), clientConfig.getD2Client());

    // Schema readers
    this.storePropertiesSchemaReader =
        getRouterBackedSchemaReader(AvroProtocolDefinition.SERVER_STORE_PROPERTIES_PAYLOAD.getSystemStoreName());
    this.storeMetaValueSchemaReader =
        getRouterBackedSchemaReader(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getSystemStoreName());
  }

  @Override
  public void clear() {
    super.clear();

    // Clear cache
    d2TransportClientMap.clear();
    storeSchemaMap.clear();
  }

  @Override
  protected StoreConfig fetchStoreConfigFromRemote(String storeName) {
    // Create StoreConfig from D2
    D2TransportClient d2TransportClient = getD2TransportClient(storeName);

    StoreClusterConfig storeClusterConfig = new StoreClusterConfig();
    storeClusterConfig.setStoreName(storeName);
    storeClusterConfig.setCluster(d2TransportClient.getServiceName());

    return new StoreConfig(storeClusterConfig);
  }

  @Override
  protected Store fetchStoreFromRemote(String storeName, String clusterName) {
    // Fetch store, bypass cache
    StoreMetaValue storeMetaValue = fetchAndCacheStoreMetaValue(storeName);
    StoreProperties storeProperties = storeMetaValue.storeProperties;
    return new ZKStore(storeProperties);
  }

  @Override
  protected SchemaData getSchemaData(String storeName) {
    if (!storeSchemaMap.containsKey(storeName)) {
      // Cache miss
      fetchAndCacheStoreMetaValue(storeName);
    }
    return storeSchemaMap.get(storeName);
  }

  protected StoreMetaValue fetchAndCacheStoreMetaValue(String storeName) {

    // Request params
    int maxValueSchemaId = getMaxValueSchemaId(storeName);
    D2TransportClient d2TransportClient = getD2TransportClient(storeName);
    String requestBasedStorePropertiesURL = QueryAction.STORE_PROPERTIES.toString().toLowerCase() + "/" + storeName;
    if (maxValueSchemaId > SchemaData.UNKNOWN_SCHEMA_ID) {
      requestBasedStorePropertiesURL += "/" + maxValueSchemaId;
    }

    // Request exec
    TransportClientResponse response;
    try {
      response = d2TransportClient.get(requestBasedStorePropertiesURL).get();
    } catch (Exception e) {
      throw new RuntimeException(
          "Encountered exception while trying to send store properties request to " + requestBasedStorePropertiesURL
              + ": " + e);
    }

    // Deserialize StorePropertiesPayloadRecord
    StorePropertiesPayloadRecord record =
        getStorePropertiesDeserializer(response.getSchemaId()).deserialize(response.getBody());

    // Deserialize StoreMetaValue
    StoreMetaValue storeMetaValue =
        getStoreMetaValueDeserializer(record.storeMetaValueSchemaVersion).deserialize(record.getStoreMetaValueAvro());

    // Cache
    cacheStoreSchema(storeName, storeMetaValue);

    return storeMetaValue;
  }

  D2TransportClient getD2TransportClient(String storeName) {
    synchronized (this) {
      // Get cluster for store
      String serverD2ServiceName =
          d2ServiceDiscovery.find(d2DiscoveryTransportClient, storeName, true).getServerD2Service();
      if (d2TransportClientMap.containsKey(serverD2ServiceName)) {
        return d2TransportClientMap.get(serverD2ServiceName);
      }
      D2TransportClient d2TransportClient = new D2TransportClient(serverD2ServiceName, clientConfig.getD2Client());
      d2TransportClientMap.put(serverD2ServiceName, d2TransportClient);
      return d2TransportClient;
    }
  }

  protected int getMaxValueSchemaId(String storeName) {
    if (!schemaMap.containsKey(storeName)) {
      return SchemaData.UNKNOWN_SCHEMA_ID;
    }
    return schemaMap.get(storeName).getMaxValueSchemaId();
  }

  protected void cacheStoreSchema(String storeName, StoreMetaValue storeMetaValue) {
    if (!storeSchemaMap.containsKey(storeName)) {
      // New store
      Map.Entry<CharSequence, CharSequence> keySchemaEntry =
          storeMetaValue.getStoreKeySchemas().getKeySchemaMap().entrySet().iterator().next();
      SchemaData schemaData = new SchemaData(
          storeName,
          new SchemaEntry(Integer.parseInt(keySchemaEntry.getKey().toString()), keySchemaEntry.getValue().toString()));
      storeSchemaMap.put(storeName, schemaData);
    }
    // Store Value Schemas
    for (Map.Entry<CharSequence, CharSequence> entry: storeMetaValue.getStoreValueSchemas()
        .getValueSchemaMap()
        .entrySet()) {
      storeSchemaMap.get(storeName)
          .addValueSchema(new SchemaEntry(Integer.parseInt(entry.getKey().toString()), entry.getValue().toString()));
    }
  }

  private RouterBackedSchemaReader getRouterBackedSchemaReader(String systemStoreName) {

    InternalAvroStoreClient responseSchemaStoreClient = new AvroGenericStoreClientImpl(
        // Create a new D2TransportClient since the other one will be set to point to server d2 after cluster discovery
        new D2TransportClient(
            this.d2DiscoveryTransportClient.getServiceName(),
            this.d2DiscoveryTransportClient.getD2Client()),
        false,
        defaultGenericClientConfig(systemStoreName));

    return new RouterBackedSchemaReader(() -> responseSchemaStoreClient, Optional.empty(), Optional.empty());
  }

  private RecordDeserializer<StorePropertiesPayloadRecord> getStorePropertiesDeserializer(int schemaVersion) {

    return storePropertiesDeserializers.computeIfAbsent(schemaVersion, key -> {
      Schema schema = fetchSchemaByVersion(
          AvroProtocolDefinition.SERVER_STORE_PROPERTIES_PAYLOAD,
          storePropertiesSchemaReader,
          key);
      return FastSerializerDeserializerFactory
          .getFastAvroSpecificDeserializer(schema, StorePropertiesPayloadRecord.class);
    });
  }

  private RecordDeserializer<StoreMetaValue> getStoreMetaValueDeserializer(int schemaVersion) {

    return storeMetaValueDeserializers.computeIfAbsent(schemaVersion, key -> {
      Schema schema =
          fetchSchemaByVersion(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE, storeMetaValueSchemaReader, key);
      return FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(schema, StoreMetaValue.class);
    });
  }

  private Schema fetchSchemaByVersion(
      AvroProtocolDefinition definition,
      RouterBackedSchemaReader schemaReader,
      int version) {

    if (definition.currentProtocolVersion.isPresent() && definition.currentProtocolVersion.get().equals(version)) {
      // Get local schema
      return definition.getCurrentProtocolVersionSchema();
    }

    // Fetch remote schema via router
    return schemaReader.getValueSchema(version);
  }
}
