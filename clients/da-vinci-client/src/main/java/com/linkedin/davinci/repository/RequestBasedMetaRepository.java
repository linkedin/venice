package com.linkedin.davinci.repository;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.D2ServiceDiscovery;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.metadata.response.StorePropertiesResponseRecord;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.systemstore.schemas.StoreClusterConfig;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import org.apache.avro.Schema;


public class RequestBasedMetaRepository extends NativeMetadataRepository {
  // storeName -> storePropertiesResponseRecord
  private final Map<String, StorePropertiesResponseRecord> storePropertiesRecordMap = new VeniceConcurrentHashMap<>();

  private final Map<String, D2TransportClient> d2TransportClientMap = new VeniceConcurrentHashMap<>(); // cluster ->
                                                                                                       // client
  private final D2TransportClient d2DiscoveryTransportClient;
  private D2ServiceDiscovery d2ServiceDiscovery;

  public RequestBasedMetaRepository(ClientConfig clientConfig, VeniceProperties backendConfig) {
    super(clientConfig, backendConfig);
    this.d2ServiceDiscovery = new D2ServiceDiscovery();
    this.d2DiscoveryTransportClient =
        new D2TransportClient(clientConfig.getD2ServiceName(), clientConfig.getD2Client());
  }

  @Override
  public void clear() {
    super.clear();
    storePropertiesRecordMap.clear();
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
    StorePropertiesResponseRecord record = fetchAndCacheStorePropertiesResponseRecord(storeName);
    StoreProperties storeProperties = record.storeMetaValue.storeProperties;
    return new ZKStore(storeProperties);
  }

  @Override
  protected StoreMetaValue getStoreMetaValue(String storeName, StoreMetaKey key) { // TODO PRANAV what is key for?
    System.out.println("HERE HERE HERE getStoreMetaValue - Key: " + key);
    if (storePropertiesRecordMap.containsKey(storeName)) {
      return storePropertiesRecordMap.get(storeName).storeMetaValue;
    }

    // Cache miss, fetch store
    StorePropertiesResponseRecord record = fetchAndCacheStorePropertiesResponseRecord(storeName);
    return record.storeMetaValue;
  }

  private StorePropertiesResponseRecord fetchAndCacheStorePropertiesResponseRecord(String storeName) {

    // Request
    // TODO PRANAV lastKnownSchemaId param
    D2TransportClient d2TransportClient = getD2TransportClient(storeName);
    String requestBasedStorePropertiesURL = QueryAction.STORE_PROPERTIES.toString().toLowerCase() + "/" + storeName;
    TransportClientResponse response;
    try {
      response = d2TransportClient.get(requestBasedStorePropertiesURL).get();
    } catch (Exception e) {
      throw new RuntimeException(
          "Encountered exception while trying to send store properties request to: " + requestBasedStorePropertiesURL
              + "/" + requestBasedStorePropertiesURL,
          e);
    }

    // Deserialize
    Schema writerSchema = StorePropertiesResponseRecord.SCHEMA$;
    RecordDeserializer<StorePropertiesResponseRecord> recordDeserializer = FastSerializerDeserializerFactory
        .getFastAvroSpecificDeserializer(writerSchema, StorePropertiesResponseRecord.class);
    StorePropertiesResponseRecord record = recordDeserializer.deserialize(response.getBody());

    // Cache
    storePropertiesRecordMap.put(storeName, record);

    return record;
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
}
