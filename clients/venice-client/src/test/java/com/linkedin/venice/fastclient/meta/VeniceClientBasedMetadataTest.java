package com.linkedin.venice.fastclient.meta;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_CLUSTER_NAME;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_PARTITION_ID;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_SCHEMA_ID;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_VERSION_NUMBER;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.stats.ClusterStats;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreClusterConfig;
import com.linkedin.venice.systemstore.schemas.StoreKeySchemas;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StorePartitionerConfig;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.systemstore.schemas.StoreReplicaStatus;
import com.linkedin.venice.systemstore.schemas.StoreValueSchema;
import com.linkedin.venice.systemstore.schemas.StoreValueSchemas;
import com.linkedin.venice.systemstore.schemas.StoreVersion;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.Test;


public class VeniceClientBasedMetadataTest {
  private static final String CLUSTER_NAME = "test-cluster";
  private static final String REPLICA_NAME = "host1";
  private static final String KEY_SCHEMA = "\"string\"";
  private static final String VALUE_SCHEMA1 = "\"string\"";
  private static final String VALUE_SCHEMA2 = "\"long\"";

  @Test
  public void testMetadata() throws ExecutionException, InterruptedException {
    String storeName = "testStore";
    ClientConfig clientConfig = getBasicMockClientConfig(storeName);
    AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> metaStoreThinClient =
        getBasicMockMetaStoreThinClient(storeName);
    VeniceClientBasedMetadata veniceClientBasedMetadata =
        new ThinClientBasedMetadata(clientConfig, metaStoreThinClient);
    veniceClientBasedMetadata.start();
    assertEquals(veniceClientBasedMetadata.getStoreName(), storeName);
    assertEquals(veniceClientBasedMetadata.getCurrentStoreVersion(), 1);
    List<String> replicas = veniceClientBasedMetadata.getReplicas(1, 0);
    assertEquals(replicas.size(), 1);
    assertEquals(replicas.iterator().next(), REPLICA_NAME);
    assertEquals(veniceClientBasedMetadata.getKeySchema().toString(), KEY_SCHEMA);
    assertEquals(veniceClientBasedMetadata.getValueSchema(1).toString(), VALUE_SCHEMA1);
    assertEquals(veniceClientBasedMetadata.getValueSchema(2).toString(), VALUE_SCHEMA2);
    assertEquals(veniceClientBasedMetadata.getLatestValueSchemaId(), Integer.valueOf(2));
    assertEquals(veniceClientBasedMetadata.getLatestValueSchema().toString(), VALUE_SCHEMA2);
  }

  private ClientConfig getBasicMockClientConfig(String storeName) {
    ClientConfig clientConfig = mock(ClientConfig.class);
    ClusterStats clusterStats = new ClusterStats(new MetricsRepository(), storeName);
    doReturn(ClientRoutingStrategyType.LEAST_LOADED).when(clientConfig).getClientRoutingStrategyType();
    doReturn(1L).when(clientConfig).getMetadataRefreshIntervalInSeconds();
    doReturn(storeName).when(clientConfig).getStoreName();
    doReturn(clusterStats).when(clientConfig).getClusterStats();
    return clientConfig;
  }

  private AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> getBasicMockMetaStoreThinClient(String storeName)
      throws ExecutionException, InterruptedException {
    AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> metaStoreThinClient = mock(AvroSpecificStoreClient.class);
    StoreMetaValue storeConfigValue = new StoreMetaValue();
    storeConfigValue.setStoreClusterConfig(new StoreClusterConfig(CLUSTER_NAME, false, null, null, storeName));
    CompletableFuture<StoreMetaValue> storeConfigFuture = mock(CompletableFuture.class);
    doReturn(storeConfigValue).when(storeConfigFuture).get();
    doReturn(storeConfigFuture).when(metaStoreThinClient)
        .get(
            MetaStoreDataType.STORE_CLUSTER_CONFIG
                .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName)));
    // Store props mocks
    CompletableFuture<StoreMetaValue> storePropFuture = mock(CompletableFuture.class);
    StoreMetaValue storePropValue = new StoreMetaValue();
    StoreProperties storeProperties = new StoreProperties();
    storeProperties.setName(storeName);

    StorePartitionerConfig storePartitionerConfig = new StorePartitionerConfig();
    storePartitionerConfig.setPartitionerClass(DefaultVenicePartitioner.class.getName());
    storePartitionerConfig.setAmplificationFactor(1);
    storePartitionerConfig.setPartitionerParams(Collections.emptyMap());

    StoreVersion storeVersion1 = new StoreVersion();
    storeVersion1.setStoreName(storeName);
    storeVersion1.setNumber(1);
    storeVersion1.setPushJobId("test-push-1");
    storeVersion1.setReplicationFactor(1);
    storeVersion1.setPartitionCount(100);
    storeVersion1.setPartitionerConfig(storePartitionerConfig);
    storeVersion1.setCompressionStrategy(CompressionStrategy.NO_OP.getValue());

    StoreVersion storeVersion2 = new StoreVersion();
    storeVersion2.setStoreName(storeName);
    storeVersion2.setNumber(2);
    storeVersion2.setPushJobId("test-push-2");
    storeVersion2.setReplicationFactor(1);
    storeVersion2.setPartitionCount(100);
    storeVersion2.setPartitionerConfig(storePartitionerConfig);
    storeVersion2.setCompressionStrategy(CompressionStrategy.NO_OP.getValue());

    storeProperties.setVersions(Arrays.asList(storeVersion1, storeVersion2));
    storeProperties.setCurrentVersion(1);
    storeProperties.setLatestSuperSetValueSchemaId(SchemaData.INVALID_VALUE_SCHEMA_ID);
    storePropValue.setStoreProperties(storeProperties);
    doReturn(storePropValue).when(storePropFuture).get();
    doReturn(storePropFuture).when(metaStoreThinClient)
        .get(MetaStoreDataType.STORE_PROPERTIES.getStoreMetaKey(new HashMap<String, String>() {
          {
            put(KEY_STRING_STORE_NAME, storeName);
            put(KEY_STRING_CLUSTER_NAME, CLUSTER_NAME);
          }
        }));
    // Store key schema mocks
    CompletableFuture<StoreMetaValue> keySchemaFuture = mock(CompletableFuture.class);
    StoreMetaValue keySchemaValue = new StoreMetaValue();
    Map<CharSequence, CharSequence> keySchemaMap = new HashMap<>();
    keySchemaMap.put("1", KEY_SCHEMA);
    keySchemaValue.setStoreKeySchemas(new StoreKeySchemas(keySchemaMap));
    doReturn(keySchemaValue).when(keySchemaFuture).get();
    doReturn(keySchemaFuture).when(metaStoreThinClient)
        .get(
            MetaStoreDataType.STORE_KEY_SCHEMAS
                .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName)));
    // Store value schema mocks
    CompletableFuture<StoreMetaValue> valueSchemaFuture = mock(CompletableFuture.class);
    StoreMetaValue valueSchemaValue = new StoreMetaValue();
    Map<CharSequence, CharSequence> valueSchemaMap = new HashMap<>();
    valueSchemaMap.put("1", "");
    valueSchemaMap.put("2", "");
    valueSchemaValue.setStoreValueSchemas(new StoreValueSchemas(valueSchemaMap));
    doReturn(valueSchemaValue).when(valueSchemaFuture).get();
    doReturn(valueSchemaFuture).when(metaStoreThinClient)
        .get(
            MetaStoreDataType.STORE_VALUE_SCHEMAS
                .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName)));
    CompletableFuture<StoreMetaValue> indiValueSchemaFuture = mock(CompletableFuture.class);
    StoreMetaValue indiSchemaValue = new StoreMetaValue();
    indiSchemaValue.setStoreValueSchema(new StoreValueSchema(VALUE_SCHEMA1));
    doReturn(indiSchemaValue).when(indiValueSchemaFuture).get();
    doReturn(indiValueSchemaFuture).when(metaStoreThinClient)
        .get(MetaStoreDataType.STORE_VALUE_SCHEMA.getStoreMetaKey(new HashMap<String, String>() {
          {
            put(KEY_STRING_STORE_NAME, storeName);
            put(KEY_STRING_SCHEMA_ID, "1");
          }
        }));

    CompletableFuture<StoreMetaValue> indiValueSchemaFuture2 = mock(CompletableFuture.class);
    StoreMetaValue indiSchemaValue2 = new StoreMetaValue();
    indiSchemaValue2.setStoreValueSchema(new StoreValueSchema(VALUE_SCHEMA2));
    doReturn(indiSchemaValue2).when(indiValueSchemaFuture2).get();
    doReturn(indiValueSchemaFuture2).when(metaStoreThinClient)
        .get(MetaStoreDataType.STORE_VALUE_SCHEMA.getStoreMetaKey(new HashMap<String, String>() {
          {
            put(KEY_STRING_STORE_NAME, storeName);
            put(KEY_STRING_SCHEMA_ID, "2");
          }
        }));

    // Ready to serve replica mocks
    for (int version = 1; version <= 2; version++) {
      for (int partition = 0; partition < 100; partition++) {
        int finalPartition = partition;
        int finalVersion = version;

        CompletableFuture<StoreMetaValue> replicaStatusFuture = mock(CompletableFuture.class);
        StoreMetaValue replicaStatusValue = new StoreMetaValue();
        Map<CharSequence, StoreReplicaStatus> replicaStatusMap = new HashMap<>();
        StoreReplicaStatus storeReplicaStatus = new StoreReplicaStatus();
        storeReplicaStatus.setStatus(COMPLETED.getValue());
        replicaStatusMap.put(REPLICA_NAME, storeReplicaStatus);
        replicaStatusValue.setStoreReplicaStatuses(replicaStatusMap);
        doReturn(replicaStatusValue).when(replicaStatusFuture).get();

        doReturn(replicaStatusFuture).when(metaStoreThinClient)
            .get(MetaStoreDataType.STORE_REPLICA_STATUSES.getStoreMetaKey(new HashMap<String, String>() {
              {
                put(KEY_STRING_STORE_NAME, storeName);
                put(KEY_STRING_CLUSTER_NAME, CLUSTER_NAME);
                put(KEY_STRING_VERSION_NUMBER, Integer.toString(finalVersion));
                put(KEY_STRING_PARTITION_ID, Integer.toString(finalPartition));
              }
            }));
      }
    }
    return metaStoreThinClient;
  }
}
