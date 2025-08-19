package com.linkedin.davinci.client;

import static com.linkedin.davinci.client.AvroGenericDaVinciClient.READ_CHUNK_EXECUTOR;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.davinci.DaVinciBackend;
import com.linkedin.davinci.StoreBackend;
import com.linkedin.davinci.VersionBackend;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.davinci.transformer.TestStringRecordTransformer;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.VeniceProperties;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroGenericDaVinciClientTest {
  private static final String storeName = "test_store";

  public AvroGenericDaVinciClient setUpClientWithRecordTransformer(
      ClientConfig clientConfig,
      DaVinciConfig daVinciConfig) throws NoSuchFieldException, IllegalAccessException {
    return setUpClientWithRecordTransformer(clientConfig, daVinciConfig, false, false, false);
  }

  public AvroGenericDaVinciClient setUpClientWithRecordTransformer(
      ClientConfig clientConfig,
      DaVinciConfig daVinciConfig,
      boolean skipCompatabilityChecks,
      boolean enableDatabaseChecksumVerification,
      boolean subscribeOnDiskPartitionsAutomatically) throws IllegalAccessException, NoSuchFieldException {

    if (daVinciConfig == null) {
      daVinciConfig = new DaVinciConfig();
    }

    DaVinciRecordTransformerConfig recordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .setOutputValueClass(String.class)
            .setOutputValueSchema(Schema.create(Schema.Type.STRING))
            .setSkipCompatibilityChecks(skipCompatabilityChecks)
            .build();
    daVinciConfig.setRecordTransformerConfig(recordTransformerConfig);

    VeniceProperties backendConfig =
        new PropertyBuilder().put(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, enableDatabaseChecksumVerification)
            .put(DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY, subscribeOnDiskPartitionsAutomatically)
            .build();

    AvroGenericDaVinciClient<Integer, String> dvcClient =
        spy(new AvroGenericDaVinciClient<>(daVinciConfig, clientConfig, backendConfig, Optional.empty()));
    doReturn(false).when(dvcClient).isReady();
    doNothing().when(dvcClient).initBackend(any(), any(), any(), any(), any());

    D2ServiceDiscoveryResponse mockDiscoveryResponse = mock(D2ServiceDiscoveryResponse.class);
    when(mockDiscoveryResponse.getCluster()).thenReturn("test_cluster");
    when(mockDiscoveryResponse.getZkAddress()).thenReturn("mock_zk_address");
    when(mockDiscoveryResponse.getKafkaBootstrapServers()).thenReturn("mock_kafka_bootstrap_servers");
    doReturn(mockDiscoveryResponse).when(dvcClient).discoverService();

    DaVinciBackend mockBackend = mock(DaVinciBackend.class);
    when(mockBackend.getSchemaRepository()).thenReturn(mock(ReadOnlySchemaRepository.class));
    when(mockBackend.getStoreOrThrow(anyString())).thenReturn(mock(StoreBackend.class));
    when(mockBackend.getObjectCache()).thenReturn(null);

    ReadOnlySchemaRepository mockSchemaRepository = mock(ReadOnlySchemaRepository.class);
    Schema mockKeySchema = new Schema.Parser().parse("{\"type\": \"int\"}");
    SchemaEntry mockValueSchemaEntry = mock(SchemaEntry.class);
    when(mockValueSchemaEntry.getId()).thenReturn(1);
    when(mockSchemaRepository.getKeySchema(anyString())).thenReturn(new SchemaEntry(1, mockKeySchema));
    when(mockSchemaRepository.getSupersetOrLatestValueSchema(anyString())).thenReturn(mockValueSchemaEntry);
    when(mockBackend.getSchemaRepository()).thenReturn(mockSchemaRepository);

    // Use reflection to set the private static daVinciBackend field
    Field backendField = AvroGenericDaVinciClient.class.getDeclaredField("daVinciBackend");
    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
      backendField.setAccessible(true);
      return null;
    });
    backendField.set(null, new ReferenceCounted<>(mockBackend, ignored -> {}));

    return dvcClient;
  }

  @Test
  public void testPropertyBuilderWithRecordTransformer() {
    String schema = "{\n" + "  \"type\": \"string\"\n" + "}\n";
    VeniceProperties config =
        new PropertyBuilder().put("kafka.admin.class", "name").put("record.transformer.value.schema", schema).build();
    RocksDBServerConfig dbconfig = new RocksDBServerConfig(config);
    Assert.assertEquals(schema, dbconfig.getTransformerValueSchema());
  }

  @Test
  public void testRecordTransformerClientAutomaticSubscriptionException()
      throws NoSuchFieldException, IllegalAccessException {
    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(storeName);
    clientConfig.setSpecificValueClass(String.class);

    assertThrows(
        VeniceClientException.class,
        () -> setUpClientWithRecordTransformer(clientConfig, null, false, false, true));
  }

  @Test
  public void testRecordTransformerClient() throws NoSuchFieldException, IllegalAccessException {
    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(storeName);
    clientConfig.setSpecificValueClass(String.class);

    AvroGenericDaVinciClient dvcClient = setUpClientWithRecordTransformer(clientConfig, null);
    dvcClient.start();
  }

  @Test
  public void testRecordTransformerClientValueClassMismatch() throws NoSuchFieldException, IllegalAccessException {
    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(storeName);
    clientConfig.setSpecificValueClass(Integer.class);

    AvroGenericDaVinciClient dvcClient = setUpClientWithRecordTransformer(clientConfig, null);
    assertThrows(VeniceClientException.class, () -> dvcClient.start());
  }

  @Test
  public void testRecordTransformerWithIngestionIsolation() {
    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(storeName);
    DaVinciConfig daVinciConfig = new DaVinciConfig();
    daVinciConfig.setIsolated(true);

    assertThrows(VeniceClientException.class, () -> setUpClientWithRecordTransformer(clientConfig, daVinciConfig));
  }

  @Test
  public void testRecordTransformerWithChecksumVerificationAndCompatabilityChecks()
      throws NoSuchFieldException, IllegalAccessException {
    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(storeName);

    AvroGenericDaVinciClient dvcClient = setUpClientWithRecordTransformer(clientConfig, null, true, true, false);
    dvcClient.start();
  }

  @Test
  public void testRecordTransformerWithChecksumVerificationAndCompatabilityChecksDisabled()
      throws NoSuchFieldException, IllegalAccessException {
    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(storeName);

    // DaVinciRecordTransformer should gracefully handle config incompatibility for checksum validation
    AvroGenericDaVinciClient dvcClient = setUpClientWithRecordTransformer(clientConfig, null, false, true, false);
    dvcClient.start();
  }

  @Test
  public void testSplit() {
    Set<Integer> intSet = new TreeSet<>();
    intSet.addAll(Arrays.asList(new Integer[] { 1, 2, 3, 4, 5, 6 }));
    List<List<Integer>> splits = AvroGenericDaVinciClient.split(intSet, 4);
    Assert.assertEquals(splits.size(), 2);
    Assert.assertEquals(splits.get(0), Arrays.asList(new Integer[] { 1, 2, 3, 4 }));
    Assert.assertEquals(splits.get(1), Arrays.asList(new Integer[] { 5, 6 }));
    splits = AvroGenericDaVinciClient.split(intSet, 2);
    Assert.assertEquals(splits.size(), 3);
    Assert.assertEquals(splits.get(0), Arrays.asList(new Integer[] { 1, 2 }));
    Assert.assertEquals(splits.get(1), Arrays.asList(new Integer[] { 3, 4 }));
    Assert.assertEquals(splits.get(2), Arrays.asList(new Integer[] { 5, 6 }));
  }

  @Test
  public void testBatchGetSplit() throws ExecutionException, InterruptedException {
    Executor readChunkExecutorForLargeRequest =
        Executors.newFixedThreadPool(2, new DaemonThreadFactory("davinci_read_chunk"));
    AvroGenericDaVinciClient<String, String> dvcClient = mock(AvroGenericDaVinciClient.class);
    when(dvcClient.getStoreName()).thenReturn(storeName);

    int largeRequestSplitThreshold = 10;
    DaVinciConfig daVinciConfig = new DaVinciConfig();
    daVinciConfig.setLargeBatchRequestSplitThreshold(largeRequestSplitThreshold);
    when(dvcClient.getDaVinciConfig()).thenReturn(daVinciConfig);

    String testValue = "test_value";
    StoreBackend storeBackend = mock(StoreBackend.class);
    VersionBackend versionBackend = mock(VersionBackend.class);
    when(versionBackend.getSupersetOrLatestValueSchemaId()).thenReturn(1);
    when(versionBackend.getPartition(any())).thenReturn(1);
    when(versionBackend.read(anyInt(), any(), any(), any(), anyInt(), any(), any(), any())).thenReturn(testValue);
    ReferenceCounted<VersionBackend> versionBackendReferenceCounted =
        new ReferenceCounted<>(versionBackend, ignored -> {});
    when(storeBackend.getDaVinciCurrentVersion()).thenReturn(versionBackendReferenceCounted);
    when(dvcClient.getStoreBackend()).thenReturn(storeBackend);

    when(dvcClient.getReadChunkExecutorForLargeRequest()).thenReturn(readChunkExecutorForLargeRequest);

    when(dvcClient.getKeySerializer()).thenReturn(new AvroSerializer<>(Schema.create(Schema.Type.STRING)));
    when(dvcClient.getStoreDeserializerCache()).thenReturn(null);
    when(dvcClient.isPartitionReadyToServe(any(), anyInt())).thenReturn(true);
    when(dvcClient.isPartitionSubscribed(any(), anyInt())).thenReturn(true);
    when(dvcClient.batchGetFromLocalStorage(any())).thenCallRealMethod();

    Set<String> keySet = new HashSet<>();
    keySet.add("key_1");
    keySet.add("key_2");
    Map<String, String> resultMap = dvcClient.batchGetFromLocalStorage(keySet).get();
    assertEquals(resultMap.size(), 2);
    assertEquals(resultMap.get("key_1"), testValue);
    assertEquals(resultMap.get("key_2"), testValue);

    // Increase the reference to avoid counter underflow as mock object always returns the same referenced counted
    // object.
    versionBackendReferenceCounted.retain();

    // Simulate a large request
    Set<String> largeKeySet = new HashSet<>();
    int keyCnt = (int) (largeRequestSplitThreshold * 1.5);
    String keyPrefix = "key_";
    for (int i = 0; i < keyCnt; ++i) {
      largeKeySet.add(keyPrefix + i);
    }
    resultMap = dvcClient.batchGetFromLocalStorage(largeKeySet).get();
    assertEquals(resultMap.size(), keyCnt);
    for (int i = 0; i < keyCnt; ++i) {
      assertEquals(resultMap.get(keyPrefix + i), testValue);
    }
  }

  @Test
  public void constructorTest() {
    DaVinciConfig daVinciConfig = new DaVinciConfig();
    ClientConfig clientConfig = mock(ClientConfig.class);
    VeniceProperties backendConfig = mock(VeniceProperties.class);
    ICProvider icProvider = mock(ICProvider.class);

    AvroGenericDaVinciClient daVinciClient =
        new AvroGenericDaVinciClient(daVinciConfig, clientConfig, backendConfig, Optional.empty(), icProvider, null);

    assertEquals(daVinciClient.getReadChunkExecutorForLargeRequest(), READ_CHUNK_EXECUTOR);

    Executor readChunkExecutor = mock(Executor.class);
    daVinciClient = new AvroGenericDaVinciClient(
        daVinciConfig,
        clientConfig,
        backendConfig,
        Optional.empty(),
        icProvider,
        readChunkExecutor);
    assertEquals(daVinciClient.getReadChunkExecutorForLargeRequest(), readChunkExecutor);

    // Close a not-ready client won't throw exception.
    daVinciClient.close();
  }

  @Test
  public void closeTest() {
    AvroGenericDaVinciClient client = mock(AvroGenericDaVinciClient.class);
    doCallRealMethod().when(client).close();
    doReturn(LogManager.getLogger(AvroGenericDaVinciClient.class)).when(client).getClientLogger();
    doReturn(false).when(client).isReady();
    client.close();
    verify(client, never()).closeInner();
    doReturn(true).when(client).isReady();
    client.close();
    verify(client, times(1)).closeInner();
  }
}
