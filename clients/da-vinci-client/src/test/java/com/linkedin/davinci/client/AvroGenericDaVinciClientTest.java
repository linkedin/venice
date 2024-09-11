package com.linkedin.davinci.client;

import static com.linkedin.davinci.client.AvroGenericDaVinciClient.READ_CHUNK_EXECUTOR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.davinci.StoreBackend;
import com.linkedin.davinci.VersionBackend;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.VeniceProperties;
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
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroGenericDaVinciClientTest {
  @Test
  public void testPropertyBuilderWithRecordTransformer() {
    String schema = "{\n" + "  \"type\": \"string\"\n" + "}\n";
    VeniceProperties config =
        new PropertyBuilder().put("kafka.admin.class", "name").put("record.transformer.value.schema", schema).build();
    RocksDBServerConfig dbconfig = new RocksDBServerConfig(config);
    Assert.assertEquals(schema, dbconfig.getTransformerValueSchema());

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
    when(dvcClient.getStoreName()).thenReturn("test_store");

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

  }
}
