package com.linkedin.davinci.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.StoreBackend;
import com.linkedin.davinci.VersionBackend;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroGenericDaVinciClientTest {
  @Test
  public void testGetValueSchemaIdForComputeRequest() {
    String storeName = "test_store";
    ReadOnlySchemaRepository repo = mock(ReadOnlySchemaRepository.class);

    Schema computeValueSchema = mock(Schema.class);
    String computeValueSchemaString = "compute_value_schema";
    doReturn(computeValueSchemaString).when(computeValueSchema).toString();
    int valueSchemaId = 1;
    doReturn(new SchemaEntry(valueSchemaId, computeValueSchema)).when(repo).getSupersetOrLatestValueSchema(storeName);
    Assert.assertEquals(
        AvroGenericDaVinciClient.getValueSchemaIdForComputeRequest(storeName, computeValueSchema, repo),
        1);

    // mismatch scenario
    Schema latestValueSchema = mock(Schema.class);
    int latestValueSchemaId = 2;
    doReturn(new SchemaEntry(latestValueSchemaId, latestValueSchema)).when(repo)
        .getSupersetOrLatestValueSchema(storeName);
    doReturn(valueSchemaId).when(repo).getValueSchemaId(storeName, computeValueSchemaString);
    Assert.assertEquals(
        AvroGenericDaVinciClient.getValueSchemaIdForComputeRequest(storeName, computeValueSchema, repo),
        1);
    verify(repo).getValueSchemaId(storeName, computeValueSchemaString);
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
}
