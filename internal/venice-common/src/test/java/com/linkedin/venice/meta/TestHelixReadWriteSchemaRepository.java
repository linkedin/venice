package com.linkedin.venice.meta;

import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadWriteSchemaRepository;
import com.linkedin.venice.helix.HelixSchemaAccessor;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.Pair;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestHelixReadWriteSchemaRepository {
  private HelixReadWriteSchemaRepository helixReadWriteSchemaRepository;
  private final HelixSchemaAccessor accessor = Mockito.mock(HelixSchemaAccessor.class);
  private final ReadWriteStoreRepository storeRepository = Mockito.mock(ReadWriteStoreRepository.class);
  private final ZkClient zkClient = Mockito.mock(ZkClient.class);
  private final HelixAdapterSerializer adapter = Mockito.mock(HelixAdapterSerializer.class);
  private final MetaStoreWriter metaStoreWriter = Mockito.mock(MetaStoreWriter.class);

  @BeforeMethod
  void setUp() {
    String clusterName = "testCluster";
    this.helixReadWriteSchemaRepository = new HelixReadWriteSchemaRepository(
        storeRepository,
        zkClient,
        adapter,
        clusterName,
        Optional.of(metaStoreWriter));
    this.helixReadWriteSchemaRepository.setAccessor(accessor);
  }

  @Test
  public void getDerivedSchemaCanonical() {
    String storeName = "test";
    String schemaStr =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    Schema schema = Schema.parse(schemaStr);
    Map<Integer, List<DerivedSchemaEntry>> derivedSchemaEntryMap = new HashMap<>();
    DerivedSchemaEntry entry = new DerivedSchemaEntry(1, 1, schema);
    Mockito.doReturn(true).when(storeRepository).hasStore(Mockito.anyString());
    derivedSchemaEntryMap.put(1, Arrays.asList(entry));
    Mockito.doReturn(Arrays.asList(entry)).when(accessor).getAllDerivedSchemas(Mockito.eq(storeName));
    Pair<Integer, Integer> pair = helixReadWriteSchemaRepository.getDerivedSchemaId(storeName, schemaStr);
    Assert.assertEquals(pair.getSecond().intValue(), 1);
  }
}
