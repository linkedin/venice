package com.linkedin.venice.store;

import static org.mockito.Mockito.mock;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.helix.StoreJSONSerializer;
import com.linkedin.venice.helix.SystemStoreJSONSerializer;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ZKStore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class StoreStateReaderTest {
  private static final StoreJSONSerializer STORE_SERIALIZER = new StoreJSONSerializer();
  private static final SystemStoreJSONSerializer SYSTEM_STORE_SERIALIZER = new SystemStoreJSONSerializer();

  private static final String storeName = "test_store";

  @Test
  public void testStoreStateReader() throws IOException, ExecutionException, InterruptedException {
    Store testStore = getStore();
    AbstractAvroStoreClient storeClient = getMockStoreClient(testStore);
    try (StoreStateReader storeStateReader = StoreStateReader.getInstance(storeClient)) {
      Store storeFromResponse = storeStateReader.getStore();
      Assert.assertEquals(
          STORE_SERIALIZER.serialize(storeFromResponse, null),
          STORE_SERIALIZER.serialize(testStore, null));
    }
  }

  @Test
  public void testStoreStateReaderAPINoResponse() throws ExecutionException, InterruptedException {
    AbstractAvroStoreClient storeClient = getMockStoreClientWithNoResponse();
    try (StoreStateReader storeStateReader = StoreStateReader.getInstance(storeClient)) {
      Assert.assertThrows(VeniceClientException.class, () -> storeStateReader.getStore());
    }
  }

  @Test
  public void testStoreStateReaderAPIBadResponse() throws ExecutionException, InterruptedException {
    AbstractAvroStoreClient storeClient = getMockStoreClientWithBadResponse();
    try (StoreStateReader storeStateReader = StoreStateReader.getInstance(storeClient)) {
      Assert.assertThrows(VeniceClientException.class, () -> storeStateReader.getStore());
    }
  }

  private ZKStore getStore() {
    int partitionCount = 10;
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();

    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
        1000,
        1000,
        -1,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);

    ZKStore store = new ZKStore(
        storeName,
        "test-owner",
        System.currentTimeMillis(),
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1,
        1000,
        1000,
        hybridStoreConfig,
        partitionerConfig,
        3);
    store.setPartitionCount(partitionCount);
    Version version = new VersionImpl(storeName, 1, "test-job-id");
    version.setPartitionCount(partitionCount);
    store.setVersions(Collections.singletonList(version));

    return store;
  }

  private AbstractAvroStoreClient getMockStoreClient(Store store)
      throws IOException, ExecutionException, InterruptedException {
    AbstractAvroStoreClient storeClient = mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(storeClient).getStoreName();

    CompletableFuture<byte[]> storeStateFuture = mock(CompletableFuture.class);
    Mockito.doReturn(STORE_SERIALIZER.serialize(store, null)).when(storeStateFuture).get();
    Mockito.doReturn(storeStateFuture).when(storeClient).getRaw("store_state/" + storeName);

    return storeClient;
  }

  private AbstractAvroStoreClient getMockStoreClientWithNoResponse() throws ExecutionException, InterruptedException {
    AbstractAvroStoreClient storeClient = mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(storeClient).getStoreName();

    CompletableFuture<byte[]> storeStateFuture = mock(CompletableFuture.class);
    Mockito.doReturn(null).when(storeStateFuture).get();
    Mockito.doReturn(storeStateFuture).when(storeClient).getRaw("store_state/" + storeName);

    return storeClient;
  }

  private AbstractAvroStoreClient getMockStoreClientWithBadResponse() throws ExecutionException, InterruptedException {
    AbstractAvroStoreClient storeClient = mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(storeClient).getStoreName();

    CompletableFuture<byte[]> storeStateFuture = mock(CompletableFuture.class);
    Mockito.doReturn("TEST".getBytes(StandardCharsets.UTF_8)).when(storeStateFuture).get();
    Mockito.doReturn(storeStateFuture).when(storeClient).getRaw("store_state/" + storeName);

    return storeClient;
  }
}
