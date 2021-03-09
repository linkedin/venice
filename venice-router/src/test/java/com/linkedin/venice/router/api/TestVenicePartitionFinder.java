package com.linkedin.venice.router.api;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.TestUtils;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 3/4/16.
 */
public class TestVenicePartitionFinder {
  private static final int NUM_VERSIONS = 10;
  private static final int NUM_PARTITIONS = 16;

  @Test
  public void partitionerShouldFindPartitions() {
    RoutingDataRepository mockDataRepo = Mockito.mock(RoutingDataRepository.class);
    HelixReadOnlyStoreRepository mockMetadataRepo = Mockito.mock(HelixReadOnlyStoreRepository.class);
    String storeName = TestUtils.getUniqueString("store");
    Store store = new ZKStore(storeName, "owner", System.currentTimeMillis(), PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, 1);
    for (int i = 0; i < NUM_VERSIONS; i++) {
      store.increaseVersion(String.valueOf(i));
    }
    VenicePartitioner partitioner = new DefaultVenicePartitioner();
    doReturn(store).when(mockMetadataRepo).getStore(storeName);
    VenicePartitionFinder finder = new VenicePartitionFinder(mockDataRepo, mockMetadataRepo);
    RouterKey key = RouterKey.fromString("mykey");
    doReturn(NUM_PARTITIONS).when(mockDataRepo).getNumberOfPartitions(anyString());

    // test happy path
    for (int i = 1; i <= NUM_VERSIONS; i++) {
      int partitionNum = finder.findPartitionNumber(key, NUM_PARTITIONS, storeName, i);
      Assert.assertEquals(partitionNum, partitioner.getPartitionId(key.getKeyBuffer(), NUM_PARTITIONS));
    }

    String resourceName = storeName + "_v1";
    String partitionName = finder.findPartitionName(resourceName, key);
    Assert.assertEquals(partitionName, resourceName + "_" + partitioner.getPartitionId(key.getKeyBuffer(), NUM_PARTITIONS));

    // test store not exist
    Assert.assertThrows(VeniceException.class,
        () -> finder.findPartitionNumber(key, NUM_PARTITIONS, "STORE_NOT_EXIST", 1));

    // test version not exist
    Assert.assertThrows(VeniceException.class,
        () -> finder.findPartitionNumber(key, NUM_PARTITIONS, storeName, NUM_VERSIONS + 1));
  }
}