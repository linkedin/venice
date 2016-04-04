package com.linkedin.venice.router.api;

import com.linkedin.venice.meta.RoutingDataRepository;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 3/4/16.
 */
public class TestVenicePartitionFinder {

  @Test
  public void partitionerShouldFindPartitions() throws InterruptedException {
    RoutingDataRepository mockDataRepo = Mockito.mock(RoutingDataRepository.class);
    doReturn(10).when(mockDataRepo).getNumberOfPartitions(anyString());
    VenicePartitionFinder finder = new VenicePartitionFinder(mockDataRepo);

    String store = "mystore_v2";
    RouterKey key = RouterKey.fromString("mykey");
    String partition = finder.findPartitionName(store, key);

    Assert.assertEquals(partition, store+"_5");
  }

}
