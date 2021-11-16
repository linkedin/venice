package com.linkedin.venice.controllerapi;

import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.ZKStore;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestUpdateStoreQueryParams {
  @Test
  public void testDiff() {
    Store store1 = new ZKStore("testStore", "owner", 10, PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_ALL_REPLICAS, 1);
    Store store2 = store1.cloneStore();
    store2.setLeaderFollowerModelEnabled(true);
    StoreInfo storeInfo1 = StoreInfo.fromStore(store1);
    StoreInfo storeInfo2 = StoreInfo.fromStore(store2);
    UpdateStoreQueryParams params1 = new UpdateStoreQueryParams(storeInfo1);
    UpdateStoreQueryParams params2 = new UpdateStoreQueryParams(storeInfo2);
    Assert.assertTrue(params1.isDifferent(params2));
  }
}
