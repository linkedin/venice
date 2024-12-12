package com.linkedin.venice.helix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.utils.DataProviderUtils;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HelixCustomizedViewOfflinePushRepositoryTest {
  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testCustomizedViewStoreHandle(boolean enableReplicaStatusHistory) {
    SafeHelixManager manager = mock(SafeHelixManager.class);
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    Store store = mock(Store.class);
    VersionImpl version = new VersionImpl("abc", 1, "jobID");
    version.setPartitionCount(1);
    when(store.getVersion(anyInt())).thenReturn(version);
    when(store.getName()).thenReturn("abc");
    when(store.getCurrentVersion()).thenReturn(1);
    when(storeRepository.getStore(anyString())).thenReturn(store);
    HelixCustomizedViewOfflinePushRepository customizedViewOfflinePushRepository =
        new HelixCustomizedViewOfflinePushRepository(manager, storeRepository, enableReplicaStatusHistory);
    RoutingTableSnapshot routingTableSnapshot = mock(RoutingTableSnapshot.class);
    Collection views = new ArrayList();
    views.add(new CustomizedView("abc_v1"));
    when(routingTableSnapshot.getCustomizeViews()).thenReturn(views);
    when(routingTableSnapshot.getCustomizedStateType()).thenReturn(HelixPartitionState.OFFLINE_PUSH.name());
    Collection instances = new ArrayList();
    instances.add(new LiveInstance("host_1234"));
    when(routingTableSnapshot.getLiveInstances()).thenReturn(instances);
    customizedViewOfflinePushRepository.onCustomizedViewDataChange(routingTableSnapshot);
    verify(storeRepository, times(1)).getStore(any());
    HelixCustomizedViewOfflinePushRepository.StoreChangeListener storeChangeListener =
        customizedViewOfflinePushRepository.new StoreChangeListener();
    storeChangeListener.handleStoreCreated(store);
    storeChangeListener.handleStoreChanged(store);
    Assert
        .assertEquals(customizedViewOfflinePushRepository.getResourceToPartitionCountMap().get("abc_v1").intValue(), 1);
    storeChangeListener.handleStoreDeleted("abc");
    Assert.assertTrue(customizedViewOfflinePushRepository.getResourceToPartitionCountMap().isEmpty());
  }
}
