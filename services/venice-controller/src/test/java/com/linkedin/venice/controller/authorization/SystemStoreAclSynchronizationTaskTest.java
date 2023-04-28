package com.linkedin.venice.controller.authorization;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.authorization.AceEntry;
import com.linkedin.venice.authorization.AclBinding;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.authorization.Method;
import com.linkedin.venice.authorization.Permission;
import com.linkedin.venice.authorization.Principal;
import com.linkedin.venice.authorization.Resource;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SystemStoreAclSynchronizationTaskTest {
  private final static long SYNCHRONIZATION_CYCLE_DELAY = 50;

  private final ExecutorService executorService = Executors.newSingleThreadExecutor();
  private final List<Store> allStores = new ArrayList<>();
  private final List<String> clustersLeaderOf = new ArrayList<>();
  private final static String defaultCluster = "test-cluster1";
  private AuthorizerService authorizerService;
  private VeniceParentHelixAdmin veniceParentHelixAdmin;

  @BeforeMethod
  public void setUp() {
    authorizerService = mock(AuthorizerService.class);
    veniceParentHelixAdmin = mock(VeniceParentHelixAdmin.class);
    allStores.clear();
    clustersLeaderOf.clear();
    // Adding the zk shared system store objects to the store list to ensure they are properly ignored.
    for (VeniceSystemStoreType veniceSystemStoreType: VeniceSystemStoreType.values()) {
      Store veniceSystemStoreObject = mock(Store.class);
      String systemStoreName = veniceSystemStoreType.getZkSharedStoreNameInCluster(defaultCluster);
      when(veniceSystemStoreObject.getName()).thenReturn(systemStoreName);
      when(veniceParentHelixAdmin.getStore(defaultCluster, systemStoreName)).thenReturn(veniceSystemStoreObject);
      allStores.add(veniceSystemStoreObject);
    }
    clustersLeaderOf.add(defaultCluster);
    when(veniceParentHelixAdmin.getClustersLeaderOf()).thenReturn(clustersLeaderOf);
    when(veniceParentHelixAdmin.getAllStores(defaultCluster)).thenReturn(allStores);
    when(veniceParentHelixAdmin.isLeaderControllerFor(defaultCluster)).thenReturn(true);
  }

  @AfterClass
  public void cleanUp() throws InterruptedException {
    TestUtils.shutdownExecutor(executorService);
  }

  @Test
  public void testSystemStoreAclSynchronizationTask() {
    String storeName1 = "userStore1";
    Store store1 = mock(Store.class);
    when(store1.getName()).thenReturn(storeName1);
    when(store1.isDaVinciPushStatusStoreEnabled()).thenReturn(true);
    when(veniceParentHelixAdmin.getStore(defaultCluster, storeName1)).thenReturn(store1);
    allStores.add(store1);
    String storeName2 = "userStore2";
    Store store2 = mock(Store.class);
    when(store2.getName()).thenReturn(storeName2);
    when(store2.isDaVinciPushStatusStoreEnabled()).thenReturn(false);
    when(veniceParentHelixAdmin.getStore(defaultCluster, storeName2)).thenReturn(store2);
    allStores.add(store2);

    Principal p1 = new Principal("user:user1");
    Principal p2 = new Principal("user:user2");
    Resource r1 = new Resource(storeName1);
    AclBinding aclBinding1 = new AclBinding(r1);
    aclBinding1.addAceEntry(new AceEntry(p1, Method.Read, Permission.ALLOW));
    aclBinding1.addAceEntry(new AceEntry(p2, Method.Read, Permission.ALLOW));
    when(authorizerService.describeAcls(r1)).thenReturn(aclBinding1);
    Resource r2 = new Resource(storeName2);
    AclBinding aclBinding2 = new AclBinding(r2);
    aclBinding2.addAceEntry(new AceEntry(p2, Method.Read, Permission.ALLOW));
    when(authorizerService.describeAcls(r2)).thenReturn(aclBinding2);

    SystemStoreAclSynchronizationTask task =
        new SystemStoreAclSynchronizationTask(authorizerService, veniceParentHelixAdmin, SYNCHRONIZATION_CYCLE_DELAY);
    executorService.submit(task);

    // The synchronization routine has completed at least once.
    verify(veniceParentHelixAdmin, timeout(200).times(2)).getClustersLeaderOf();
    task.close();
    ArgumentCaptor<AclBinding> aclBindingArgumentCaptor = ArgumentCaptor.forClass(AclBinding.class);
    verify(veniceParentHelixAdmin, atLeastOnce())
        .updateSystemStoreAclForStore(eq(defaultCluster), eq(storeName1), aclBindingArgumentCaptor.capture());
    Set<AclBinding> aclBindingSet = new HashSet<>(aclBindingArgumentCaptor.getAllValues());
    Assert.assertEquals(aclBindingSet.size(), 1);
    Assert.assertTrue(
        aclBindingArgumentCaptor.getAllValues()
            .contains(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.generateSystemStoreAclBinding(aclBinding1)));

    aclBindingArgumentCaptor = ArgumentCaptor.forClass(AclBinding.class);
    aclBindingSet = new HashSet<>(aclBindingArgumentCaptor.getAllValues());
    Assert.assertEquals(aclBindingSet.size(), 0);
  }
}
