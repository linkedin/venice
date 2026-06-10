package com.linkedin.venice.controller;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.StoreInfo;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;


public class FabricControllerClientProviderTest {
  private static final String CLUSTER = "test-cluster";
  private static final String STORE = "test-store";
  private static final int SENTINEL = -1;

  private static StoreResponse successStore(int currentVersion) {
    // StoreInfo#getCurrentVersion is final, so use a real StoreInfo/StoreResponse rather than mocks.
    StoreInfo storeInfo = new StoreInfo();
    storeInfo.setCurrentVersion(currentVersion);
    StoreResponse response = new StoreResponse();
    response.setStore(storeInfo);
    return response;
  }

  private static StoreResponse errorStore() {
    StoreResponse response = new StoreResponse();
    response.setError("boom");
    return response;
  }

  private static FabricControllerClientProvider providerWith(
      VeniceControllerMultiClusterConfig configs,
      Map<String, D2Client> d2Clients) {
    return new FabricControllerClientProvider(configs, Optional.empty(), d2Clients);
  }

  private static VeniceControllerClusterConfig clusterConfig(VeniceControllerMultiClusterConfig configs) {
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    when(configs.getControllerConfig(CLUSTER)).thenReturn(clusterConfig);
    return clusterConfig;
  }

  @Test
  public void testQueryAllRegionsNoRetryAggregatesPerRegion() {
    ControllerClient regionA = mock(ControllerClient.class);
    when(regionA.getStore(STORE)).thenReturn(successStore(3));
    ControllerClient regionB = mock(ControllerClient.class);
    when(regionB.getStore(STORE)).thenReturn(successStore(5));
    Map<String, ControllerClient> clients = new LinkedHashMap<>();
    clients.put("regionA", regionA);
    clients.put("regionB", regionB);

    FabricControllerClientProvider provider = providerWith(mock(VeniceControllerMultiClusterConfig.class), null);
    Map<String, Integer> result = provider
        .queryAllRegions(clients, CLUSTER, 1, c -> c.getStore(STORE), r -> r.getStore().getCurrentVersion(), SENTINEL);

    assertEquals(result.size(), 2);
    assertEquals(result.get("regionA").intValue(), 3);
    assertEquals(result.get("regionB").intValue(), 5);
  }

  @Test
  public void testQueryAllRegionsStoresSentinelOnError() {
    ControllerClient good = mock(ControllerClient.class);
    when(good.getStore(STORE)).thenReturn(successStore(9));
    ControllerClient bad = mock(ControllerClient.class);
    when(bad.getStore(STORE)).thenReturn(errorStore());
    Map<String, ControllerClient> clients = new LinkedHashMap<>();
    clients.put("good", good);
    clients.put("bad", bad);

    FabricControllerClientProvider provider = providerWith(mock(VeniceControllerMultiClusterConfig.class), null);
    Map<String, Integer> result = provider
        .queryAllRegions(clients, CLUSTER, 1, c -> c.getStore(STORE), r -> r.getStore().getCurrentVersion(), SENTINEL);

    assertEquals(result.get("good").intValue(), 9);
    assertEquals(result.get("bad").intValue(), SENTINEL);
  }

  @Test
  public void testQueryAllRegionsWithRetryUsesRetryableRequest() {
    // maxAttempts > 1 exercises the ControllerClient.retryableRequest branch; a successful response returns on the
    // first attempt, so no retry sleep is incurred.
    ControllerClient region = mock(ControllerClient.class);
    when(region.getStore(STORE)).thenReturn(successStore(4));
    Map<String, ControllerClient> clients = Collections.singletonMap("region", region);

    FabricControllerClientProvider provider = providerWith(mock(VeniceControllerMultiClusterConfig.class), null);
    Map<String, Integer> result = provider
        .queryAllRegions(clients, CLUSTER, 3, c -> c.getStore(STORE), r -> r.getStore().getCurrentVersion(), SENTINEL);

    assertEquals(result.get("region").intValue(), 4);
  }

  @Test
  public void testGetControllerClientMapBuildsAndCachesClients() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig clusterConfig = clusterConfig(configs);
    when(clusterConfig.getChildDataCenterControllerUrlMap())
        .thenReturn(Collections.singletonMap("urlRegion", "http://localhost:1234"));
    when(clusterConfig.getChildDataCenterControllerD2Map())
        .thenReturn(Collections.singletonMap("d2Region", "d2-zk-host"));
    when(clusterConfig.getD2ServiceName()).thenReturn("venice-controller");

    Map<String, D2Client> d2Clients = new HashMap<>();
    d2Clients.put("d2Region", mock(D2Client.class));
    FabricControllerClientProvider provider = providerWith(configs, d2Clients);

    Map<String, ControllerClient> map = provider.getControllerClientMap(CLUSTER);
    assertEquals(map.size(), 2);
    assertNotNull(map.get("urlRegion"));
    assertNotNull(map.get("d2Region"));
    // The per-cluster map is cached: the same instance is returned on a second call.
    assertSame(provider.getControllerClientMap(CLUSTER), map);
  }

  @Test
  public void testGetControllerClientMapWithoutInjectedD2Clients() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig clusterConfig = clusterConfig(configs);
    when(clusterConfig.getChildDataCenterControllerUrlMap())
        .thenReturn(Collections.singletonMap("urlRegion", "http://localhost:1234"));
    // Empty D2 map so the d2Clients==null branch is exercised without constructing a ZK-backed D2 client.
    when(clusterConfig.getChildDataCenterControllerD2Map()).thenReturn(Collections.emptyMap());

    FabricControllerClientProvider provider = providerWith(configs, null);
    Map<String, ControllerClient> map = provider.getControllerClientMap(CLUSTER);
    assertEquals(map.size(), 1);
    assertNotNull(map.get("urlRegion"));
  }

  @Test
  public void testGetFabricBuildoutReturnsAllowlistedClient() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig clusterConfig = clusterConfig(configs);
    when(clusterConfig.getChildDataCenterControllerUrlMap())
        .thenReturn(Collections.singletonMap("dc-1", "http://localhost:1234"));
    when(clusterConfig.getChildDataCenterControllerD2Map()).thenReturn(Collections.emptyMap());

    FabricControllerClientProvider provider = providerWith(configs, null);
    ControllerClient client = provider.getFabricBuildoutControllerClient(CLUSTER, "dc-1");
    assertNotNull(client);
    // An allowlisted fabric is served straight from the standard controller-client map.
    assertSame(provider.getControllerClientMap(CLUSTER).get("dc-1"), client);
  }

  @Test
  public void testGetFabricBuildoutBuildsD2ClientForNonAllowlistedFabric() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig clusterConfig = clusterConfig(configs);
    when(clusterConfig.getChildDataCenterControllerUrlMap()).thenReturn(Collections.emptyMap());
    when(clusterConfig.getChildDataCenterControllerD2Map()).thenReturn(Collections.emptyMap());
    when(clusterConfig.getChildControllerD2ZkHost("dc-x")).thenReturn("dc-x-zk");
    when(clusterConfig.getD2ServiceName()).thenReturn("venice-controller");

    Map<String, D2Client> d2Clients = new HashMap<>();
    d2Clients.put("dc-x", mock(D2Client.class));
    FabricControllerClientProvider provider = providerWith(configs, d2Clients);

    ControllerClient client = provider.getFabricBuildoutControllerClient(CLUSTER, "dc-x");
    assertNotNull(client);
    // Cached in the non-allowlist fabric map on subsequent lookups.
    assertSame(provider.getFabricBuildoutControllerClient(CLUSTER, "dc-x"), client);
  }

  @Test
  public void testGetFabricBuildoutBuildsUrlClientForNonAllowlistedFabric() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig clusterConfig = clusterConfig(configs);
    when(clusterConfig.getChildDataCenterControllerUrlMap()).thenReturn(Collections.emptyMap());
    when(clusterConfig.getChildDataCenterControllerD2Map()).thenReturn(Collections.emptyMap());
    when(clusterConfig.getChildControllerD2ZkHost("dc-y")).thenReturn("");
    when(clusterConfig.getD2ServiceName()).thenReturn("venice-controller");
    when(clusterConfig.getChildControllerUrl("dc-y")).thenReturn("http://localhost:5678");

    FabricControllerClientProvider provider = providerWith(configs, null);
    assertNotNull(provider.getFabricBuildoutControllerClient(CLUSTER, "dc-y"));
  }

  @Test
  public void testGetFabricBuildoutThrowsWhenNoConfigForFabric() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig clusterConfig = clusterConfig(configs);
    when(clusterConfig.getChildDataCenterControllerUrlMap()).thenReturn(Collections.emptyMap());
    when(clusterConfig.getChildDataCenterControllerD2Map()).thenReturn(Collections.emptyMap());
    when(clusterConfig.getChildControllerD2ZkHost("dc-z")).thenReturn("");
    when(clusterConfig.getD2ServiceName()).thenReturn("");
    when(clusterConfig.getChildControllerUrl("dc-z")).thenReturn("");

    FabricControllerClientProvider provider = providerWith(configs, null);
    assertThrows(VeniceException.class, () -> provider.getFabricBuildoutControllerClient(CLUSTER, "dc-z"));
  }
}
