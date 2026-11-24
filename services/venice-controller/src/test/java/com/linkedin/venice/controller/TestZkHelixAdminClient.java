package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigConstants.CONTROLLER_DEFAULT_HELIX_RESOURCE_CAPACITY_KEY;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_HELIX_ADMIN_CLASS;
import static com.linkedin.venice.controller.ZkHelixAdminClient.HELIX_PARTICIPANT_DEREGISTRATION_TIMEOUT_CONFIG;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controller.helix.HelixCapacityConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestZkHelixAdminClient {
  private ZkHelixAdminClient zkHelixAdminClient;
  private HelixAdmin mockHelixAdmin;
  private ConfigAccessor mockHelixConfigAccessor;
  private VeniceControllerMultiClusterConfig mockMultiClusterConfigs;
  private static final String VENICE_CONTROLLER_CLUSTER = "venice-controller-cluster";

  @Test
  public void testDefaultAdminOwnsClientLifecycle() {
    ZkClient zkClient = connectedZkClient();
    ZkHelixAdminClient client = constructClient("", zkClient);
    assertEquals(client.getHelixAdmin().getClass(), ZKHelixAdmin.class);
    client.close();
    verify(zkClient).close();
  }

  @Test
  public void testConfiguredAdminReceivesClientAndConfiguration() {
    ZkClient zkClient = connectedZkClient();
    ZkHelixAdminClient client = constructClient(ConfiguredAdmin.class.getName(), zkClient);
    assertSame(((ConfiguredAdmin) client.getHelixAdmin()).client, zkClient);
    assertTrue(client.isVeniceControllerClusterCreated());
    verify(zkClient).setZkSerializer(any(ZNRecordSerializer.class));
    verify(zkClient).waitUntilConnected(ZkClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
    client.close();
    verify(zkClient).close();
  }

  @DataProvider
  public Object[][] invalidAdminClasses() {
    return new Object[][] { { "not.a.real.HelixAdmin", IllegalArgumentException.class },
        { String.class.getName(), ClassCastException.class },
        { ZKHelixAdmin.class.getName(), IllegalArgumentException.class },
        { FailingAdmin.class.getName(), IllegalStateException.class } };
  }

  @Test(dataProvider = "invalidAdminClasses")
  public void testInvalidAdminFailsWithoutDefaultFallback(String className, Class<?> causeType) {
    ZkClient zkClient = connectedZkClient();
    VeniceException failure = expectThrows(VeniceException.class, () -> constructClient(className, zkClient));
    assertTrue(failure.getMessage().contains("Failed to create configured Helix admin: " + className));
    assertEquals(failure.getCause().getClass(), causeType);
    verify(zkClient).close();
  }

  @Test
  public void testConflictingAdminClassesFailBeforeOpeningClient() {
    VeniceControllerMultiClusterConfig configs = configsFor(ConfiguredAdmin.class.getName());
    when(configs.getClusters()).thenReturn(Collections.singleton("other-cluster"));
    VeniceControllerClusterConfig other = mock(VeniceControllerClusterConfig.class);
    when(other.getProps()).thenReturn(new VeniceProperties());
    when(configs.getControllerConfig("other-cluster")).thenReturn(other);
    MetricsRepository metrics = new MetricsRepository();
    try (MockedStatic<ZkClientFactory> factory = mockStatic(ZkClientFactory.class)) {
      VeniceException failure = expectThrows(VeniceException.class, () -> new ZkHelixAdminClient(configs, metrics));
      assertTrue(failure.getMessage().contains(CONTROLLER_HELIX_ADMIN_CLASS));
      factory.verifyNoInteractions();
    } finally {
      metrics.close();
    }
  }

  @Test
  public void testCleanupFailureDoesNotHideInitializationFailure() {
    ZkClient zkClient = connectedZkClient();
    IllegalStateException closeFailure = new IllegalStateException("close failed");
    doThrow(closeFailure).when(zkClient).close();
    VeniceException failure =
        expectThrows(VeniceException.class, () -> constructClient(FailingAdmin.class.getName(), zkClient));
    assertEquals(failure.getCause().getMessage(), "initialization failed");
    assertEquals(failure.getSuppressed(), new Throwable[] { closeFailure });
  }

  private static ZkClient connectedZkClient() {
    ZkClient zkClient = mock(ZkClient.class);
    when(zkClient.waitUntilConnected(ZkClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)).thenReturn(true);
    return zkClient;
  }

  private static VeniceControllerMultiClusterConfig configsFor(String className) {
    VeniceControllerClusterConfig common = mock(VeniceControllerClusterConfig.class);
    when(common.getProps())
        .thenReturn(new VeniceProperties(Collections.singletonMap(CONTROLLER_HELIX_ADMIN_CLASS, className)));
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    when(configs.getCommonConfig()).thenReturn(common);
    when(configs.getControllerClusterName()).thenReturn(VENICE_CONTROLLER_CLUSTER);
    return configs;
  }

  private static ZkHelixAdminClient constructClient(String className, ZkClient zkClient) {
    VeniceControllerMultiClusterConfig configs = configsFor(className);
    MetricsRepository metrics = new MetricsRepository();
    try (MockedStatic<ZkClientFactory> factory = mockStatic(ZkClientFactory.class)) {
      factory.when(() -> ZkClientFactory.newZkClient(configs.getZkAddress())).thenReturn(zkClient);
      return new ZkHelixAdminClient(configs, metrics);
    } finally {
      metrics.close();
    }
  }

  public static class ConfiguredAdmin extends ZKHelixAdmin {
    private final HelixZkClient client;
    private final String controllerCluster;

    public ConfiguredAdmin(HelixZkClient client, VeniceControllerMultiClusterConfig configs) {
      super(client);
      this.client = client;
      this.controllerCluster = configs.getControllerClusterName();
    }

    @Override
    public List<String> getClusters() {
      return Collections.singletonList(controllerCluster);
    }
  }

  public static class FailingAdmin extends ZKHelixAdmin {
    public FailingAdmin(HelixZkClient client, VeniceControllerMultiClusterConfig configs) {
      super(client);
      throw new IllegalStateException("initialization failed");
    }
  }

  @BeforeMethod
  public void setUp() {
    zkHelixAdminClient = mock(ZkHelixAdminClient.class);
    mockHelixAdmin = mock(HelixAdmin.class);
    mockHelixConfigAccessor = mock(ConfigAccessor.class);
    mockMultiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);

    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
      try {
        Field helixAdminField = ZkHelixAdminClient.class.getDeclaredField("helixAdmin");
        helixAdminField.setAccessible(true);
        helixAdminField.set(zkHelixAdminClient, mockHelixAdmin);

        Field helixConfigAccessorField = ZkHelixAdminClient.class.getDeclaredField("helixConfigAccessor");
        helixConfigAccessorField.setAccessible(true);
        helixConfigAccessorField.set(zkHelixAdminClient, mockHelixConfigAccessor);

        Field multiClusterConfigsField = ZkHelixAdminClient.class.getDeclaredField("multiClusterConfigs");
        multiClusterConfigsField.setAccessible(true);
        multiClusterConfigsField.set(zkHelixAdminClient, mockMultiClusterConfigs);

        Field controllerClusterNameField = ZkHelixAdminClient.class.getDeclaredField("controllerClusterName");
        controllerClusterNameField.setAccessible(true);
        controllerClusterNameField.set(zkHelixAdminClient, VENICE_CONTROLLER_CLUSTER);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      return null;
    });

    // VeniceControllerClusterConfig always provides non-null rebalance preference and capacity (production defaults),
    // so mirror that here for tests that exercise the real createVeniceControllerCluster path.
    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> defaultRebalancePreference = new HashMap<>();
    defaultRebalancePreference.put(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 10);
    defaultRebalancePreference.put(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 1);
    defaultRebalancePreference.put(ClusterConfig.GlobalRebalancePreferenceKey.FORCE_BASELINE_CONVERGE, 0);
    doReturn(defaultRebalancePreference).when(mockMultiClusterConfigs).getHelixGlobalRebalancePreference();

    HelixCapacityConfig defaultCapacityConfig = new HelixCapacityConfig(
        Collections.singletonList(CONTROLLER_DEFAULT_HELIX_RESOURCE_CAPACITY_KEY),
        Collections.singletonMap(CONTROLLER_DEFAULT_HELIX_RESOURCE_CAPACITY_KEY, 10000),
        Collections.singletonMap(CONTROLLER_DEFAULT_HELIX_RESOURCE_CAPACITY_KEY, 100));
    doReturn(defaultCapacityConfig).when(mockMultiClusterConfigs).getHelixCapacityConfig();
  }

  @Test
  public void testAddVeniceStorageClusterToControllerCluster() {
    String clusterName1 = "test-cluster1";
    String clusterName2 = "test-cluster2";

    VeniceControllerClusterConfig mockClusterConfig1 = mock(VeniceControllerClusterConfig.class);
    IdealState mockIdealState1 = mock(IdealState.class);
    doReturn(clusterName1).when(mockClusterConfig1).getClusterName();
    when(mockClusterConfig1.getControllerClusterReplica()).thenReturn(2);
    when(mockClusterConfig1.getControllerResourceInstanceGroupTag()).thenReturn("GENERAL");
    when(mockHelixAdmin.getResourceIdealState(any(), eq(clusterName1))).thenReturn(mockIdealState1);

    VeniceControllerClusterConfig mockClusterConfig2 = mock(VeniceControllerClusterConfig.class);
    IdealState mockIdealState2 = mock(IdealState.class);
    doReturn(clusterName2).when(mockClusterConfig2).getClusterName();
    when(mockClusterConfig2.getControllerClusterReplica()).thenReturn(5);
    when(mockClusterConfig2.getControllerResourceInstanceGroupTag()).thenReturn("SPECIAL");
    when(mockHelixAdmin.getResourceIdealState(any(), eq(clusterName2))).thenReturn(mockIdealState2);

    when(mockMultiClusterConfigs.getControllerConfig(clusterName1)).thenReturn(mockClusterConfig1);
    when(mockMultiClusterConfigs.getControllerConfig(clusterName2)).thenReturn(mockClusterConfig2);

    doCallRealMethod().when(zkHelixAdminClient).addVeniceStorageClusterToControllerCluster(anyString());

    zkHelixAdminClient.addVeniceStorageClusterToControllerCluster(clusterName1);
    verify(mockIdealState1).setReplicas("2");
    verify(mockIdealState1).setMinActiveReplicas(1);
    verify(mockIdealState1).setInstanceGroupTag("GENERAL");

    zkHelixAdminClient.addVeniceStorageClusterToControllerCluster(clusterName2);
    verify(mockIdealState2).setReplicas("5");
    verify(mockIdealState2).setMinActiveReplicas(4);
    verify(mockIdealState2).setInstanceGroupTag("SPECIAL");
  }

  @Test
  public void testAddVeniceStorageClusterToControllerClusterException() {
    String clusterName1 = "test-cluster1";
    String clusterName2 = "test-cluster2";

    doThrow(new RuntimeException("Test exception")).when(mockHelixAdmin)
        .addResource(any(), any(), anyInt(), any(), any());

    doCallRealMethod().when(zkHelixAdminClient).addVeniceStorageClusterToControllerCluster(anyString());
    doReturn(false).when(zkHelixAdminClient).isVeniceStorageClusterInControllerCluster(clusterName1);
    doReturn(true).when(zkHelixAdminClient).isVeniceStorageClusterInControllerCluster(clusterName2);

    Exception e = expectThrows(
        Exception.class,
        () -> zkHelixAdminClient.addVeniceStorageClusterToControllerCluster(clusterName1));
    assertEquals(e.getMessage(), "Test exception");

    // No exception
    zkHelixAdminClient.addVeniceStorageClusterToControllerCluster(clusterName2);

    // Further code never executes in either case
    verify(mockMultiClusterConfigs, never()).getControllerConfig(any());
  }

  @Test
  public void testCreateVeniceControllerCluster() {
    doReturn(true).when(mockHelixAdmin).addCluster(VENICE_CONTROLLER_CLUSTER, false);
    doReturn(true).when(mockMultiClusterConfigs).isControllerClusterHelixCloudEnabled();
    doReturn(600000L).when(mockMultiClusterConfigs).getControllerHelixParticipantDeregistrationTimeoutMs();

    CloudConfig cloudConfig = mock(CloudConfig.class);
    doReturn(cloudConfig).when(mockMultiClusterConfigs).getHelixCloudConfig();

    doCallRealMethod().when(zkHelixAdminClient).createVeniceControllerCluster();

    doAnswer(invocation -> {
      ClusterConfig clusterConfig = invocation.getArgument(1);

      assertEquals(clusterConfig.getClusterName(), VENICE_CONTROLLER_CLUSTER);
      assertTrue(clusterConfig.getRecord().getBooleanField(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, false));
      assertEquals(
          clusterConfig.getRecord().getLongField(HELIX_PARTICIPANT_DEREGISTRATION_TIMEOUT_CONFIG, -1L),
          600000L);
      assertFalse(clusterConfig.isTopologyAwareEnabled());

      return null;
    }).when(zkHelixAdminClient).updateClusterConfigs(eq(VENICE_CONTROLLER_CLUSTER), any(ClusterConfig.class));

    zkHelixAdminClient.createVeniceControllerCluster();

    verify(mockHelixAdmin).addCloudConfig(VENICE_CONTROLLER_CLUSTER, cloudConfig);
  }

  @Test
  public void testCreateVeniceStorageCluster() {
    String clusterName = "testCluster";

    VeniceControllerClusterConfig mockClusterConfig = mock(VeniceControllerClusterConfig.class);
    when(mockMultiClusterConfigs.getControllerConfig(clusterName)).thenReturn(mockClusterConfig);

    doReturn(true).when(mockHelixAdmin).addCluster(clusterName, false);
    doCallRealMethod().when(zkHelixAdminClient).createVeniceStorageCluster(any(), any(), any());

    ClusterConfig helixClusterConfig = mock(ClusterConfig.class);
    zkHelixAdminClient.createVeniceStorageCluster(clusterName, helixClusterConfig, null);

    verify(zkHelixAdminClient).updateClusterConfigs(clusterName, helixClusterConfig);
    verify(mockHelixAdmin, never()).addCloudConfig(any(), any());
    verify(zkHelixAdminClient, never()).updateRESTConfigs(any(), any());

    clearInvocations(zkHelixAdminClient);

    doReturn(true).when(mockClusterConfig).isStorageClusterHelixCloudEnabled();
    CloudConfig cloudConfig = mock(CloudConfig.class);
    doReturn(cloudConfig).when(mockClusterConfig).getHelixCloudConfig();
    zkHelixAdminClient.createVeniceStorageCluster(clusterName, helixClusterConfig, null);

    verify(zkHelixAdminClient).updateClusterConfigs(clusterName, helixClusterConfig);
    verify(mockHelixAdmin).addCloudConfig(clusterName, cloudConfig);
    verify(zkHelixAdminClient, never()).updateRESTConfigs(any(), any());

    clearInvocations(zkHelixAdminClient, mockHelixAdmin);
    doReturn(false).when(mockClusterConfig).isStorageClusterHelixCloudEnabled();

    RESTConfig restConfig = mock(RESTConfig.class);
    zkHelixAdminClient.createVeniceStorageCluster(clusterName, helixClusterConfig, restConfig);

    verify(zkHelixAdminClient).updateClusterConfigs(clusterName, helixClusterConfig);
    verify(mockHelixAdmin, never()).addCloudConfig(any(), any());
    verify(zkHelixAdminClient).updateRESTConfigs(clusterName, restConfig);
  }

  @Test
  public void testCreateVeniceStorageClusterLegacy() {
    String clusterName = "test-storage-cluster";

    VeniceControllerClusterConfig mockClusterConfig = mock(VeniceControllerClusterConfig.class);
    when(mockMultiClusterConfigs.getControllerConfig(clusterName)).thenReturn(mockClusterConfig);
    when(mockClusterConfig.getDelayToRebalanceMS()).thenReturn(1000L);
    when(mockClusterConfig.isServerHelixClusterTopologyAware()).thenReturn(false);
    when(mockClusterConfig.getControllerClusterReplica()).thenReturn(3);

    // The storage cluster does not exist yet, and addCluster succeeds.
    doReturn(Collections.emptyList()).when(mockHelixAdmin).getClusters();
    doReturn(true).when(mockHelixAdmin).addCluster(clusterName, false);

    IdealState mockIdealState = mock(IdealState.class);
    when(mockHelixAdmin.getResourceIdealState(VENICE_CONTROLLER_CLUSTER, clusterName)).thenReturn(mockIdealState);

    doAnswer(invocation -> {
      Map<String, String> props = invocation.getArgument(1);
      assertEquals(props.get(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN), "true");
      assertEquals(props.get(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_TIME.name()), "1000");
      assertEquals(props.get(ClusterConfig.ClusterConfigProperty.PERSIST_BEST_POSSIBLE_ASSIGNMENT.name()), "true");
      assertEquals(props.get(ClusterConfig.ClusterConfigProperty.TOPOLOGY_AWARE_ENABLED.name()), "false");
      return null;
    }).when(mockHelixAdmin).setConfig(any(), any());

    doCallRealMethod().when(zkHelixAdminClient).createVeniceStorageClusterLegacy(clusterName);
    zkHelixAdminClient.createVeniceStorageClusterLegacy(clusterName);

    // Storage cluster + LeaderStandby state model created on the storage helixAdmin.
    verify(mockHelixAdmin)
        .addStateModelDef(eq(clusterName), eq(LeaderStandbySMD.name), any(StateModelDefinition.class));
    // Registered as a resource in the controller cluster with the legacy (non-HAAS) WAGED rebalancer config.
    verify(mockHelixAdmin)
        .addResource(eq(VENICE_CONTROLLER_CLUSTER), eq(clusterName), anyInt(), eq(LeaderStandbySMD.name), any());
    verify(mockIdealState).setReplicas("3");
    verify(mockIdealState).setMinActiveReplicas(2);
    verify(mockIdealState).setRebalancerClassName(WagedRebalancer.class.getName());
    verify(mockHelixAdmin).rebalance(VENICE_CONTROLLER_CLUSTER, clusterName, 3);
  }

  @Test
  public void testCreateVeniceStorageClusterLegacyAlreadyExists() {
    String clusterName = "test-storage-cluster";

    // The cluster already exists, so the method should short-circuit before doing any work.
    doReturn(Collections.singletonList(clusterName)).when(mockHelixAdmin).getClusters();

    doCallRealMethod().when(zkHelixAdminClient).createVeniceStorageClusterLegacy(clusterName);
    zkHelixAdminClient.createVeniceStorageClusterLegacy(clusterName);

    verify(mockHelixAdmin, never()).addCluster(anyString(), eq(false));
    verify(mockHelixAdmin, never()).addResource(any(), any(), anyInt(), any(), any());
    verify(mockMultiClusterConfigs, never()).getControllerConfig(any());
  }

  @Test
  public void testUpdateClusterConfigs() {
    doCallRealMethod().when(zkHelixAdminClient).updateClusterConfigs(anyString(), any());

    String clusterName = "testCluster";
    ClusterConfig clusterConfig = new ClusterConfig(clusterName);

    clusterConfig.getRecord().setBooleanField(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, true);
    clusterConfig.setRebalanceDelayTime(1000);
    clusterConfig.setDelayRebalaceEnabled(true);

    clusterConfig.setPersistBestPossibleAssignment(true);

    String topology = "/zone/rack/host/instance";
    String faultZoneType = "zone";
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology(topology);
    clusterConfig.setFaultZoneType(faultZoneType);

    doAnswer(invocation -> {
      String clusterNameProp = invocation.getArgument(0);
      ClusterConfig clusterProps = invocation.getArgument(1);

      assertEquals(clusterNameProp, clusterName);
      assertEquals(clusterProps.getClusterName(), clusterName);
      Map<String, String> simpleFields = clusterProps.getRecord().getSimpleFields();

      assertEquals(simpleFields.size(), 7);
      assertEquals(simpleFields.get(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN), "true");
      assertEquals(simpleFields.get(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_ENABLED.name()), "true");
      assertEquals(simpleFields.get(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_TIME.name()), "1000");
      assertEquals(
          simpleFields.get(ClusterConfig.ClusterConfigProperty.PERSIST_BEST_POSSIBLE_ASSIGNMENT.name()),
          "true");
      assertEquals(simpleFields.get(ClusterConfig.ClusterConfigProperty.TOPOLOGY_AWARE_ENABLED.name()), "true");
      assertEquals(simpleFields.get(ClusterConfig.ClusterConfigProperty.TOPOLOGY.name()), topology);
      assertEquals(simpleFields.get(ClusterConfig.ClusterConfigProperty.FAULT_ZONE_TYPE.name()), faultZoneType);

      return null;
    }).when(mockHelixConfigAccessor).setClusterConfig(any(), any());

    zkHelixAdminClient.updateClusterConfigs(clusterName, clusterConfig);
  }

  @Test
  public void testUpdateRESTConfigs() {
    doCallRealMethod().when(zkHelixAdminClient).updateRESTConfigs(anyString(), any());

    String clusterName = "testCluster";
    String restUrl = "http://localhost:8080";
    RESTConfig restConfig = new RESTConfig(clusterName);

    restConfig.set(RESTConfig.SimpleFields.CUSTOMIZED_HEALTH_URL, restUrl);
    restConfig.getRecord().setSimpleField("FIELD1", "VALUE1");

    doAnswer(invocation -> {
      String clusterNameProp = invocation.getArgument(0);
      RESTConfig restProps = invocation.getArgument(1);
      Map<String, String> simpleFields = restProps.getRecord().getSimpleFields();

      assertEquals(clusterNameProp, clusterName);
      assertEquals(simpleFields.size(), 2);
      assertEquals(simpleFields.get(RESTConfig.SimpleFields.CUSTOMIZED_HEALTH_URL.name()), restUrl);
      assertEquals(simpleFields.get("FIELD1"), "VALUE1");

      return null;
    }).when(mockHelixConfigAccessor).setRESTConfig(any(), any());

    zkHelixAdminClient.updateRESTConfigs(clusterName, restConfig);
  }

  @Test
  public void testRebalancePreferenceAndCapacityKeys() {
    when(zkHelixAdminClient.isVeniceControllerClusterCreated()).thenReturn(false);
    when(mockHelixAdmin.addCluster(VENICE_CONTROLLER_CLUSTER, false)).thenReturn(true);

    int helixRebalancePreferenceEvenness = 10;
    int helixRebalancePreferenceLessMovement = 1;
    int helixRebalancePreferenceForceBaselineConverge = 1;
    int helixInstanceCapacity = 10000;
    int helixResourceCapacityWeight = 100;

    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> helixGlobalRebalancePreference = new HashMap<>();
    helixGlobalRebalancePreference
        .put(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, helixRebalancePreferenceEvenness);
    helixGlobalRebalancePreference
        .put(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, helixRebalancePreferenceLessMovement);
    helixGlobalRebalancePreference.put(
        ClusterConfig.GlobalRebalancePreferenceKey.FORCE_BASELINE_CONVERGE,
        helixRebalancePreferenceForceBaselineConverge);

    List<String> helixInstanceCapacityKeys = Collections.singletonList(CONTROLLER_DEFAULT_HELIX_RESOURCE_CAPACITY_KEY);
    Map<String, Integer> helixDefaultInstanceCapacityMap =
        Collections.singletonMap(CONTROLLER_DEFAULT_HELIX_RESOURCE_CAPACITY_KEY, helixInstanceCapacity);
    Map<String, Integer> helixDefaultPartitionWeightMap =
        Collections.singletonMap(CONTROLLER_DEFAULT_HELIX_RESOURCE_CAPACITY_KEY, helixResourceCapacityWeight);

    HelixCapacityConfig capacityConfig = new HelixCapacityConfig(
        helixInstanceCapacityKeys,
        helixDefaultInstanceCapacityMap,
        helixDefaultPartitionWeightMap);
    doReturn(helixGlobalRebalancePreference).when(mockMultiClusterConfigs).getHelixGlobalRebalancePreference();
    doReturn(capacityConfig).when(mockMultiClusterConfigs).getHelixCapacityConfig();

    doAnswer(invocation -> {
      String controllerClusterName = invocation.getArgument(0);
      ClusterConfig helixClusterConfig = invocation.getArgument(1);

      assertEquals(controllerClusterName, VENICE_CONTROLLER_CLUSTER);
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> globalRebalancePreference =
          helixClusterConfig.getGlobalRebalancePreference();
      assertEquals(
          (int) globalRebalancePreference.get(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS),
          helixRebalancePreferenceEvenness);
      assertEquals(
          (int) globalRebalancePreference.get(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT),
          helixRebalancePreferenceLessMovement);
      assertEquals(
          (int) globalRebalancePreference.get(ClusterConfig.GlobalRebalancePreferenceKey.FORCE_BASELINE_CONVERGE),
          helixRebalancePreferenceForceBaselineConverge);

      List<String> instanceCapacityKeys = helixClusterConfig.getInstanceCapacityKeys();
      assertEquals(instanceCapacityKeys.size(), 1);

      Map<String, Integer> defaultInstanceCapacityMap = helixClusterConfig.getDefaultInstanceCapacityMap();
      assertEquals(
          (int) defaultInstanceCapacityMap.get(CONTROLLER_DEFAULT_HELIX_RESOURCE_CAPACITY_KEY),
          helixInstanceCapacity);

      Map<String, Integer> defaultPartitionWeightMap = helixClusterConfig.getDefaultPartitionWeightMap();
      assertEquals(
          (int) defaultPartitionWeightMap.get(CONTROLLER_DEFAULT_HELIX_RESOURCE_CAPACITY_KEY),
          helixResourceCapacityWeight);
      return null;
    }).when(zkHelixAdminClient).updateClusterConfigs(any(), any());

    doCallRealMethod().when(zkHelixAdminClient).createVeniceControllerCluster();
    zkHelixAdminClient.createVeniceControllerCluster();
  }
}
