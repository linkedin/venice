package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigConstants.CONTROLLER_DEFAULT_HELIX_RESOURCE_CAPACITY_KEY;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controller.helix.HelixCapacityConfig;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.RESTConfig;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestZkHelixAdminClient {
  private ZkHelixAdminClient zkHelixAdminClient;
  private HelixAdmin mockHelixAdmin;
  private ConfigAccessor mockHelixConfigAccessor;
  private VeniceControllerMultiClusterConfig mockMultiClusterConfigs;
  private static final String VENICE_CONTROLLER_CLUSTER = "venice-controller-cluster";

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
        .addResource(any(), any(), anyInt(), any(), any(), any());

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

  @Test
  public void testUndefinedRebalancePreferenceAndCapacityKeys() {
    when(zkHelixAdminClient.isVeniceControllerClusterCreated()).thenReturn(false);
    when(mockHelixAdmin.addCluster(VENICE_CONTROLLER_CLUSTER, false)).thenReturn(true);

    doAnswer(invocation -> {
      String controllerClusterName = invocation.getArgument(0);
      ClusterConfig helixClusterConfig = invocation.getArgument(1);

      assertEquals(controllerClusterName, VENICE_CONTROLLER_CLUSTER);

      // When you don't specify rebalance preferences, it will use Helix's default settings
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> globalRebalancePreference =
          helixClusterConfig.getGlobalRebalancePreference();
      assertEquals((int) globalRebalancePreference.get(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS), 1);
      assertEquals((int) globalRebalancePreference.get(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT), 1);
      assertEquals(
          (int) globalRebalancePreference.get(ClusterConfig.GlobalRebalancePreferenceKey.FORCE_BASELINE_CONVERGE),
          0);

      List<String> instanceCapacityKeys = helixClusterConfig.getInstanceCapacityKeys();
      assertEquals(instanceCapacityKeys.size(), 0);

      Map<String, Integer> defaultInstanceCapacityMap = helixClusterConfig.getDefaultInstanceCapacityMap();
      assertEquals(defaultInstanceCapacityMap.size(), 0);

      Map<String, Integer> defaultPartitionWeightMap = helixClusterConfig.getDefaultPartitionWeightMap();
      assertEquals(defaultPartitionWeightMap.size(), 0);
      return null;
    }).when(zkHelixAdminClient).updateClusterConfigs(any(), any());

    doCallRealMethod().when(zkHelixAdminClient).createVeniceControllerCluster();
    zkHelixAdminClient.createVeniceControllerCluster();
  }

  @Test
  public void testPartiallyDefinedRebalancePreferenceOnlyForceBaselineConvergence() {
    when(zkHelixAdminClient.isVeniceControllerClusterCreated()).thenReturn(false);
    when(mockHelixAdmin.addCluster(VENICE_CONTROLLER_CLUSTER, false)).thenReturn(true);

    int helixRebalancePreferenceForceBaselineConverge = 1;
    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> helixGlobalRebalancePreference = Collections.singletonMap(
        ClusterConfig.GlobalRebalancePreferenceKey.FORCE_BASELINE_CONVERGE,
        helixRebalancePreferenceForceBaselineConverge);
    doReturn(helixGlobalRebalancePreference).when(mockMultiClusterConfigs).getHelixGlobalRebalancePreference();

    doAnswer(invocation -> {
      String controllerClusterName = invocation.getArgument(0);
      ClusterConfig helixClusterConfig = invocation.getArgument(1);

      assertEquals(controllerClusterName, VENICE_CONTROLLER_CLUSTER);

      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> globalRebalancePreference =
          helixClusterConfig.getGlobalRebalancePreference();
      assertFalse(globalRebalancePreference.containsKey(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS));
      assertFalse(globalRebalancePreference.containsKey(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT));
      // FORCE_BASELINE_CONVERGE can be defined without setting EVENNESS and LESS_MOVEMENT
      assertEquals(
          (int) globalRebalancePreference.get(ClusterConfig.GlobalRebalancePreferenceKey.FORCE_BASELINE_CONVERGE),
          helixRebalancePreferenceForceBaselineConverge);

      List<String> instanceCapacityKeys = helixClusterConfig.getInstanceCapacityKeys();
      assertEquals(instanceCapacityKeys.size(), 0);

      Map<String, Integer> defaultInstanceCapacityMap = helixClusterConfig.getDefaultInstanceCapacityMap();
      assertEquals(defaultInstanceCapacityMap.size(), 0);

      Map<String, Integer> defaultPartitionWeightMap = helixClusterConfig.getDefaultPartitionWeightMap();
      assertEquals(defaultPartitionWeightMap.size(), 0);
      return null;
    }).when(zkHelixAdminClient).updateClusterConfigs(any(), any());

    doCallRealMethod().when(zkHelixAdminClient).createVeniceControllerCluster();
    zkHelixAdminClient.createVeniceControllerCluster();
  }

  @Test
  public void testRebalancePreferenceWithoutCapacityKeysDefined() {
    when(zkHelixAdminClient.isVeniceControllerClusterCreated()).thenReturn(false);
    when(mockHelixAdmin.addCluster(VENICE_CONTROLLER_CLUSTER, false)).thenReturn(true);

    int helixRebalancePreferenceEvenness = 10;
    int helixRebalancePreferenceLessMovement = 1;
    int helixRebalancePreferenceForceBaselineConverge = 1;

    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> helixGlobalRebalancePreference = new HashMap<>();
    helixGlobalRebalancePreference
        .put(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, helixRebalancePreferenceEvenness);
    helixGlobalRebalancePreference
        .put(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, helixRebalancePreferenceLessMovement);
    helixGlobalRebalancePreference.put(
        ClusterConfig.GlobalRebalancePreferenceKey.FORCE_BASELINE_CONVERGE,
        helixRebalancePreferenceForceBaselineConverge);

    doReturn(helixGlobalRebalancePreference).when(mockMultiClusterConfigs).getHelixGlobalRebalancePreference();
    doReturn(null).when(mockMultiClusterConfigs).getHelixCapacityConfig();

    // Both defaultInstanceCapacityMap and defaultPartitionWeightMap need to be specified
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
      assertEquals(instanceCapacityKeys.size(), 0);

      Map<String, Integer> defaultInstanceCapacityMap = helixClusterConfig.getDefaultInstanceCapacityMap();
      assertEquals(defaultInstanceCapacityMap.size(), 0);

      Map<String, Integer> defaultPartitionWeightMap = helixClusterConfig.getDefaultPartitionWeightMap();
      assertEquals(defaultPartitionWeightMap.size(), 0);
      return null;
    }).when(zkHelixAdminClient).updateClusterConfigs(any(), any());

    doCallRealMethod().when(zkHelixAdminClient).createVeniceControllerCluster();
    zkHelixAdminClient.createVeniceControllerCluster();
  }

  @Test
  public void testCapacityKeysDefinedWithoutRebalancePreference() {
    when(zkHelixAdminClient.isVeniceControllerClusterCreated()).thenReturn(false);
    when(mockHelixAdmin.addCluster(VENICE_CONTROLLER_CLUSTER, false)).thenReturn(true);

    int helixInstanceCapacity = 10000;
    int helixResourceCapacityWeight = 100;

    List<String> helixInstanceCapacityKeys = Collections.singletonList(CONTROLLER_DEFAULT_HELIX_RESOURCE_CAPACITY_KEY);
    Map<String, Integer> helixDefaultInstanceCapacityMap =
        Collections.singletonMap(CONTROLLER_DEFAULT_HELIX_RESOURCE_CAPACITY_KEY, helixInstanceCapacity);
    Map<String, Integer> helixDefaultPartitionWeightMap =
        Collections.singletonMap(CONTROLLER_DEFAULT_HELIX_RESOURCE_CAPACITY_KEY, helixResourceCapacityWeight);

    HelixCapacityConfig capacityConfig = new HelixCapacityConfig(
        helixInstanceCapacityKeys,
        helixDefaultInstanceCapacityMap,
        helixDefaultPartitionWeightMap);
    doReturn(null).when(mockMultiClusterConfigs).getHelixGlobalRebalancePreference();
    doReturn(capacityConfig).when(mockMultiClusterConfigs).getHelixCapacityConfig();

    doAnswer(invocation -> {
      String controllerClusterName = invocation.getArgument(0);
      ClusterConfig helixClusterConfig = invocation.getArgument(1);

      assertEquals(controllerClusterName, VENICE_CONTROLLER_CLUSTER);
      // When you don't specify rebalance preferences, it will use Helix's default settings
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> globalRebalancePreference =
          helixClusterConfig.getGlobalRebalancePreference();
      assertEquals((int) globalRebalancePreference.get(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS), 1);
      assertEquals((int) globalRebalancePreference.get(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT), 1);
      assertEquals(
          (int) globalRebalancePreference.get(ClusterConfig.GlobalRebalancePreferenceKey.FORCE_BASELINE_CONVERGE),
          0);

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
