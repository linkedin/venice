package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigConstants.CONTROLLER_DEFAULT_HELIX_RESOURCE_CAPACITY_KEY;
import static com.linkedin.venice.controller.TestVeniceControllerClusterConfig.getBaseSingleRegionProperties;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
  private VeniceControllerClusterConfig mockCommonConfig;
  private static final String VENICE_CONTROLLER_CLUSTER = "venice-controller-cluster";

  @BeforeMethod
  public void setUp() throws NoSuchFieldException, IllegalAccessException {
    zkHelixAdminClient = mock(ZkHelixAdminClient.class);
    mockHelixAdmin = mock(HelixAdmin.class);
    mockHelixConfigAccessor = mock(ConfigAccessor.class);
    mockMultiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    mockCommonConfig = mock(VeniceControllerClusterConfig.class);

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

        Field commonConfigsField = ZkHelixAdminClient.class.getDeclaredField("commonConfig");
        commonConfigsField.setAccessible(true);
        commonConfigsField.set(zkHelixAdminClient, mockCommonConfig);

        Field controllerClusterNameField = ZkHelixAdminClient.class.getDeclaredField("controllerClusterName");
        controllerClusterNameField.setAccessible(true);
        controllerClusterNameField.set(zkHelixAdminClient, VENICE_CONTROLLER_CLUSTER);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      return null;
    });

    doReturn(mockCommonConfig).when(mockMultiClusterConfigs).getCommonConfig();
  }

  @Test
  public void testInstanceGroupTag() {
    String clusterName = "test-cluster";
    IdealState mockIdealState = mock(IdealState.class);
    VeniceControllerClusterConfig mockClusterConfig = mock(VeniceControllerClusterConfig.class);

    when(mockClusterConfig.getControllerResourceInstanceGroupTag()).thenReturn("GENERAL");
    when(mockMultiClusterConfigs.getControllerConfig(clusterName)).thenReturn(mockClusterConfig);
    when(mockHelixAdmin.getResourceIdealState(any(), any())).thenReturn(mockIdealState);

    doCallRealMethod().when(zkHelixAdminClient).addVeniceStorageClusterToControllerCluster(anyString());

    zkHelixAdminClient.addVeniceStorageClusterToControllerCluster(clusterName);

    verify(mockIdealState).setInstanceGroupTag("GENERAL");
  }

  @Test
  public void testCreateVeniceControllerCluster() {
    doReturn(true).when(mockHelixAdmin).addCluster(VENICE_CONTROLLER_CLUSTER, false);
    doReturn(true).when(mockCommonConfig).isControllerClusterHelixCloudEnabled();

    CloudConfig cloudConfig = mock(CloudConfig.class);
    doReturn(cloudConfig).when(mockCommonConfig).getHelixCloudConfig();

    doCallRealMethod().when(zkHelixAdminClient).createVeniceControllerCluster();

    doAnswer(invocation -> {
      ClusterConfig clusterConfig = invocation.getArgument(1);

      assertEquals(clusterConfig.getClusterName(), VENICE_CONTROLLER_CLUSTER);
      assertTrue(clusterConfig.getRecord().getBooleanField(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, false));
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
    // Topology and fault zone type fields are used by CRUSH alg. Helix would apply the constrains on CRUSH alg to
    // choose proper instance to hold the replica.
    clusterConfig.setTopology("/" + HelixUtils.TOPOLOGY_CONSTRAINT);
    clusterConfig.setFaultZoneType(HelixUtils.TOPOLOGY_CONSTRAINT);

    doAnswer(invocation -> {
      String clusterNameProp = invocation.getArgument(0);
      ClusterConfig clusterProps = invocation.getArgument(1);

      assertEquals(clusterNameProp, clusterName);
      assertEquals(clusterProps.getClusterName(), clusterName);
      Map<String, String> simpleFields = clusterProps.getRecord().getSimpleFields();

      assertEquals(simpleFields.size(), 6);
      assertEquals(simpleFields.get(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN), "true");
      assertEquals(simpleFields.get(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_ENABLED.name()), "true");
      assertEquals(simpleFields.get(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_TIME.name()), "1000");
      assertEquals(
          simpleFields.get(ClusterConfig.ClusterConfigProperty.PERSIST_BEST_POSSIBLE_ASSIGNMENT.name()),
          "true");
      assertEquals(
          simpleFields.get(ClusterConfig.ClusterConfigProperty.TOPOLOGY.name()),
          "/" + HelixUtils.TOPOLOGY_CONSTRAINT);
      assertEquals(
          simpleFields.get(ClusterConfig.ClusterConfigProperty.FAULT_ZONE_TYPE.name()),
          HelixUtils.TOPOLOGY_CONSTRAINT);

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
  public void testRebalancePreferenceAndCapacityKeys() throws NoSuchFieldException, IllegalAccessException {
    int helixRebalancePreferenceEvenness = 10;
    int helixRebalancePreferenceLessMovement = 1;
    int helixRebalancePreferenceForceBaselineConverge = 1;
    int helixInstanceCapacity = 10000;
    int helixResourceCapacityWeight = 100;

    when(zkHelixAdminClient.isVeniceControllerClusterCreated()).thenReturn(false);
    when(mockHelixAdmin.addCluster(VENICE_CONTROLLER_CLUSTER, false)).thenReturn(true);

    Properties clusterProperties = getBaseSingleRegionProperties(false);
    clusterProperties.put(ConfigKeys.CONTROLLER_HELIX_REBALANCE_PREFERENCE_EVENNESS, helixRebalancePreferenceEvenness);
    clusterProperties
        .put(ConfigKeys.CONTROLLER_HELIX_REBALANCE_PREFERENCE_LESS_MOVEMENT, helixRebalancePreferenceLessMovement);
    clusterProperties.put(
        ConfigKeys.CONTROLLER_HELIX_REBALANCE_PREFERENCE_FORCE_BASELINE_CONVERGE,
        helixRebalancePreferenceForceBaselineConverge);
    clusterProperties.put(ConfigKeys.CONTROLLER_HELIX_INSTANCE_CAPACITY, helixInstanceCapacity);
    clusterProperties.put(ConfigKeys.CONTROLLER_HELIX_RESOURCE_CAPACITY_WEIGHT, helixResourceCapacityWeight);
    VeniceControllerClusterConfig clusterConfig =
        new VeniceControllerClusterConfig(new VeniceProperties(clusterProperties));

    Field commonConfigsField = ZkHelixAdminClient.class.getDeclaredField("commonConfig");
    commonConfigsField.setAccessible(true);
    commonConfigsField.set(zkHelixAdminClient, clusterConfig);

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
  public void testUndefinedRebalancePreferenceAndCapacityKeys() throws NoSuchFieldException, IllegalAccessException {
    when(zkHelixAdminClient.isVeniceControllerClusterCreated()).thenReturn(false);
    when(mockHelixAdmin.addCluster(VENICE_CONTROLLER_CLUSTER, false)).thenReturn(true);

    Properties clusterProperties = getBaseSingleRegionProperties(false);
    VeniceControllerClusterConfig clusterConfig =
        new VeniceControllerClusterConfig(new VeniceProperties(clusterProperties));

    Field commonConfigsField = ZkHelixAdminClient.class.getDeclaredField("commonConfig");
    commonConfigsField.setAccessible(true);
    commonConfigsField.set(zkHelixAdminClient, clusterConfig);

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
  public void testPartiallyDefinedRebalancePreference() throws NoSuchFieldException, IllegalAccessException {
    int helixRebalancePreferenceForceBaselineConverge = 1;

    when(zkHelixAdminClient.isVeniceControllerClusterCreated()).thenReturn(false);
    when(mockHelixAdmin.addCluster(VENICE_CONTROLLER_CLUSTER, false)).thenReturn(true);

    Properties clusterProperties = getBaseSingleRegionProperties(false);
    clusterProperties.put(
        ConfigKeys.CONTROLLER_HELIX_REBALANCE_PREFERENCE_FORCE_BASELINE_CONVERGE,
        helixRebalancePreferenceForceBaselineConverge);
    VeniceControllerClusterConfig clusterConfig =
        new VeniceControllerClusterConfig(new VeniceProperties(clusterProperties));

    Field commonConfigsField = ZkHelixAdminClient.class.getDeclaredField("commonConfig");
    commonConfigsField.setAccessible(true);
    commonConfigsField.set(zkHelixAdminClient, clusterConfig);

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
}
