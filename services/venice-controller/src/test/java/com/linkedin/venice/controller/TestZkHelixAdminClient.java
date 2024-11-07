package com.linkedin.venice.controller;

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

import com.linkedin.venice.utils.HelixUtils;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestZkHelixAdminClient {
  private ZkHelixAdminClient zkHelixAdminClient;
  private HelixAdmin mockHelixAdmin;
  private VeniceControllerMultiClusterConfig mockMultiClusterConfigs;
  private VeniceControllerClusterConfig mockCommonConfig;
  private static final String VENICE_CONTROLLER_CLUSTER = "venice-controller-cluster";

  @BeforeMethod
  public void setUp() throws NoSuchFieldException, IllegalAccessException {
    zkHelixAdminClient = mock(ZkHelixAdminClient.class);
    mockHelixAdmin = mock(HelixAdmin.class);
    mockMultiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    mockCommonConfig = mock(VeniceControllerClusterConfig.class);

    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
      try {
        Field helixAdminField = ZkHelixAdminClient.class.getDeclaredField("helixAdmin");
        helixAdminField.setAccessible(true);
        helixAdminField.set(zkHelixAdminClient, mockHelixAdmin);

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
    doCallRealMethod().when(zkHelixAdminClient).createVeniceStorageCluster(any(), any());

    // When the cluster is not Helix cloud enabled
    ClusterConfig helixClusterConfig = mock(ClusterConfig.class);
    zkHelixAdminClient.createVeniceStorageCluster(clusterName, helixClusterConfig);

    verify(zkHelixAdminClient).updateClusterConfigs(clusterName, helixClusterConfig);
    verify(mockHelixAdmin, never()).addCloudConfig(any(), any());

    clearInvocations(zkHelixAdminClient);

    // When the cluster is Helix cloud enabled
    doReturn(true).when(mockClusterConfig).isStorageClusterHelixCloudEnabled();
    CloudConfig cloudConfig = mock(CloudConfig.class);
    doReturn(cloudConfig).when(mockClusterConfig).getHelixCloudConfig();
    zkHelixAdminClient.createVeniceStorageCluster(clusterName, helixClusterConfig);

    verify(zkHelixAdminClient).updateClusterConfigs(clusterName, helixClusterConfig);
    verify(mockHelixAdmin).addCloudConfig(clusterName, cloudConfig);
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
      HelixConfigScope scope = invocation.getArgument(0);
      Map<String, String> clusterProps = invocation.getArgument(1);

      assertEquals(scope.getType(), HelixConfigScope.ConfigScopeProperty.CLUSTER);
      assertEquals(scope.getClusterName(), clusterName);
      assertEquals(clusterProps.size(), 6);
      assertEquals(clusterProps.get(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN), "true");
      assertEquals(clusterProps.get(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_ENABLED.name()), "true");
      assertEquals(clusterProps.get(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_TIME.name()), "1000");
      assertEquals(
          clusterProps.get(ClusterConfig.ClusterConfigProperty.PERSIST_BEST_POSSIBLE_ASSIGNMENT.name()),
          "true");
      assertEquals(
          clusterProps.get(ClusterConfig.ClusterConfigProperty.TOPOLOGY.name()),
          "/" + HelixUtils.TOPOLOGY_CONSTRAINT);
      assertEquals(
          clusterProps.get(ClusterConfig.ClusterConfigProperty.FAULT_ZONE_TYPE.name()),
          HelixUtils.TOPOLOGY_CONSTRAINT);

      return null;
    }).when(mockHelixAdmin).setConfig(any(), any());

    zkHelixAdminClient.updateClusterConfigs(clusterName, clusterConfig);
  }
}
