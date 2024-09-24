package com.linkedin.venice.controller;

import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.testng.annotations.Test;


public class TestZkHelixAdminClient {
  @Test
  public void testInstanceGroupTag() throws NoSuchFieldException, IllegalAccessException {
    ZkHelixAdminClient zkHelixAdminClient = mock(ZkHelixAdminClient.class);
    HelixAdmin mockHelixAdmin = mock(HelixAdmin.class);
    VeniceControllerMultiClusterConfig mockMultiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig mockClusterConfig = mock(VeniceControllerClusterConfig.class);
    IdealState mockIdealState = mock(IdealState.class);

    when(mockClusterConfig.getControllerResourceInstanceGroupTag()).thenReturn("GENERAL");
    when(mockMultiClusterConfigs.getControllerConfig(anyString())).thenReturn(mockClusterConfig);
    when(mockHelixAdmin.getResourceIdealState(any(), any())).thenReturn(mockIdealState);

    doCallRealMethod().when(zkHelixAdminClient).addVeniceStorageClusterToControllerCluster(anyString());

    Field multiClusterConfigsField = ZkHelixAdminClient.class.getDeclaredField("multiClusterConfigs");
    multiClusterConfigsField.setAccessible(true);
    multiClusterConfigsField.set(zkHelixAdminClient, mockMultiClusterConfigs);

    Field helixAdminField = ZkHelixAdminClient.class.getDeclaredField("helixAdmin");
    helixAdminField.setAccessible(true);
    helixAdminField.set(zkHelixAdminClient, mockHelixAdmin);

    String clusterName = "test-cluster";
    zkHelixAdminClient.addVeniceStorageClusterToControllerCluster(clusterName);

    verify(mockIdealState, times(1)).setInstanceGroupTag("GENERAL");
  }
}
