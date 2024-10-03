package com.linkedin.venice.controller;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.exceptions.VeniceException;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.model.IdealState;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestZkHelixAdminClient {
  private ZkHelixAdminClient zkHelixAdminClient;
  private HelixAdmin mockHelixAdmin;
  private VeniceControllerMultiClusterConfig mockMultiClusterConfigs;
  private VeniceControllerClusterConfig mockClusterConfig;

  @BeforeMethod
  public void setUp() throws NoSuchFieldException, IllegalAccessException {
    zkHelixAdminClient = mock(ZkHelixAdminClient.class);
    mockHelixAdmin = mock(HelixAdmin.class);
    mockMultiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    mockClusterConfig = mock(VeniceControllerClusterConfig.class);

    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
      try {
        Field helixAdminField = ZkHelixAdminClient.class.getDeclaredField("helixAdmin");
        helixAdminField.setAccessible(true);
        helixAdminField.set(zkHelixAdminClient, mockHelixAdmin);

        Field multiClusterConfigsField = ZkHelixAdminClient.class.getDeclaredField("multiClusterConfigs");
        multiClusterConfigsField.setAccessible(true);
        multiClusterConfigsField.set(zkHelixAdminClient, mockMultiClusterConfigs);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }

  @Test
  public void testInstanceGroupTag() {
    IdealState mockIdealState = mock(IdealState.class);

    when(mockClusterConfig.getControllerResourceInstanceGroupTag()).thenReturn("GENERAL");
    when(mockMultiClusterConfigs.getControllerConfig(anyString())).thenReturn(mockClusterConfig);
    when(mockHelixAdmin.getResourceIdealState(any(), any())).thenReturn(mockIdealState);

    doCallRealMethod().when(zkHelixAdminClient).addVeniceStorageClusterToControllerCluster(anyString());

    String clusterName = "test-cluster";
    zkHelixAdminClient.addVeniceStorageClusterToControllerCluster(clusterName);

    verify(mockIdealState).setInstanceGroupTag("GENERAL");
  }

  @Test
  public void testSetCloudConfig() {
    List<String> cloudInfoSources = new ArrayList<>();
    cloudInfoSources.add("TestSource");

    when(mockClusterConfig.isControllerClusterHelixCloudEnabled()).thenReturn(true);
    when(mockClusterConfig.isControllerStorageClusterHelixCloudEnabled()).thenReturn(true);
    when(mockClusterConfig.getControllerHelixCloudProvider()).thenReturn("CUSTOMIZED");
    when(mockClusterConfig.getControllerHelixCloudId()).thenReturn("NA");
    when(mockClusterConfig.getControllerHelixCloudInfoSources()).thenReturn(cloudInfoSources);
    when(mockClusterConfig.getControllerHelixCloudInfoProcessorName()).thenReturn("TestProcessor");

    doCallRealMethod().when(zkHelixAdminClient).setCloudConfig(mockClusterConfig, true);
    doCallRealMethod().when(zkHelixAdminClient).setCloudConfig(mockClusterConfig, false);
    zkHelixAdminClient.setCloudConfig(mockClusterConfig, true);
    zkHelixAdminClient.setCloudConfig(mockClusterConfig, false);

    verify(mockHelixAdmin, times(2)).addCloudConfig(any(), any());
  }

  @Test
  public void testControllerClusterCloudDisabled() {
    when(mockClusterConfig.isControllerClusterHelixCloudEnabled()).thenReturn(false);
    when(mockClusterConfig.isControllerStorageClusterHelixCloudEnabled()).thenReturn(false);

    doCallRealMethod().when(zkHelixAdminClient).setCloudConfig(mockClusterConfig, true);
    zkHelixAdminClient.setCloudConfig(mockClusterConfig, true);

    verify(mockHelixAdmin, never()).addCloudConfig(any(), any());
  }

  @Test
  public void testControllerStorageClusterCloudDisabled() {
    when(mockClusterConfig.isControllerClusterHelixCloudEnabled()).thenReturn(false);
    when(mockClusterConfig.isControllerStorageClusterHelixCloudEnabled()).thenReturn(false);

    doCallRealMethod().when(zkHelixAdminClient).setCloudConfig(mockClusterConfig, false);
    zkHelixAdminClient.setCloudConfig(mockClusterConfig, false);

    verify(mockHelixAdmin, never()).addCloudConfig(any(), any());
  }

  @Test
  public void testControllerCloudProviderNotSet() {
    when(mockClusterConfig.isControllerClusterHelixCloudEnabled()).thenReturn(true);
    when(mockClusterConfig.getControllerHelixCloudProvider()).thenReturn("");

    doCallRealMethod().when(zkHelixAdminClient).setCloudConfig(mockClusterConfig, true);
    assertThrows(VeniceException.class, () -> zkHelixAdminClient.setCloudConfig(mockClusterConfig, true));
  }

  @Test
  public void testControllerCloudInfoSourcesNotSet() {
    when(mockClusterConfig.isControllerClusterHelixCloudEnabled()).thenReturn(true);
    when(mockClusterConfig.getControllerHelixCloudProvider()).thenReturn("CUSTOMIZED");
    when(mockClusterConfig.getControllerHelixCloudInfoSources()).thenReturn(Collections.emptyList());
    when(mockClusterConfig.getControllerHelixCloudInfoProcessorName()).thenReturn("TestProcessor");

    doCallRealMethod().when(zkHelixAdminClient).setCloudConfig(mockClusterConfig, true);
    assertThrows(HelixException.class, () -> zkHelixAdminClient.setCloudConfig(mockClusterConfig, true));
  }

  @Test
  public void testControllerCloudInfoProcessorNameNotSet() {
    List<String> cloudInfoSources = new ArrayList<>();
    cloudInfoSources.add("TestSource");

    when(mockClusterConfig.isControllerClusterHelixCloudEnabled()).thenReturn(true);
    when(mockClusterConfig.getControllerHelixCloudProvider()).thenReturn("CUSTOMIZED");
    when(mockClusterConfig.getControllerHelixCloudInfoSources()).thenReturn(cloudInfoSources);
    when(mockClusterConfig.getControllerHelixCloudInfoProcessorName()).thenReturn("");

    doCallRealMethod().when(zkHelixAdminClient).setCloudConfig(mockClusterConfig, true);
    assertThrows(HelixException.class, () -> zkHelixAdminClient.setCloudConfig(mockClusterConfig, true));
  }
}
