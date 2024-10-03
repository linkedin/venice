package com.linkedin.venice.controller;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.exceptions.VeniceException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.helix.HelixAdmin;
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

    Field helixAdminField = ZkHelixAdminClient.class.getDeclaredField("helixAdmin");
    helixAdminField.setAccessible(true);
    helixAdminField.set(zkHelixAdminClient, mockHelixAdmin);

    Field multiClusterConfigsField = ZkHelixAdminClient.class.getDeclaredField("multiClusterConfigs");
    multiClusterConfigsField.setAccessible(true);
    multiClusterConfigsField.set(zkHelixAdminClient, mockMultiClusterConfigs);

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

    when(mockClusterConfig.isControllerCloudEnabled()).thenReturn(true);
    when(mockClusterConfig.getControllerCloudProvider()).thenReturn("CUSTOMIZED");
    when(mockClusterConfig.getControllerCloudId()).thenReturn("NA");
    when(mockClusterConfig.getControllerCloudInfoSources()).thenReturn(cloudInfoSources);
    when(mockClusterConfig.getControllerCloudInfoProcessorName()).thenReturn("TestProcessor");

    doCallRealMethod().when(zkHelixAdminClient).setCloudConfig(any());
    zkHelixAdminClient.setCloudConfig(mockClusterConfig);

    verify(mockHelixAdmin).addCloudConfig(any(), any());
  }

  @Test
  public void testControllerCloudDisabled() {
    when(mockClusterConfig.isControllerCloudEnabled()).thenReturn(false);

    doCallRealMethod().when(zkHelixAdminClient).setCloudConfig(any());
    zkHelixAdminClient.setCloudConfig(mockClusterConfig);

    verify(mockHelixAdmin, never()).addCloudConfig(any(), any());
  }

  @Test
  public void testControllerCloudProviderNotSet() {
    when(mockClusterConfig.isControllerCloudEnabled()).thenReturn(true);
    when(mockClusterConfig.getControllerCloudProvider()).thenReturn("");

    doCallRealMethod().when(zkHelixAdminClient).setCloudConfig(any());
    assertThrows(VeniceException.class, () -> zkHelixAdminClient.setCloudConfig(mockClusterConfig));
  }

  @Test
  public void testControllerCloudInfoSourcesNotSet() {
    when(mockClusterConfig.isControllerCloudEnabled()).thenReturn(true);
    when(mockClusterConfig.getControllerCloudProvider()).thenReturn("CUSTOMIZED");
    when(mockClusterConfig.getControllerCloudInfoSources()).thenReturn(Collections.emptyList());

    doCallRealMethod().when(zkHelixAdminClient).setCloudConfig(any());
    assertThrows(VeniceException.class, () -> zkHelixAdminClient.setCloudConfig(mockClusterConfig));
  }

  @Test
  public void testControllerCloudInfoProcessorNameNotSet() {
    List<String> cloudInfoSources = new ArrayList<>();
    cloudInfoSources.add("TestSource");

    when(mockClusterConfig.isControllerCloudEnabled()).thenReturn(true);
    when(mockClusterConfig.getControllerCloudProvider()).thenReturn("CUSTOMIZED");
    when(mockClusterConfig.getControllerCloudInfoSources()).thenReturn(cloudInfoSources);
    when(mockClusterConfig.getControllerCloudInfoProcessorName()).thenReturn("");

    doCallRealMethod().when(zkHelixAdminClient).setCloudConfig(any());
    assertThrows(VeniceException.class, () -> zkHelixAdminClient.setCloudConfig(mockClusterConfig));
  }
}
