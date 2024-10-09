package com.linkedin.venice.controller;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.model.IdealState;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestZkHelixAdminClient {
  private ZkHelixAdminClient zkHelixAdminClient;
  private HelixAdmin mockHelixAdmin;
  private VeniceControllerMultiClusterConfig mockMultiClusterConfigs;
  private VeniceControllerClusterConfig mockClusterConfig;

  private static final String controllerClusterName = "venice-controllers";

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
    CloudProvider cloudProvider = CloudProvider.CUSTOMIZED;
    List<String> cloudInfoSources = new ArrayList<>();
    cloudInfoSources.add("TestSource");

    when(mockClusterConfig.getHelixCloudProvider()).thenReturn(cloudProvider);
    when(mockClusterConfig.getHelixCloudId()).thenReturn("NA");
    when(mockClusterConfig.getHelixCloudInfoSources()).thenReturn(cloudInfoSources);
    when(mockClusterConfig.getHelixCloudInfoProcessorName()).thenReturn("TestProcessor");

    doCallRealMethod().when(zkHelixAdminClient).setCloudConfig(controllerClusterName, mockClusterConfig);
    zkHelixAdminClient.setCloudConfig(controllerClusterName, mockClusterConfig);

    verify(mockHelixAdmin).addCloudConfig(any(), any());
  }

  @Test
  public void testControllerCloudInfoSourcesNotSet() {
    CloudProvider cloudProvider = CloudProvider.CUSTOMIZED;
    when(mockClusterConfig.getHelixCloudProvider()).thenReturn(cloudProvider);
    when(mockClusterConfig.getHelixCloudId()).thenReturn("NA");
    when(mockClusterConfig.getHelixCloudInfoSources()).thenReturn(Collections.emptyList());
    when(mockClusterConfig.getHelixCloudInfoProcessorName()).thenReturn("TestProcessor");

    doCallRealMethod().when(zkHelixAdminClient).setCloudConfig(controllerClusterName, mockClusterConfig);
    assertThrows(
        HelixException.class,
        () -> zkHelixAdminClient.setCloudConfig(controllerClusterName, mockClusterConfig));
  }

  @Test
  public void testControllerCloudInfoProcessorNameNotSet() {
    CloudProvider cloudProvider = CloudProvider.CUSTOMIZED;
    List<String> cloudInfoSources = new ArrayList<>();
    cloudInfoSources.add("TestSource");

    when(mockClusterConfig.getHelixCloudProvider()).thenReturn(cloudProvider);
    when(mockClusterConfig.getHelixCloudId()).thenReturn("NA");
    when(mockClusterConfig.getHelixCloudInfoSources()).thenReturn(cloudInfoSources);
    when(mockClusterConfig.getHelixCloudInfoProcessorName()).thenReturn("");

    doCallRealMethod().when(zkHelixAdminClient).setCloudConfig(controllerClusterName, mockClusterConfig);
    assertThrows(
        HelixException.class,
        () -> zkHelixAdminClient.setCloudConfig(controllerClusterName, mockClusterConfig));
  }
}
