package com.linkedin.venice.controller;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.SafeHelixManager;
import java.util.Arrays;
import java.util.List;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.model.InstanceConfig;
import org.testng.annotations.Test;


public class TestControllerInstanceTagRefresher {
  @Test
  public void testOnPreConnect_addAndRemoveTags() {
    SafeHelixManager mockManager = mock(SafeHelixManager.class);
    ConfigAccessor mockAccessor = mock(ConfigAccessor.class);
    InstanceConfig mockConfig = new InstanceConfig("instance1");
    mockConfig.addTag("old-tag");

    VeniceControllerMultiClusterConfig mockConfigSource = mock(VeniceControllerMultiClusterConfig.class);
    List<String> expectedTags = Arrays.asList("new-tag-1", "new-tag-2");

    when(mockManager.getInstanceName()).thenReturn("instance1");
    when(mockManager.getClusterName()).thenReturn("test-cluster");
    when(mockManager.getConfigAccessor()).thenReturn(mockAccessor);
    when(mockAccessor.getInstanceConfig("test-cluster", "instance1")).thenReturn(mockConfig);
    when(mockConfigSource.getControllerInstanceTagList()).thenReturn(expectedTags);

    ControllerInstanceTagRefresher refresher = new ControllerInstanceTagRefresher(mockManager, mockConfigSource);
    refresher.onPreConnect();

    verify(mockAccessor).setInstanceConfig(eq("test-cluster"), eq("instance1"), any(InstanceConfig.class));
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testOnPreConnect_throwsException() {
    SafeHelixManager mockManager = mock(SafeHelixManager.class);
    VeniceControllerMultiClusterConfig mockConfigSource = mock(VeniceControllerMultiClusterConfig.class);

    when(mockManager.getInstanceName()).thenThrow(new RuntimeException("Boom"));

    ControllerInstanceTagRefresher refresher = new ControllerInstanceTagRefresher(mockManager, mockConfigSource);
    refresher.onPreConnect(); // should throw
  }

  @Test
  public void testOnPreConnect_noChangesNeeded() {
    SafeHelixManager mockManager = mock(SafeHelixManager.class);
    ConfigAccessor mockAccessor = mock(ConfigAccessor.class);
    InstanceConfig mockConfig = new InstanceConfig("instance1");
    mockConfig.addTag("tag1");
    mockConfig.addTag("tag2");

    VeniceControllerMultiClusterConfig mockConfigSource = mock(VeniceControllerMultiClusterConfig.class);
    List<String> expectedTags = Arrays.asList("tag1", "tag2");

    when(mockManager.getInstanceName()).thenReturn("instance1");
    when(mockManager.getClusterName()).thenReturn("cluster");
    when(mockManager.getConfigAccessor()).thenReturn(mockAccessor);
    when(mockAccessor.getInstanceConfig("cluster", "instance1")).thenReturn(mockConfig);
    when(mockConfigSource.getControllerInstanceTagList()).thenReturn(expectedTags);

    ControllerInstanceTagRefresher refresher = new ControllerInstanceTagRefresher(mockManager, mockConfigSource);
    refresher.onPreConnect();

    verify(mockAccessor, never()).setInstanceConfig(anyString(), anyString(), any());
  }
}
