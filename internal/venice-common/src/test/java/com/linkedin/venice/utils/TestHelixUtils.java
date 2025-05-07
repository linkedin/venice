package com.linkedin.venice.utils;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixException;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestHelixUtils {
  private final String TEST_PATH = "/test/path";
  private final String TEST_DATA = "testData";
  private ZkBaseDataAccessor<String> mockDataAccessor;

  private final List<String> TEST_PATH_LIST = Arrays.asList("/test/path/child1", "/test/path/child2");
  private final List<String> TEST_DATA_LIST = Arrays.asList("data1", "data2");
  private final boolean[] SUCCESS_RESULTS = new boolean[] { true, true };
  private final boolean[] FAILED_RESULTS = new boolean[] { true, false };

  @Test
  public void parsesHostnameFromInstanceName() {
    Instance instance1 = HelixUtils.getInstanceFromHelixInstanceName("host_1234");
    Assert.assertEquals(instance1.getHost(), "host");
    Assert.assertEquals(instance1.getPort(), 1234);

    Instance instance2 = HelixUtils.getInstanceFromHelixInstanceName("host_name_5678");
    Assert.assertEquals(instance2.getHost(), "host_name");
    Assert.assertEquals(instance2.getPort(), 5678);
  }

  @Test
  public void testGetCloudConfig() {
    List<String> cloudInfoSources = new ArrayList<>();
    cloudInfoSources.add("TestSource");

    CloudConfig cloudConfig = HelixUtils.getCloudConfig(
        CloudProvider.CUSTOMIZED,
        "NA",
        cloudInfoSources,
        "com.linkedin.venice.controller.helix",
        "TestProcessor");

    assertTrue(cloudConfig.isCloudEnabled());
    assertEquals(cloudConfig.getCloudProvider(), "CUSTOMIZED");
    assertEquals(cloudConfig.getCloudID(), "NA");
    assertEquals(cloudConfig.getCloudInfoSources().size(), 1);
    assertEquals(cloudConfig.getCloudInfoSources().get(0), "TestSource");
    assertEquals(cloudConfig.getCloudInfoProcessorPackage(), "com.linkedin.venice.controller.helix");
    assertEquals(cloudConfig.getCloudInfoProcessorName(), "TestProcessor");
  }

  @Test
  public void testGetCloudConfigWhenControllerCloudInfoSourcesNotSet() {
    assertThrows(
        HelixException.class,
        () -> HelixUtils.getCloudConfig(
            CloudProvider.CUSTOMIZED,
            "NA",
            Collections.emptyList(),
            "com.linkedin.venice.controller.helix",
            "TestProcessor"));
  }

  @Test
  public void testGetCloudConfigWhenControllerCloudInfoProcessorNameNotSet() {
    List<String> cloudInfoSources = new ArrayList<>();
    cloudInfoSources.add("TestSource");

    assertThrows(
        HelixException.class,
        () -> HelixUtils.getCloudConfig(
            CloudProvider.CUSTOMIZED,
            "NA",
            cloudInfoSources,
            "com.linkedin.venice.controller.helix",
            ""));
  }

  @BeforeMethod
  public void setUp() {
    mockDataAccessor = mock(ZkBaseDataAccessor.class);
  }

  @Test
  public void testCreate() {
    doReturn(true).when(mockDataAccessor).create(TEST_PATH, TEST_DATA, AccessOption.PERSISTENT);

    HelixUtils.create(mockDataAccessor, TEST_PATH, TEST_DATA, 3);

    verify(mockDataAccessor, times(1)).create(TEST_PATH, TEST_DATA, AccessOption.PERSISTENT);
  }

  @Test
  public void testCreateFailsAfterRetries() {
    doReturn(false).when(mockDataAccessor).create(TEST_PATH, TEST_DATA, AccessOption.PERSISTENT);

    Assert.assertThrows(VeniceException.class, () -> {
      HelixUtils.create(mockDataAccessor, TEST_PATH, TEST_DATA, 3);
    });
  }

  @Test
  public void testCreateSucceedsAfterRetries() {
    doReturn(false).doReturn(false)
        .doReturn(true)
        .when(mockDataAccessor)
        .create(TEST_PATH, TEST_DATA, AccessOption.PERSISTENT);

    HelixUtils.create(mockDataAccessor, TEST_PATH, TEST_DATA, 3);

    verify(mockDataAccessor, times(3)).create(TEST_PATH, TEST_DATA, AccessOption.PERSISTENT);
  }

  @Test
  public void testUpdate() {
    doReturn(true).when(mockDataAccessor).set(TEST_PATH, TEST_DATA, AccessOption.PERSISTENT);

    HelixUtils.update(mockDataAccessor, TEST_PATH, TEST_DATA, 3);

    verify(mockDataAccessor, times(1)).set(TEST_PATH, TEST_DATA, AccessOption.PERSISTENT);
  }

  @Test
  public void testUpdateFailsAfterRetries() {
    doReturn(false).when(mockDataAccessor).set(TEST_PATH, TEST_DATA, AccessOption.PERSISTENT);

    Assert.assertThrows(VeniceException.class, () -> {
      HelixUtils.update(mockDataAccessor, TEST_PATH, TEST_DATA, 3);
    });
  }

  @Test
  public void testUpdateSucceedsAfterRetries() {
    doReturn(false).doReturn(false)
        .doReturn(true)
        .when(mockDataAccessor)
        .set(TEST_PATH, TEST_DATA, AccessOption.PERSISTENT);

    HelixUtils.update(mockDataAccessor, TEST_PATH, TEST_DATA, 3);

    verify(mockDataAccessor, times(3)).set(TEST_PATH, TEST_DATA, AccessOption.PERSISTENT);
  }

  @Test
  public void testUpdateChildren() {
    boolean[] SUCCESS_RESULTS = new boolean[] { true, true };
    doReturn(SUCCESS_RESULTS).when(mockDataAccessor)
        .setChildren(TEST_PATH_LIST, TEST_DATA_LIST, AccessOption.PERSISTENT);

    HelixUtils.updateChildren(mockDataAccessor, TEST_PATH_LIST, TEST_DATA_LIST, 3);

    verify(mockDataAccessor, times(1)).setChildren(TEST_PATH_LIST, TEST_DATA_LIST, AccessOption.PERSISTENT);
  }

  @Test
  public void testUpdateChildrenFailsAfterRetries() {
    doReturn(FAILED_RESULTS).when(mockDataAccessor)
        .setChildren(TEST_PATH_LIST, TEST_DATA_LIST, AccessOption.PERSISTENT);

    Assert.assertThrows(VeniceException.class, () -> {
      HelixUtils.updateChildren(mockDataAccessor, TEST_PATH_LIST, TEST_DATA_LIST, 3);
    });
  }

  @Test
  public void testUpdateChildrenSucceedsAfterRetries() {
    doReturn(FAILED_RESULTS).doReturn(FAILED_RESULTS)
        .doReturn(SUCCESS_RESULTS)
        .when(mockDataAccessor)
        .setChildren(TEST_PATH_LIST, TEST_DATA_LIST, AccessOption.PERSISTENT);

    HelixUtils.updateChildren(mockDataAccessor, TEST_PATH_LIST, TEST_DATA_LIST, 3);

    verify(mockDataAccessor, times(3)).setChildren(TEST_PATH_LIST, TEST_DATA_LIST, AccessOption.PERSISTENT);
  }

  @Test
  public void testRemove() {
    doReturn(true).when(mockDataAccessor).remove(TEST_PATH, AccessOption.PERSISTENT);

    HelixUtils.remove(mockDataAccessor, TEST_PATH, 3);

    verify(mockDataAccessor, times(1)).remove(TEST_PATH, AccessOption.PERSISTENT);
  }

  @Test
  public void testRemoveFailsAfterRetries() {
    doReturn(false).when(mockDataAccessor).remove(TEST_PATH, AccessOption.PERSISTENT);

    Assert.assertThrows(VeniceException.class, () -> {
      HelixUtils.remove(mockDataAccessor, TEST_PATH, 3);
    });
  }

  @Test
  public void testRemoveSucceedsAfterRetries() {
    doReturn(false).doReturn(false).doReturn(true).when(mockDataAccessor).remove(TEST_PATH, AccessOption.PERSISTENT);

    HelixUtils.remove(mockDataAccessor, TEST_PATH, 3);
  }

  @Test
  public void testCompareAndUpdate() {
    DataUpdater<String> dataUpdater = mock(DataUpdater.class);

    doReturn(true).when(mockDataAccessor).update(TEST_PATH, dataUpdater, AccessOption.PERSISTENT);

    HelixUtils.compareAndUpdate(mockDataAccessor, TEST_PATH, 3, dataUpdater);

    verify(mockDataAccessor, times(1)).update(TEST_PATH, dataUpdater, AccessOption.PERSISTENT);
  }

  @Test
  public void testCompareAndUpdateFailsAfterRetries() {
    DataUpdater<String> dataUpdater = mock(DataUpdater.class);

    doReturn(false).when(mockDataAccessor).update(TEST_PATH, dataUpdater, AccessOption.PERSISTENT);

    Assert.assertThrows(VeniceException.class, () -> {
      HelixUtils.compareAndUpdate(mockDataAccessor, TEST_PATH, 3, dataUpdater);
    });
  }

  @Test
  public void testCompareAndUpdateSucceedsAfterRetries() {
    DataUpdater<String> dataUpdater = mock(DataUpdater.class);

    doReturn(false).doReturn(false)
        .doReturn(true)
        .when(mockDataAccessor)
        .update(TEST_PATH, dataUpdater, AccessOption.PERSISTENT);

    HelixUtils.compareAndUpdate(mockDataAccessor, TEST_PATH, 3, dataUpdater);

    verify(mockDataAccessor, times(3)).update(TEST_PATH, dataUpdater, AccessOption.PERSISTENT);
  }
}
