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
import java.util.Collections;
import java.util.List;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixException;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHelixUtils {
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

  @Test
  public void testCreate() {
    ZkBaseDataAccessor<String> mockDataAccessor = mock(ZkBaseDataAccessor.class);
    String testPath = "/test/path";
    String testData = "testData";

    doReturn(true).when(mockDataAccessor).create(testPath, testData, AccessOption.PERSISTENT);

    HelixUtils.create(mockDataAccessor, testPath, testData, 3);

    verify(mockDataAccessor, times(1)).create(testPath, testData, AccessOption.PERSISTENT);
  }

  @Test
  public void testCreateFailsAfterRetries() {
    ZkBaseDataAccessor<String> mockDataAccessor = mock(ZkBaseDataAccessor.class);
    String testPath = "/test/path";
    String testData = "testData";

    doReturn(false).when(mockDataAccessor).create(testPath, testData, AccessOption.PERSISTENT);

    Assert.assertThrows(VeniceException.class, () -> {
      HelixUtils.create(mockDataAccessor, testPath, testData, 3);
    });
  }

  @Test
  public void testCreateSucceedsAfterRetries() {
    ZkBaseDataAccessor<String> mockDataAccessor = mock(ZkBaseDataAccessor.class);
    String testPath = "/test/path";
    String testData = "testData";

    doReturn(false).doReturn(false)
        .doReturn(true)
        .when(mockDataAccessor)
        .create(testPath, testData, AccessOption.PERSISTENT);

    HelixUtils.create(mockDataAccessor, testPath, testData, 3);

    verify(mockDataAccessor, times(3)).create(testPath, testData, AccessOption.PERSISTENT);
  }

  @Test
  public void testUpdate() {
    ZkBaseDataAccessor<String> mockDataAccessor = mock(ZkBaseDataAccessor.class);
    String testPath = "/test/path";
    String testData = "testData";

    doReturn(true).when(mockDataAccessor).set(testPath, testData, AccessOption.PERSISTENT);

    HelixUtils.update(mockDataAccessor, testPath, testData, 3);

    verify(mockDataAccessor, times(1)).set(testPath, testData, AccessOption.PERSISTENT);
  }

  @Test
  public void testUpdateFailsAfterRetries() {
    ZkBaseDataAccessor<String> mockDataAccessor = mock(ZkBaseDataAccessor.class);
    String testPath = "/test/path";
    String testData = "testData";

    doReturn(false).when(mockDataAccessor).set(testPath, testData, AccessOption.PERSISTENT);

    Assert.assertThrows(VeniceException.class, () -> {
      HelixUtils.update(mockDataAccessor, testPath, testData, 3);
    });
  }

  @Test
  public void testUpdateSucceedsAfterRetries() {
    ZkBaseDataAccessor<String> mockDataAccessor = mock(ZkBaseDataAccessor.class);
    String testPath = "/test/path";
    String testData = "testData";

    doReturn(false).doReturn(false)
        .doReturn(true)
        .when(mockDataAccessor)
        .set(testPath, testData, AccessOption.PERSISTENT);

    HelixUtils.update(mockDataAccessor, testPath, testData, 3);

    verify(mockDataAccessor, times(3)).set(testPath, testData, AccessOption.PERSISTENT);
  }

  @Test
  public void testUpdateChildren() {
    ZkBaseDataAccessor<String> mockDataAccessor = mock(ZkBaseDataAccessor.class);
    List<String> paths = new ArrayList<>();
    paths.add("/test/path/child1");
    paths.add("/test/path/child2");
    List<String> data = new ArrayList<>();
    data.add("data1");
    data.add("data2");

    boolean[] successResults = new boolean[] { true, true };
    doReturn(successResults).when(mockDataAccessor).setChildren(paths, data, AccessOption.PERSISTENT);

    HelixUtils.updateChildren(mockDataAccessor, paths, data, 3);

    verify(mockDataAccessor, times(1)).setChildren(paths, data, AccessOption.PERSISTENT);
  }

  @Test
  public void testUpdateChildrenFailsAfterRetries() {
    ZkBaseDataAccessor<String> mockDataAccessor = mock(ZkBaseDataAccessor.class);
    List<String> paths = new ArrayList<>();
    paths.add("/test/path/child1");
    paths.add("/test/path/child2");
    List<String> data = new ArrayList<>();
    data.add("data1");
    data.add("data2");

    boolean[] failedResults = new boolean[] { true, false };
    doReturn(failedResults).when(mockDataAccessor).setChildren(paths, data, AccessOption.PERSISTENT);

    Assert.assertThrows(VeniceException.class, () -> {
      HelixUtils.updateChildren(mockDataAccessor, paths, data, 3);
    });
  }

  @Test
  public void testUpdateChildrenSucceedsAfterRetries() {
    ZkBaseDataAccessor<String> mockDataAccessor = mock(ZkBaseDataAccessor.class);
    List<String> paths = new ArrayList<>();
    paths.add("/test/path/child1");
    paths.add("/test/path/child2");
    List<String> data = new ArrayList<>();
    data.add("data1");
    data.add("data2");

    boolean[] successResults = new boolean[] { true, true };
    boolean[] failedResults = new boolean[] { true, false };

    doReturn(failedResults).doReturn(failedResults)
        .doReturn(successResults)
        .when(mockDataAccessor)
        .setChildren(paths, data, AccessOption.PERSISTENT);

    HelixUtils.updateChildren(mockDataAccessor, paths, data, 3);

    verify(mockDataAccessor, times(3)).setChildren(paths, data, AccessOption.PERSISTENT);
  }

  @Test
  public void testRemove() {
    ZkBaseDataAccessor<String> mockDataAccessor = mock(ZkBaseDataAccessor.class);
    String testPath = "/test/path";

    doReturn(true).when(mockDataAccessor).remove(testPath, AccessOption.PERSISTENT);

    HelixUtils.remove(mockDataAccessor, testPath, 3);

    verify(mockDataAccessor, times(1)).remove(testPath, AccessOption.PERSISTENT);
  }

  @Test
  public void testRemoveFailsAfterRetries() {
    ZkBaseDataAccessor<String> mockDataAccessor = mock(ZkBaseDataAccessor.class);
    String testPath = "/test/path";

    doReturn(false).when(mockDataAccessor).remove(testPath, AccessOption.PERSISTENT);

    Assert.assertThrows(VeniceException.class, () -> {
      HelixUtils.remove(mockDataAccessor, testPath, 3);
    });
  }

  @Test
  public void testRemoveSucceedsAfterRetries() {
    ZkBaseDataAccessor<String> mockDataAccessor = mock(ZkBaseDataAccessor.class);
    String testPath = "/test/path";

    doReturn(false).doReturn(false).doReturn(true).when(mockDataAccessor).remove(testPath, AccessOption.PERSISTENT);

    HelixUtils.remove(mockDataAccessor, testPath, 3);
  }

  @Test
  public void testCompareAndUpdate() {
    ZkBaseDataAccessor<String> mockDataAccessor = mock(ZkBaseDataAccessor.class);
    String testPath = "/test/path";
    DataUpdater<String> dataUpdater = mock(DataUpdater.class);

    doReturn(true).when(mockDataAccessor).update(testPath, dataUpdater, AccessOption.PERSISTENT);

    HelixUtils.compareAndUpdate(mockDataAccessor, testPath, 3, dataUpdater);

    verify(mockDataAccessor, times(1)).update(testPath, dataUpdater, AccessOption.PERSISTENT);
  }

  @Test
  public void testCompareAndUpdateFailsAfterRetries() {
    ZkBaseDataAccessor<String> mockDataAccessor = mock(ZkBaseDataAccessor.class);
    String testPath = "/test/path";
    DataUpdater<String> dataUpdater = mock(DataUpdater.class);

    doReturn(false).when(mockDataAccessor).update(testPath, dataUpdater, AccessOption.PERSISTENT);

    Assert.assertThrows(VeniceException.class, () -> {
      HelixUtils.compareAndUpdate(mockDataAccessor, testPath, 3, dataUpdater);
    });
  }

  @Test
  public void testCompareAndUpdateSucceedsAfterRetries() {
    ZkBaseDataAccessor<String> mockDataAccessor = mock(ZkBaseDataAccessor.class);
    String testPath = "/test/path";
    DataUpdater<String> dataUpdater = mock(DataUpdater.class);

    doReturn(false).doReturn(false)
        .doReturn(true)
        .when(mockDataAccessor)
        .update(testPath, dataUpdater, AccessOption.PERSISTENT);

    HelixUtils.compareAndUpdate(mockDataAccessor, testPath, 3, dataUpdater);

    verify(mockDataAccessor, times(3)).update(testPath, dataUpdater, AccessOption.PERSISTENT);
  }
}
