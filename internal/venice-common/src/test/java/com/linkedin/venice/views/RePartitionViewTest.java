package com.linkedin.venice.views;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ViewParameterKeys;
import com.linkedin.venice.partitioner.ConstantVenicePartitioner;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.testng.annotations.Test;


public class RePartitionViewTest {
  @Test
  public void testValidateConfigs() {
    Properties properties = new Properties();
    Map<String, String> viewParams = new HashMap<>();
    Store testStore = getMockStore("test-store", 12);
    // Fail due to missing view name
    assertThrows(() -> new RePartitionView(properties, testStore, viewParams).validateConfigs());
    viewParams.put(ViewParameterKeys.RE_PARTITION_VIEW_NAME.name(), "test-view");
    // Fail due to missing partition count
    assertThrows(() -> new RePartitionView(properties, testStore, viewParams).validateConfigs());
    viewParams.put(ViewParameterKeys.RE_PARTITION_VIEW_PARTITION_COUNT.name(), "12");
    // Fail due to same partitioner and partition count
    assertThrows(() -> new RePartitionView(properties, testStore, viewParams).validateConfigs());
    viewParams.put(
        ViewParameterKeys.RE_PARTITION_VIEW_PARTITIONER.name(),
        ConstantVenicePartitioner.class.getCanonicalName());
    // Pass, same partition count but different partitioner
    new RePartitionView(properties, testStore, viewParams).validateConfigs();
    viewParams.remove(ViewParameterKeys.RE_PARTITION_VIEW_PARTITIONER.name());
    viewParams.put(ViewParameterKeys.RE_PARTITION_VIEW_PARTITION_COUNT.name(), "24");
    // Pass, same partitioner but different partition count
    new RePartitionView(properties, testStore, viewParams).validateConfigs();
  }

  @Test
  public void testRePartitionViewTopicProcessing() {
    String storeName = "test-store";
    Map<String, String> viewParams = new HashMap<>();
    int version = 8;
    Store testStore = getMockStore(storeName, 6);
    String rePartitionViewName = "test-view";
    viewParams.put(ViewParameterKeys.RE_PARTITION_VIEW_NAME.name(), rePartitionViewName);
    viewParams.put(ViewParameterKeys.RE_PARTITION_VIEW_PARTITION_COUNT.name(), "24");
    RePartitionView rePartitionView = new RePartitionView(new Properties(), testStore, viewParams);
    Map<String, VeniceProperties> rePartitionViewTopicMap = rePartitionView.getTopicNamesAndConfigsForVersion(version);
    assertEquals(rePartitionViewTopicMap.size(), 1);
    for (Map.Entry<String, VeniceProperties> entry: rePartitionViewTopicMap.entrySet()) {
      String viewTopic = entry.getKey();
      assertTrue(viewTopic.contains(rePartitionViewName));
      assertTrue(VeniceView.isViewTopic(viewTopic));
      assertEquals(VeniceView.parseStoreFromViewTopic(viewTopic), storeName);
      assertEquals(VeniceView.parseVersionFromViewTopic(viewTopic), version);
    }
  }

  private Store getMockStore(String storeName, int partitionCount) {
    Store testStore = mock(Store.class);
    // We can remove this requirement from VeniceView into ChangeCaptureView once we refactor the ingestion path to
    // perform view related actions in L/F instead of A/A SIT.
    doReturn(true).when(testStore).isActiveActiveReplicationEnabled();
    doReturn(storeName).when(testStore).getName();
    doReturn(partitionCount).when(testStore).getPartitionCount();
    PartitionerConfig partitionerConfig = mock(PartitionerConfig.class);
    doReturn(DefaultVenicePartitioner.class.getCanonicalName()).when(partitionerConfig).getPartitionerClass();
    doReturn(partitionerConfig).when(testStore).getPartitionerConfig();
    return testStore;
  }
}