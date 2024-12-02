package com.linkedin.venice.views;

import static com.linkedin.venice.partitioner.ConstantVenicePartitioner.CONSTANT_PARTITION;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.venice.meta.MaterializedViewParameters;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.partitioner.ConstantVenicePartitioner;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.testng.annotations.Test;


public class TestMaterializedView {
  @Test
  public void testValidateConfigs() {
    Properties properties = new Properties();
    Map<String, String> viewParams = new HashMap<>();
    String storeName = "test-store";
    Store testStore = getMockStore("test-store", 12);
    // Fail due to missing view name
    assertThrows(() -> new MaterializedView(properties, storeName, viewParams).validateConfigs(testStore));
    viewParams.put(MaterializedViewParameters.MATERIALIZED_VIEW_NAME.name(), "test-view");
    // Fail due to missing partition count
    assertThrows(() -> new MaterializedView(properties, storeName, viewParams).validateConfigs(testStore));
    viewParams.put(MaterializedViewParameters.MATERIALIZED_VIEW_PARTITION_COUNT.name(), "12");
    // Fail due to same partitioner and partition count
    assertThrows(() -> new MaterializedView(properties, storeName, viewParams).validateConfigs(testStore));
    viewParams.put(
        MaterializedViewParameters.MATERIALIZED_VIEW_PARTITIONER.name(),
        ConstantVenicePartitioner.class.getCanonicalName());
    // Pass, same partition count but different partitioner
    new MaterializedView(properties, storeName, viewParams).validateConfigs(testStore);
    viewParams.put(MaterializedViewParameters.MATERIALIZED_VIEW_PARTITION_COUNT.name(), "24");
    // Pass, same partitioner but different partition count
    new MaterializedView(properties, storeName, viewParams).validateConfigs(testStore);
    viewParams.put(
        MaterializedViewParameters.MATERIALIZED_VIEW_PARTITIONER.name(),
        ConstantVenicePartitioner.class.getCanonicalName() + "DNE");
    // Fail due to invalid partitioner class
    assertThrows(() -> new MaterializedView(properties, storeName, viewParams).validateConfigs(testStore));

    viewParams.put(MaterializedViewParameters.MATERIALIZED_VIEW_PARTITION_COUNT.name(), "12");
    viewParams.put(
        MaterializedViewParameters.MATERIALIZED_VIEW_PARTITIONER.name(),
        ConstantVenicePartitioner.class.getCanonicalName());
    String newStoreName = "test-store-existing-config";
    Store storeWithExistingViews = getMockStore(newStoreName, 12);
    ViewConfig viewConfig = mock(ViewConfig.class);
    doReturn(Collections.singletonMap("test-view", viewConfig)).when(storeWithExistingViews).getViewConfigs();
    // Fail due to same view name
    assertThrows(
        () -> new MaterializedView(properties, newStoreName, viewParams).validateConfigs(storeWithExistingViews));
    Map<String, String> existingViewConfigParams = new HashMap<>();
    existingViewConfigParams.put(
        MaterializedViewParameters.MATERIALIZED_VIEW_PARTITIONER.name(),
        ConstantVenicePartitioner.class.getCanonicalName());
    existingViewConfigParams
        .put(MaterializedViewParameters.MATERIALIZED_VIEW_PARTITION_COUNT.name(), Integer.toString(12));
    doReturn(existingViewConfigParams).when(viewConfig).getViewParameters();
    doReturn(MaterializedView.class.getCanonicalName()).when(viewConfig).getViewClassName();
    doReturn(Collections.singletonMap("old-view", viewConfig)).when(storeWithExistingViews).getViewConfigs();
    // Fail due to existing identical view config
    assertThrows(
        () -> new MaterializedView(properties, newStoreName, viewParams).validateConfigs(storeWithExistingViews));
    existingViewConfigParams
        .put(MaterializedViewParameters.MATERIALIZED_VIEW_PARTITION_COUNT.name(), Integer.toString(36));
    // Pass, same partitioner but different partition count
    new MaterializedView(properties, storeName, viewParams).validateConfigs(testStore);
  }

  @Test
  public void testRePartitionViewTopicProcessing() {
    String storeName = "test-store";
    Map<String, String> viewParams = new HashMap<>();
    int version = 8;
    String rePartitionViewName = "test-view";
    viewParams.put(MaterializedViewParameters.MATERIALIZED_VIEW_NAME.name(), rePartitionViewName);
    viewParams.put(MaterializedViewParameters.MATERIALIZED_VIEW_PARTITION_COUNT.name(), "24");
    MaterializedView materializedView = new MaterializedView(new Properties(), storeName, viewParams);
    Map<String, VeniceProperties> rePartitionViewTopicMap = materializedView.getTopicNamesAndConfigsForVersion(version);
    assertEquals(rePartitionViewTopicMap.size(), 1);
    for (Map.Entry<String, VeniceProperties> entry: rePartitionViewTopicMap.entrySet()) {
      String viewTopic = entry.getKey();
      assertTrue(viewTopic.contains(rePartitionViewName));
      assertTrue(VeniceView.isViewTopic(viewTopic));
      assertEquals(VeniceView.parseStoreFromViewTopic(viewTopic), storeName);
      assertEquals(VeniceView.parseVersionFromViewTopic(viewTopic), version);
    }
  }

  @Test
  public void testViewConfigMapFlattening() throws JsonProcessingException {
    String viewName = "testView1";
    String viewName2 = "testView2";
    MaterializedViewParameters.Builder builder = new MaterializedViewParameters.Builder(viewName);
    builder.setPartitionCount(3).setPartitioner(DefaultVenicePartitioner.class.getCanonicalName());
    ViewConfig viewConfig = new ViewConfigImpl(MaterializedView.class.getCanonicalName(), builder.build());
    MaterializedViewParameters.Builder builder2 = new MaterializedViewParameters.Builder(viewName2);
    builder2.setPartitionCount(6)
        .setPartitioner(ConstantVenicePartitioner.class.getCanonicalName())
        .setPartitionerParams(Collections.singletonMap(CONSTANT_PARTITION, String.valueOf(0)));
    ViewConfig viewConfig2 = new ViewConfigImpl(MaterializedView.class.getCanonicalName(), builder2.build());
    Map<String, ViewConfig> storeViewConfigMap = new HashMap<>();
    storeViewConfigMap.put(viewName, viewConfig);
    storeViewConfigMap.put(viewName2, viewConfig2);
    // Flatten the config map to a string, parse it back and ensure it can still be used to create the correct views
    String flattenString = ViewUtils.flatViewConfigMapString(storeViewConfigMap);
    Map<String, ViewConfig> parsedViewConfigMap = ViewUtils.parseViewConfigMapString(flattenString);
    ViewConfig parsedViewConfig = parsedViewConfigMap.get(viewName);
    ViewConfig parsedViewConfig2 = parsedViewConfigMap.get(viewName2);
    VeniceView view = ViewUtils.getVeniceView(
        parsedViewConfig.getViewClassName(),
        new Properties(),
        "testStore",
        parsedViewConfig.getViewParameters());
    VeniceView view2 = ViewUtils.getVeniceView(
        parsedViewConfig2.getViewClassName(),
        new Properties(),
        "testStore",
        parsedViewConfig2.getViewParameters());
    assertTrue(view instanceof MaterializedView);
    assertEquals(((MaterializedView) view).getViewPartitionCount(), 3);
    assertTrue(((MaterializedView) view).getViewPartitioner() instanceof DefaultVenicePartitioner);
    assertTrue(view2 instanceof MaterializedView);
    assertEquals(((MaterializedView) view2).getViewPartitionCount(), 6);
    assertTrue(((MaterializedView) view2).getViewPartitioner() instanceof ConstantVenicePartitioner);
  }

  @Test
  public void testGetWriterOptionsBuilder() {
    MaterializedViewParameters.Builder builder = new MaterializedViewParameters.Builder("testView");
    builder.setPartitioner(DefaultVenicePartitioner.class.getCanonicalName()).setPartitionCount(3);
    VeniceView view = ViewUtils
        .getVeniceView(MaterializedView.class.getCanonicalName(), new Properties(), "testStore", builder.build());
    Version version = mock(Version.class);
    doReturn(true).when(version).isRmdChunkingEnabled();
    doReturn(true).when(version).isChunkingEnabled();
    String viewTopic = "dummyViewTopic";
    VeniceWriterOptions options = view.getWriterOptionsBuilder(viewTopic, version).build();
    assertEquals(options.getTopicName(), viewTopic);
    assertEquals(options.getPartitionCount().intValue(), 3);
    assertTrue(options.getPartitioner() instanceof DefaultVenicePartitioner);
    assertTrue(options.isChunkingEnabled());
    assertTrue(options.isRmdChunkingEnabled());
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
