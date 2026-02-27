package com.linkedin.venice.meta;

import static com.linkedin.venice.views.MaterializedView.MATERIALIZED_VIEW_TOPIC_SUFFIX;
import static com.linkedin.venice.views.VeniceView.VIEW_NAME_SEPARATOR;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.StoreVersionNotFoundException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.partitioner.ConstantVenicePartitioner;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.views.VeniceView;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class ReadOnlyViewStoreTest {
  @Test
  public void testViewStoreAndVersions() {
    String storeName = Utils.getUniqueString("testStore");
    String viewName = "testView";
    String viewStoreName = VeniceView.getViewStoreName(storeName, viewName);
    Store store = mock(Store.class);
    List<Version> storeVersions = new ArrayList<>();
    Version storeVersion = new VersionImpl(storeName, 1, "dummyId");
    Version storeVersion2 = new VersionImpl(storeName, 2, "dummyId2");
    storeVersions.add(storeVersion);
    storeVersions.add(storeVersion2);
    int storePartitionCount = 6;
    PartitionerConfig storePartitionerConfig = new PartitionerConfigImpl();
    storePartitionerConfig.setPartitionerClass(DefaultVenicePartitioner.class.getCanonicalName());
    for (Version version: storeVersions) {
      version.setPartitionCount(storePartitionCount);
      version.setPartitionerConfig(storePartitionerConfig);
      version.setChunkingEnabled(true);
      version.setRmdChunkingEnabled(true);
    }
    MaterializedViewParameters.Builder builder = new MaterializedViewParameters.Builder(viewName);
    builder.setPartitionCount(12).setPartitioner(ConstantVenicePartitioner.class.getCanonicalName());
    ViewConfig viewConfig = new ViewConfigImpl(MaterializedView.class.getCanonicalName(), builder.build());
    Map<String, ViewConfig> storeViewConfigMap = new HashMap<>();
    storeViewConfigMap.put(viewName, viewConfig);
    storeVersion2.setViewConfigs(storeViewConfigMap);
    doReturn(storeVersions).when(store).getVersions();
    doReturn(storeName).when(store).getName();
    doReturn(2).when(store).getCurrentVersion();

    Store viewStore = new ReadOnlyViewStore(store, viewStoreName);
    assertEquals(viewStore.getName(), viewStoreName);
    // v1 should be filtered since it doesn't have view configs
    assertEquals(viewStore.getVersions().size(), 1);
    assertNull(viewStore.getVersion(1));
    assertThrows(StoreVersionNotFoundException.class, () -> viewStore.getVersionOrThrow(1));
    Version viewStoreVersion = viewStore.getVersionOrThrow(viewStore.getCurrentVersion());
    assertNotNull(viewStoreVersion);
    assertTrue(viewStoreVersion instanceof ReadOnlyViewStore.ReadOnlyMaterializedViewVersion);
    assertEquals(viewStoreVersion.getPartitionCount(), 12);
    assertEquals(
        viewStoreVersion.getPartitionerConfig().getPartitionerClass(),
        ConstantVenicePartitioner.class.getCanonicalName());
    String viewStoreVersionTopicName =
        Version.composeKafkaTopic(storeName, 2) + VIEW_NAME_SEPARATOR + viewName + MATERIALIZED_VIEW_TOPIC_SUFFIX;
    assertEquals(viewStoreVersion.kafkaTopicName(), viewStoreVersionTopicName);
    assertTrue(viewStoreVersion.isChunkingEnabled());
    assertTrue(viewStoreVersion.isRmdChunkingEnabled());
  }

  @Test
  public void testInvalidViewConfig() {
    String storeName = Utils.getUniqueString("testStore");
    Version version = new VersionImpl(storeName, 1, "dummyId");
    assertThrows(VeniceException.class, () -> new ReadOnlyViewStore.ReadOnlyMaterializedViewVersion(version, null));
  }
}
