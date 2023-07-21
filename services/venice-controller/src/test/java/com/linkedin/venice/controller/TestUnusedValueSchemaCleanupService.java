package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class TestUnusedValueSchemaCleanupService {
  static final String SCHEMA = "\"string\"";

  private Store mockStore() {
    Store store = mock(Store.class);
    doReturn(Utils.getUniqueString()).when(store).getName();
    doReturn(1).when(store).getCurrentVersion();
    List<Version> versionList = new ArrayList<>();
    Version v = mock(Version.class);
    doReturn(null).when(v).getHybridStoreConfig();
    versionList.add(v);
    doReturn(versionList).when(store).getVersions();

    return store;
  }

  @Test
  public void testCleanupBackupVersionSleepValidation() throws Exception {
    VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);
    VeniceParentHelixAdmin parentHelixAdmin = mock(VeniceParentHelixAdmin.class);
    VeniceControllerMultiClusterConfig config = mock(VeniceControllerMultiClusterConfig.class);
    doReturn(1).when(config).getUnusedSchemaCleanupIntervalMinutes();
    doReturn(1).when(config).getMinSchemaCountToKeep();
    VeniceControllerConfig controllerConfig = mock(VeniceControllerConfig.class);
    doReturn(controllerConfig).when(config).getControllerConfig(any());
    doReturn(true).when(controllerConfig).isUnusedValueSchemaCleanupServiceEnabled();
    doReturn(true).when(admin).isLeaderControllerFor(any());
    Store store = mockStore();

    Set<String> stores = new HashSet<>();
    String clusterName = "test_cluster";
    stores.add(store.getName());
    Set<String> clusters = new HashSet<>();
    clusters.add(clusterName);
    doReturn(clusters).when(config).getClusters();
    List<Store> storeList = new ArrayList<>();
    storeList.add(store);
    doReturn(storeList).when(admin).getAllStores(any());
    Collection<SchemaEntry> schemaEntries = new ArrayList<>();
    schemaEntries.add(new SchemaEntry(1, SCHEMA));
    schemaEntries.add(new SchemaEntry(2, SCHEMA));
    schemaEntries.add(new SchemaEntry(3, SCHEMA));
    schemaEntries.add(new SchemaEntry(4, SCHEMA));

    doReturn(schemaEntries).when(parentHelixAdmin).getValueSchemas(anyString(), anyString());
    Set<Integer> schemaIds = new HashSet<>();
    schemaIds.add(3);
    schemaIds.add(4);

    doReturn(schemaIds).when(parentHelixAdmin).getInUseValueSchemaIds(anyString(), anyString());

    UnusedValueSchemaCleanupService service = new UnusedValueSchemaCleanupService(config, admin, parentHelixAdmin);

    service.startInner();
    Set<Integer> unusedSchemas = new HashSet<>();
    unusedSchemas.add(1);
    unusedSchemas.add(2);
    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> verify(admin, atLeast(1)).deleteValueSchemas(clusterName, store.getName(), unusedSchemas));
  }
}
