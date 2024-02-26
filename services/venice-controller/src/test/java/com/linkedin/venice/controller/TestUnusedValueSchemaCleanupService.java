package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.meta.ReadWriteSchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
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
  public void testCleanupUnusedSchema() throws Exception {
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

    String clusterName = "test_cluster";
    Set<String> clusters = new HashSet<>();
    HelixVeniceClusterResources helixVeniceClusterResources = mock(HelixVeniceClusterResources.class);
    ReadWriteSchemaRepository schemaRepository = mock(ReadWriteSchemaRepository.class);
    clusters.add(clusterName);
    doReturn(clusters).when(config).getClusters();
    List<Store> storeList = new ArrayList<>();
    storeList.add(store);
    doReturn(storeList).when(admin).getAllStores(any());
    doReturn(helixVeniceClusterResources).when(admin).getHelixVeniceClusterResources(anyString());
    doReturn(schemaRepository).when(helixVeniceClusterResources).getSchemaRepository();
    SchemaEntry schemaEntry = new SchemaEntry(4, SCHEMA);
    doReturn(schemaEntry).when(schemaRepository).getSupersetOrLatestValueSchema(anyString());
    Collection<SchemaEntry> schemaEntries = new ArrayList<>();
    schemaEntries.add(new SchemaEntry(1, SCHEMA));
    schemaEntries.add(new SchemaEntry(2, SCHEMA));
    schemaEntries.add(new SchemaEntry(3, SCHEMA));
    schemaEntries.add(new SchemaEntry(4, SCHEMA));
    Set<Integer> schemaIds = new HashSet<>();
    schemaIds.add(3);
    schemaIds.add(4);
    UnusedValueSchemaCleanupService service = new UnusedValueSchemaCleanupService(config, admin, parentHelixAdmin);
    service.startInner();
    Set<Integer> unusedSchemas = new HashSet<>();
    unusedSchemas.add(1);
    unusedSchemas.add(2);
    doReturn(Collections.emptySet()).when(parentHelixAdmin).getValueSchemas(anyString(), anyString());

    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> verify(admin, times(0)).deleteValueSchemas(clusterName, store.getName(), unusedSchemas));

    doReturn(schemaEntries).when(parentHelixAdmin).getValueSchemas(anyString(), anyString());
    doReturn(schemaIds).when(parentHelixAdmin).getInUseValueSchemaIds(anyString(), anyString());
    service.startInner();
    TestUtils.waitForNonDeterministicAssertion(
        10,
        TimeUnit.SECONDS,
        () -> verify(admin, times(1)).deleteValueSchemas(clusterName, store.getName(), unusedSchemas));
    TestUtils.waitForNonDeterministicAssertion(
        10,
        TimeUnit.SECONDS,
        () -> verify(parentHelixAdmin, times(1)).deleteValueSchemas(clusterName, store.getName(), unusedSchemas));
  }

  @Test
  void testGetUnusedSchema() {
    VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);
    VeniceParentHelixAdmin parentHelixAdmin = mock(VeniceParentHelixAdmin.class);
    VeniceControllerMultiClusterConfig config = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerConfig controllerConfig = mock(VeniceControllerConfig.class);
    doReturn(true).when(controllerConfig).isUnusedValueSchemaCleanupServiceEnabled();
    doReturn(true).when(admin).isLeaderControllerFor(any());
    Store store = mockStore();
    ReadWriteSchemaRepository schemaRepository = mock(ReadWriteSchemaRepository.class);
    List<SchemaEntry> schemaEntries = new ArrayList<>();
    schemaEntries.add(new SchemaEntry(1, SCHEMA));
    schemaEntries.add(new SchemaEntry(2, SCHEMA));
    schemaEntries.add(new SchemaEntry(3, SCHEMA));
    schemaEntries.add(new SchemaEntry(4, SCHEMA));
    schemaEntries.add(new SchemaEntry(5, SCHEMA));
    schemaEntries.add(new SchemaEntry(6, SCHEMA));

    Set<Integer> inuseSchemaIds = new HashSet<>();
    inuseSchemaIds.add(3);
    inuseSchemaIds.add(4);
    doReturn(new SchemaEntry(6, SCHEMA)).when(schemaRepository).getSupersetOrLatestValueSchema(anyString());
    UnusedValueSchemaCleanupService service = new UnusedValueSchemaCleanupService(config, admin, parentHelixAdmin);
    Set<Integer> unusedSchemas = service.findSchemaIdsToDelete(schemaEntries, store, schemaRepository, inuseSchemaIds);
    Assert.assertTrue(unusedSchemas.contains(1));
    Assert.assertTrue(unusedSchemas.contains(2));
    doReturn(5).when(store).getLatestSuperSetValueSchemaId();
    unusedSchemas = service.findSchemaIdsToDelete(schemaEntries, store, schemaRepository, inuseSchemaIds);
    Assert.assertFalse(unusedSchemas.contains(5));
    Assert.assertFalse(unusedSchemas.contains(6));
  }
}
