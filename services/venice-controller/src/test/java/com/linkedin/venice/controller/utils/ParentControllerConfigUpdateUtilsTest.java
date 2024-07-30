package com.linkedin.venice.controller.utils;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.controller.HelixVeniceClusterResources;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.util.ParentControllerConfigUpdateUtils;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.TestWriteUtils;
import java.util.Collections;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ParentControllerConfigUpdateUtilsTest {
  @Test
  public void testPartialUpdateConfigUpdate() {
    VeniceParentHelixAdmin parentHelixAdmin = mock(VeniceParentHelixAdmin.class);
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    String cluster = "foo";
    String storeName = "bar";
    Store store = mock(Store.class);
    when(parentHelixAdmin.getVeniceHelixAdmin()).thenReturn(veniceHelixAdmin);
    when(veniceHelixAdmin.getStore(anyString(), anyString())).thenReturn(store);
    HelixVeniceClusterResources helixVeniceClusterResources = mock(HelixVeniceClusterResources.class);
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
    when(helixVeniceClusterResources.getConfig()).thenReturn(controllerConfig);
    when(veniceHelixAdmin.getHelixVeniceClusterResources(anyString())).thenReturn(helixVeniceClusterResources);
    SchemaEntry schemaEntry = new SchemaEntry(1, TestWriteUtils.USER_WITH_DEFAULT_SCHEMA);
    when(veniceHelixAdmin.getValueSchemas(anyString(), anyString())).thenReturn(Collections.singletonList(schemaEntry));
    when(parentHelixAdmin.getValueSchemas(anyString(), anyString())).thenReturn(Collections.singletonList(schemaEntry));

    /**
     * Explicit request.
     */
    Optional<Boolean> partialUpdateRequest = Optional.of(true);
    // Case 1: partial update config updated.
    UpdateStore setStore = new UpdateStore();
    Assert.assertTrue(
        ParentControllerConfigUpdateUtils.checkAndMaybeApplyPartialUpdateConfig(
            parentHelixAdmin,
            cluster,
            storeName,
            partialUpdateRequest,
            setStore,
            true));
    // Case 2: partial update config updated.
    setStore = new UpdateStore();
    when(store.isWriteComputationEnabled()).thenReturn(true);
    Assert.assertTrue(
        ParentControllerConfigUpdateUtils.checkAndMaybeApplyPartialUpdateConfig(
            parentHelixAdmin,
            cluster,
            storeName,
            partialUpdateRequest,
            setStore,
            true));
    // Case 3: partial update config updated.
    partialUpdateRequest = Optional.of(false);
    when(store.isWriteComputationEnabled()).thenReturn(true);
    Assert.assertTrue(
        ParentControllerConfigUpdateUtils.checkAndMaybeApplyPartialUpdateConfig(
            parentHelixAdmin,
            cluster,
            storeName,
            partialUpdateRequest,
            setStore,
            true));
    // Case 4: partial update config updated.
    setStore = new UpdateStore();
    when(store.isWriteComputationEnabled()).thenReturn(false);
    Assert.assertTrue(
        ParentControllerConfigUpdateUtils.checkAndMaybeApplyPartialUpdateConfig(
            parentHelixAdmin,
            cluster,
            storeName,
            partialUpdateRequest,
            setStore,
            true));

    /**
     * No request.
     */
    partialUpdateRequest = Optional.empty();
    when(controllerConfig.isEnablePartialUpdateForHybridActiveActiveUserStores()).thenReturn(false);
    when(controllerConfig.isEnablePartialUpdateForHybridNonActiveActiveUserStores()).thenReturn(false);
    // Case 1: partial update config not updated.
    setStore = new UpdateStore();
    Assert.assertFalse(
        ParentControllerConfigUpdateUtils.checkAndMaybeApplyPartialUpdateConfig(
            parentHelixAdmin,
            cluster,
            storeName,
            partialUpdateRequest,
            setStore,
            true));
    setStore.activeActiveReplicationEnabled = true;
    Assert.assertFalse(
        ParentControllerConfigUpdateUtils.checkAndMaybeApplyPartialUpdateConfig(
            parentHelixAdmin,
            cluster,
            storeName,
            partialUpdateRequest,
            setStore,
            true));
    // Case 2: partial update config updated.
    when(controllerConfig.isEnablePartialUpdateForHybridActiveActiveUserStores()).thenReturn(true);
    when(controllerConfig.isEnablePartialUpdateForHybridNonActiveActiveUserStores()).thenReturn(true);
    setStore = new UpdateStore();
    Assert.assertTrue(
        ParentControllerConfigUpdateUtils.checkAndMaybeApplyPartialUpdateConfig(
            parentHelixAdmin,
            cluster,
            storeName,
            partialUpdateRequest,
            setStore,
            true));
    setStore.activeActiveReplicationEnabled = true;
    Assert.assertTrue(
        ParentControllerConfigUpdateUtils.checkAndMaybeApplyPartialUpdateConfig(
            parentHelixAdmin,
            cluster,
            storeName,
            partialUpdateRequest,
            setStore,
            true));
  }

  @Test
  public void testChunkingConfigUpdate() {
    VeniceParentHelixAdmin parentHelixAdmin = mock(VeniceParentHelixAdmin.class);
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    String cluster = "foo";
    String storeName = "bar";
    Store store = mock(Store.class);
    when(parentHelixAdmin.getVeniceHelixAdmin()).thenReturn(veniceHelixAdmin);
    when(veniceHelixAdmin.getStore(anyString(), anyString())).thenReturn(store);

    /**
     * Explicit request.
     */
    Optional<Boolean> chunkingRequest = Optional.of(true);
    when(store.isChunkingEnabled()).thenReturn(false);
    // Case 1: chunking config updated.
    UpdateStore setStore = new UpdateStore();
    setStore.chunkingEnabled = false;
    Assert.assertTrue(
        ParentControllerConfigUpdateUtils
            .checkAndMaybeApplyChunkingConfigChange(parentHelixAdmin, cluster, storeName, chunkingRequest, setStore));
    // Case 2: chunking config updated.
    setStore = new UpdateStore();
    setStore.chunkingEnabled = false;
    when(store.isChunkingEnabled()).thenReturn(true);
    Assert.assertTrue(
        ParentControllerConfigUpdateUtils
            .checkAndMaybeApplyChunkingConfigChange(parentHelixAdmin, cluster, storeName, chunkingRequest, setStore));
    // Case 3: chunking config updated.
    chunkingRequest = Optional.of(false);
    when(store.isChunkingEnabled()).thenReturn(true);
    Assert.assertTrue(
        ParentControllerConfigUpdateUtils
            .checkAndMaybeApplyChunkingConfigChange(parentHelixAdmin, cluster, storeName, chunkingRequest, setStore));
    // Case 4: chunking config updated.
    setStore = new UpdateStore();
    when(store.isChunkingEnabled()).thenReturn(false);
    Assert.assertTrue(
        ParentControllerConfigUpdateUtils
            .checkAndMaybeApplyChunkingConfigChange(parentHelixAdmin, cluster, storeName, chunkingRequest, setStore));
    /**
     * No request.
     */
    chunkingRequest = Optional.empty();
    when(store.isWriteComputationEnabled()).thenReturn(false);
    // Case 1: already enabled, chunking config not updated.
    when(store.isChunkingEnabled()).thenReturn(true);
    setStore = new UpdateStore();
    setStore.writeComputationEnabled = true;
    Assert.assertFalse(
        ParentControllerConfigUpdateUtils
            .checkAndMaybeApplyChunkingConfigChange(parentHelixAdmin, cluster, storeName, chunkingRequest, setStore));
    // Case 2: chunking config updated.
    when(store.isChunkingEnabled()).thenReturn(false);
    setStore = new UpdateStore();
    setStore.writeComputationEnabled = true;
    Assert.assertTrue(
        ParentControllerConfigUpdateUtils
            .checkAndMaybeApplyChunkingConfigChange(parentHelixAdmin, cluster, storeName, chunkingRequest, setStore));
  }

  @Test
  public void testRmdChunkingConfigUpdate() {
    VeniceParentHelixAdmin parentHelixAdmin = mock(VeniceParentHelixAdmin.class);
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    String cluster = "foo";
    String storeName = "bar";
    Store store = mock(Store.class);
    when(parentHelixAdmin.getVeniceHelixAdmin()).thenReturn(veniceHelixAdmin);
    when(veniceHelixAdmin.getStore(anyString(), anyString())).thenReturn(store);

    /**
     * Explicit request.
     */
    Optional<Boolean> chunkingRequest = Optional.of(true);
    when(store.isChunkingEnabled()).thenReturn(false);
    // Case 1: chunking config updated.
    UpdateStore setStore = new UpdateStore();
    setStore.chunkingEnabled = false;
    Assert.assertTrue(
        ParentControllerConfigUpdateUtils.checkAndMaybeApplyRmdChunkingConfigChange(
            parentHelixAdmin,
            cluster,
            storeName,
            chunkingRequest,
            setStore));
    // Case 2: chunking config updated.
    setStore = new UpdateStore();
    setStore.chunkingEnabled = false;
    when(store.isChunkingEnabled()).thenReturn(true);
    Assert.assertTrue(
        ParentControllerConfigUpdateUtils.checkAndMaybeApplyRmdChunkingConfigChange(
            parentHelixAdmin,
            cluster,
            storeName,
            chunkingRequest,
            setStore));
    // Case 3: chunking config updated.
    chunkingRequest = Optional.of(false);
    when(store.isChunkingEnabled()).thenReturn(true);
    Assert.assertTrue(
        ParentControllerConfigUpdateUtils.checkAndMaybeApplyRmdChunkingConfigChange(
            parentHelixAdmin,
            cluster,
            storeName,
            chunkingRequest,
            setStore));
    // Case 4: chunking config updated.
    setStore = new UpdateStore();
    when(store.isChunkingEnabled()).thenReturn(false);
    Assert.assertTrue(
        ParentControllerConfigUpdateUtils.checkAndMaybeApplyRmdChunkingConfigChange(
            parentHelixAdmin,
            cluster,
            storeName,
            chunkingRequest,
            setStore));

    /**
     * No request.
     */
    chunkingRequest = Optional.empty();
    when(store.isWriteComputationEnabled()).thenReturn(false);
    // Case 1: already enabled, chunking config not updated.
    when(store.isChunkingEnabled()).thenReturn(true);
    setStore = new UpdateStore();
    setStore.writeComputationEnabled = true;
    Assert.assertFalse(
        ParentControllerConfigUpdateUtils.checkAndMaybeApplyRmdChunkingConfigChange(
            parentHelixAdmin,
            cluster,
            storeName,
            chunkingRequest,
            setStore));
    // Case 2: chunking config not updated.
    when(store.isChunkingEnabled()).thenReturn(false);
    setStore = new UpdateStore();
    setStore.writeComputationEnabled = true;
    setStore.activeActiveReplicationEnabled = false;
    Assert.assertFalse(
        ParentControllerConfigUpdateUtils.checkAndMaybeApplyRmdChunkingConfigChange(
            parentHelixAdmin,
            cluster,
            storeName,
            chunkingRequest,
            setStore));
    // Case 3: chunking config not updated.
    when(store.isChunkingEnabled()).thenReturn(false);
    setStore = new UpdateStore();
    setStore.writeComputationEnabled = false;
    setStore.activeActiveReplicationEnabled = true;
    Assert.assertFalse(
        ParentControllerConfigUpdateUtils.checkAndMaybeApplyRmdChunkingConfigChange(
            parentHelixAdmin,
            cluster,
            storeName,
            chunkingRequest,
            setStore));
    // Case 4: chunking config updated.
    when(store.isChunkingEnabled()).thenReturn(false);
    setStore = new UpdateStore();
    setStore.writeComputationEnabled = true;
    setStore.activeActiveReplicationEnabled = true;
    Assert.assertTrue(
        ParentControllerConfigUpdateUtils.checkAndMaybeApplyRmdChunkingConfigChange(
            parentHelixAdmin,
            cluster,
            storeName,
            chunkingRequest,
            setStore));

  }
}
