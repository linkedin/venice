package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.TestUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class StoreMigrationHelperTest {
  private static final String SRC_CLUSTER = "src-cluster";
  private static final String DEST_CLUSTER = "dest-cluster";
  private static final String STORE_NAME = "test-store";

  @DataProvider(name = "writeQuotaEnabledValues")
  public Object[][] writeQuotaEnabledValues() {
    return new Object[][] { { true }, { false } };
  }

  @Test(dataProvider = "writeQuotaEnabledValues")
  public void testMigrationRestoresWriteQuotaThroughUpdate(boolean enabled) {
    StoreInfo source = StoreInfo.fromStore(TestUtils.createTestStore(STORE_NAME, "owner", 1L));
    source.setWriteQuotaEnabled(enabled);
    ControllerClient destination = destinationController();

    StoreMigrationHelper.cloneDestinationStoreAndSyncConfigs(
        destination,
        source,
        "\"string\"",
        Collections.singletonList(new SchemaEntry(1, "\"string\"")),
        Collections.singletonMap(STORE_NAME, Collections.emptyMap()),
        DEST_CLUSTER,
        STORE_NAME,
        "region",
        LogManager.getLogger(StoreMigrationHelperTest.class));

    verify(destination).createNewStore(STORE_NAME, "owner", "\"string\"", "\"string\"");
    ArgumentCaptor<UpdateStoreQueryParams> captor = ArgumentCaptor.forClass(UpdateStoreQueryParams.class);
    verify(destination).updateStore(eq(STORE_NAME), captor.capture());
    assertEquals(captor.getValue().getWriteQuotaEnabled(), Optional.of(enabled));
  }

  @Test
  public void testMigrationPropagatesPubSubEncryptionKeyUrn() {
    String urn = "urn:li:pubSubEncryptionKey:test-key";
    Store store = TestUtils.createTestStore(STORE_NAME, "owner", 1L);
    store.setEncryptionEnabled(true);
    StoreInfo source = StoreInfo.fromStore(store);
    source.setPubSubEncryptionKeyUrn(urn);
    ControllerClient destination = destinationController();

    StoreMigrationHelper.cloneDestinationStoreAndSyncConfigs(
        destination,
        source,
        "\"string\"",
        Collections.singletonList(new SchemaEntry(1, "\"string\"")),
        Collections.singletonMap(STORE_NAME, Collections.emptyMap()),
        DEST_CLUSTER,
        STORE_NAME,
        "region",
        LogManager.getLogger(StoreMigrationHelperTest.class));

    ArgumentCaptor<UpdateStoreQueryParams> captor = ArgumentCaptor.forClass(UpdateStoreQueryParams.class);
    verify(destination).updateStore(eq(STORE_NAME), captor.capture());
    assertEquals(captor.getValue().getPubSubEncryptionKeyUrn(), Optional.of(urn));
  }

  private ControllerClient destinationController() {
    ControllerClient destination = mock(ControllerClient.class);
    when(destination.createNewStore(STORE_NAME, "owner", "\"string\"", "\"string\""))
        .thenReturn(new NewStoreResponse());
    when(destination.addValueSchema(anyString(), anyString(), anyInt())).thenAnswer(invocation -> {
      SchemaResponse response = new SchemaResponse();
      response.setId(invocation.getArgument(2));
      return response;
    });
    when(destination.updateStore(eq(STORE_NAME), any())).thenReturn(new ControllerResponse());
    return destination;
  }

  @Test
  public void testClonePreservesSourceIdsAndSuperset() {
    List<SchemaEntry> schemas = Arrays.asList(new SchemaEntry(274, "\"string\""), new SchemaEntry(315, "\"string\""));
    ControllerClient destination = destinationController();
    StoreInfo source = StoreInfo.fromStore(TestUtils.createTestStore(STORE_NAME, "owner", 1L));
    source.setLatestSuperSetValueSchemaId(315);
    StoreMigrationHelper.cloneDestinationStoreAndSyncConfigs(
        destination,
        source,
        "\"string\"",
        schemas,
        Collections.singletonMap(STORE_NAME, Collections.emptyMap()),
        DEST_CLUSTER,
        STORE_NAME,
        "region",
        LogManager.getLogger(StoreMigrationHelperTest.class));
    verify(destination).createNewStore(STORE_NAME, "owner", "\"string\"", "\"string\"");
    for (SchemaEntry schema: schemas) {
      verify(destination).addValueSchema(STORE_NAME, "\"string\"", schema.getId());
    }
    ArgumentCaptor<UpdateStoreQueryParams> configs = ArgumentCaptor.forClass(UpdateStoreQueryParams.class);
    verify(destination).updateStore(eq(STORE_NAME), configs.capture());
    assertEquals(configs.getValue().getLatestSupersetSchemaId(), Optional.of(315));
  }

  @Test
  public void testCloneRejectsRenumberedSchemaBeforeConfigUpdate() {
    ControllerClient destination = destinationController();
    SchemaResponse renumbered = new SchemaResponse();
    renumbered.setId(1);
    when(destination.addValueSchema(STORE_NAME, "\"string\"", 274)).thenReturn(renumbered);
    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> StoreMigrationHelper.cloneDestinationStoreAndSyncConfigs(
            destination,
            StoreInfo.fromStore(TestUtils.createTestStore(STORE_NAME, "owner", 1L)),
            "\"string\"",
            Collections.singletonList(new SchemaEntry(274, "\"string\"")),
            Collections.singletonMap(STORE_NAME, Collections.emptyMap()),
            DEST_CLUSTER,
            STORE_NAME,
            "region",
            LogManager.getLogger(StoreMigrationHelperTest.class)));
    assertTrue(exception.getMessage().contains("Returned ID 1"));
    verify(destination, never()).updateStore(anyString(), any());
  }

  @Test
  public void testAllowsMigrationBetweenNonEncryptionClusters() {
    StoreMigrationHelper.validateEncryptionClusterMigration(false, false, SRC_CLUSTER, DEST_CLUSTER, STORE_NAME);
  }

  @Test
  public void testAllowsMigrationBetweenEncryptionClusters() {
    StoreMigrationHelper.validateEncryptionClusterMigration(true, true, SRC_CLUSTER, DEST_CLUSTER, STORE_NAME);
  }

  @Test
  public void testBlocksMigrationFromEncryptionCluster() {
    assertEncryptionClusterMigrationBlocked(true, false);
  }

  @Test
  public void testBlocksMigrationToEncryptionCluster() {
    assertEncryptionClusterMigrationBlocked(false, true);
  }

  private void assertEncryptionClusterMigrationBlocked(boolean srcEncryptionCluster, boolean destEncryptionCluster) {
    VeniceHttpException exception = expectThrows(
        VeniceHttpException.class,
        () -> StoreMigrationHelper.validateEncryptionClusterMigration(
            srcEncryptionCluster,
            destEncryptionCluster,
            SRC_CLUSTER,
            DEST_CLUSTER,
            STORE_NAME));
    assertTrue(exception.getHttpStatusCode() == HttpStatus.SC_BAD_REQUEST);
    assertTrue(
        exception.getMessage()
            .contains("migrating between an encryption cluster and a non-encryption cluster is not allowed"));
  }
}
