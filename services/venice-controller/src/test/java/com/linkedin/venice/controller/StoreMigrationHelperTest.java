package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.TestUtils;
import java.util.Collections;
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
  public void testMigrationPreservesWriteQuotaAtInitialCreation(boolean enabled) {
    StoreInfo source = StoreInfo.fromStore(TestUtils.createTestStore(STORE_NAME, "owner", 1L));
    source.setWriteQuotaEnabled(enabled);
    ControllerClient destination = mock(ControllerClient.class);
    when(destination.createNewStore(STORE_NAME, "owner", "\"string\"", "\"string\"", enabled))
        .thenReturn(new NewStoreResponse());
    when(destination.addValueSchema(anyString(), anyString())).thenReturn(new SchemaResponse());
    when(destination.updateStore(eq(STORE_NAME), any())).thenReturn(new ControllerResponse());

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

    verify(destination).createNewStore(STORE_NAME, "owner", "\"string\"", "\"string\"", enabled);
    ArgumentCaptor<UpdateStoreQueryParams> captor = ArgumentCaptor.forClass(UpdateStoreQueryParams.class);
    verify(destination).updateStore(eq(STORE_NAME), captor.capture());
    assertEquals(captor.getValue().getWriteQuotaEnabled(), Optional.of(enabled));
  }

  @Test
  public void testAllowsMigrationBetweenNonEncryptionClusters() {
    StoreMigrationHelper.validateEncryptionClusterMigration(false, false, SRC_CLUSTER, DEST_CLUSTER, STORE_NAME);
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
    assertTrue(exception.getMessage().contains("migration from or to an encryption cluster is not allowed"));
  }
}
