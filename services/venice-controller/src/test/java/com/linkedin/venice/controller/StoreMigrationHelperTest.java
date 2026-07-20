package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.TestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;


public class StoreMigrationHelperTest {
  private static final String STORE_NAME = "test-store";
  private static final String DEST_CLUSTER = "dest-cluster";
  private static final String LOCAL_REGION = "dc-0";
  private static final String KEY_SCHEMA = "\"string\"";

  @Test
  public void testEncryptionForcedWhenDestinationIsEncryptionCluster() {
    UpdateStoreQueryParams captured = runCloneAndCaptureUpdateParams(false, true);
    Optional<Boolean> encryptionEnabled = captured.getEncryptionEnabled();
    assertTrue(encryptionEnabled.isPresent(), "encryptionEnabled should be set on the migrated store");
    assertTrue(
        encryptionEnabled.get(),
        "encryptionEnabled must be forced to true when migrating into an encryption cluster");
  }

  @Test
  public void testEncryptionNotForcedWhenDestinationIsNotEncryptionCluster() {
    UpdateStoreQueryParams captured = runCloneAndCaptureUpdateParams(false, false);
    // Source store has encryption disabled and the destination is not an encryption cluster, so the
    // migrated store should keep encryption disabled.
    assertEquals(captured.getEncryptionEnabled(), Optional.of(false));
  }

  @Test
  public void testEncryptionStaysEnabledWhenSourceAlreadyEncrypted() {
    UpdateStoreQueryParams captured = runCloneAndCaptureUpdateParams(true, false);
    assertEquals(captured.getEncryptionEnabled(), Optional.of(true));
  }

  private UpdateStoreQueryParams runCloneAndCaptureUpdateParams(
      boolean sourceEncryptionEnabled,
      boolean enforceEncryptionEnabled) {
    Store srcStore = TestUtils.createTestStore(STORE_NAME, "owner", System.currentTimeMillis());
    srcStore.setEncryptionEnabled(sourceEncryptionEnabled);
    StoreInfo srcStoreInfo = StoreInfo.fromStore(srcStore);

    ControllerClient destControllerClient = mock(ControllerClient.class);
    NewStoreResponse newStoreResponse = mock(NewStoreResponse.class);
    doReturn(false).when(newStoreResponse).isError();
    doReturn(newStoreResponse).when(destControllerClient)
        .createNewStore(anyString(), anyString(), anyString(), anyString());

    SchemaResponse schemaResponse = mock(SchemaResponse.class);
    doReturn(false).when(schemaResponse).isError();
    doReturn(schemaResponse).when(destControllerClient).addValueSchema(anyString(), anyString());

    ControllerResponse updateStoreResponse = mock(ControllerResponse.class);
    doReturn(false).when(updateStoreResponse).isError();
    doReturn(updateStoreResponse).when(destControllerClient).updateStore(anyString(), any());

    Map<String, Map<String, StoreInfo>> srcStoresInChildColos =
        Collections.singletonMap(STORE_NAME, Collections.emptyMap());

    StoreMigrationHelper.cloneDestinationStoreAndSyncConfigs(
        destControllerClient,
        srcStoreInfo,
        KEY_SCHEMA,
        Collections.singletonList(new SchemaEntry(1, "\"int\"")),
        srcStoresInChildColos,
        DEST_CLUSTER,
        STORE_NAME,
        LOCAL_REGION,
        enforceEncryptionEnabled,
        mock(Logger.class));

    ArgumentCaptor<UpdateStoreQueryParams> paramsCaptor = ArgumentCaptor.forClass(UpdateStoreQueryParams.class);
    verify(destControllerClient).updateStore(eq(STORE_NAME), paramsCaptor.capture());
    return paramsCaptor.getValue();
  }
}
