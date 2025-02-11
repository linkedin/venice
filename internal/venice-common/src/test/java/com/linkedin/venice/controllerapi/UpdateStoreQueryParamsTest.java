package com.linkedin.venice.controllerapi;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_STORE_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_MIGRATION;
import static org.testng.Assert.assertEquals;

import java.util.Optional;
import org.testng.annotations.Test;


public class UpdateStoreQueryParamsTest {
  @Test
  public void testSetStoreMigrationSetsBothKeys() {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setStoreMigration(true);
    assertEquals(params.getStoreMigration(), Optional.of(Boolean.TRUE));
    assertEquals(params.getString(STORE_MIGRATION).orElse(null), "true");
    assertEquals(params.getString(ENABLE_STORE_MIGRATION).orElse(null), "true");

    params = new UpdateStoreQueryParams();
    params.setStoreMigration(false);
    assertEquals(params.getStoreMigration(), Optional.of(Boolean.FALSE));
    assertEquals(params.getString(STORE_MIGRATION).orElse(null), "false");
    assertEquals(params.getString(ENABLE_STORE_MIGRATION).orElse(null), "false");
  }
}
