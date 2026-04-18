package com.linkedin.venice.controllerapi;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_STORE_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_MIGRATION;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.meta.IngestionPauseMode;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.testng.Assert;
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
    params.setEnumSchemaEvolutionAllowed(true);
    assertEquals(params.isEnumSchemaEvolutionAllowed(), Optional.of(Boolean.TRUE));
  }

  @Test
  public void testIngestionPauseModeRoundTrip() {
    for (IngestionPauseMode mode: IngestionPauseMode.values()) {
      UpdateStoreQueryParams params = new UpdateStoreQueryParams();
      params.setIngestionPauseMode(mode);
      Assert.assertEquals(params.getIngestionPauseMode(), Optional.of(mode));
    }
  }

  @Test
  public void testIngestionPausedRegionsRoundTrip() {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    List<String> regions = Arrays.asList("prod-lor1", "prod-ltx1");
    params.setIngestionPausedRegions(regions);
    Assert.assertEquals(params.getIngestionPausedRegions().get(), regions);
  }

  @Test
  public void testIngestionPausedRegionsEmpty() {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setIngestionPausedRegions(Collections.emptyList());
    Assert.assertTrue(params.getIngestionPausedRegions().get().isEmpty());
  }
}
