package com.linkedin.venice.controller.server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaData;
import java.util.Optional;
import org.testng.annotations.Test;


public class SchemaRoutesTest {
  @Test
  public void schemaMismatchErrorMessage() {
    String cluster = "cluster_name";
    String store = "store_name";
    String schemaStr = "int";

    Admin admin = mock(Admin.class);
    when(admin.getValueSchemaId(cluster, store, schemaStr)).thenReturn(SchemaData.INVALID_VALUE_SCHEMA_ID);
    when(admin.getStore(cluster, store)).thenReturn(null);
    SchemaRoutes schemaRoutes = new SchemaRoutes(false, Optional.empty(), Optional.empty(), Optional.empty());
    try {
      schemaRoutes.populateSchemaResponseForValueOrDerivedSchemaID(admin, cluster, store, schemaStr);
    } catch (VeniceNoStoreException e) {
      assertTrue(
          e.getMessage().contains("Store: store_name does not exist in cluster cluster_name"),
          "Got this instead: " + e.getMessage());
    }

    Store storeInfo = mock(Store.class);
    when(admin.getStore(cluster, store)).thenReturn(storeInfo);

    when(storeInfo.isWriteComputationEnabled()).thenReturn(false);

    try {
      schemaRoutes.populateSchemaResponseForValueOrDerivedSchemaID(admin, cluster, store, schemaStr);
    } catch (VeniceException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "Can not find any registered value schema for the store store_name that matches the schema of data being pushed."),
          "Got this instead: " + e.getMessage());
    }

    when(storeInfo.isWriteComputationEnabled()).thenReturn(true);
    when(admin.getDerivedSchemaId(cluster, store, schemaStr)).thenReturn(GeneratedSchemaID.INVALID);

    try {
      schemaRoutes.populateSchemaResponseForValueOrDerivedSchemaID(admin, cluster, store, schemaStr);
    } catch (VeniceException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "Can not find any registered value schema nor derived schema for the store store_name that matches the schema of data being pushed."),
          "Got this instead: " + e.getMessage());
    }
  }

}
