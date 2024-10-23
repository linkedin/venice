package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SCHEMA_COMPAT_TYPE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SCHEMA_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VALUE_SCHEMA;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import spark.Request;
import spark.Response;
import spark.Route;


public class SchemaRoutesTest {
  private VeniceControllerRequestHandler requestHandler;
  private Admin mockAdmin;

  @BeforeMethod
  public void setUp() {
    mockAdmin = mock(VeniceParentHelixAdmin.class);
    ControllerRequestHandlerDependencies dependencies = mock(ControllerRequestHandlerDependencies.class);
    doReturn(mockAdmin).when(dependencies).getAdmin();
    requestHandler = new VeniceControllerRequestHandler(dependencies);
  }

  @Test
  public void schemaMismatchErrorMessage() {
    String cluster = "cluster_name";
    String store = "store_name";
    String schemaStr = "int";

    Admin admin = mock(Admin.class);
    when(admin.getValueSchemaId(cluster, store, schemaStr)).thenReturn(SchemaData.INVALID_VALUE_SCHEMA_ID);
    when(admin.getStore(cluster, store)).thenReturn(null);
    SchemaRoutes schemaRoutes = new SchemaRoutes(false, Optional.empty(), requestHandler);
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

  @Test
  public void testAddValueSchemaWithIdAndCompatType() throws Exception {
    String cluster = "cluster_name";
    String store = "store_name";
    String schemaStr = "\"int\"";
    int schemaId = 2;
    DirectionalSchemaCompatibilityType schemaCompatType = DirectionalSchemaCompatibilityType.BACKWARD;

    Admin admin = mock(Admin.class);
    Request request = mock(Request.class);
    Response response = mock(Response.class);

    doReturn(cluster).when(request).queryParams(CLUSTER);
    doReturn(store).when(request).queryParams(NAME);
    doReturn(schemaStr).when(request).queryParams(VALUE_SCHEMA);
    doReturn(Integer.toString(schemaId)).when(request).queryParams(SCHEMA_ID);
    doReturn(schemaCompatType.toString()).when(request).queryParams(SCHEMA_COMPAT_TYPE);

    doReturn(true).when(admin).isLeaderControllerFor("cluster_name");
    doReturn(new SchemaEntry(schemaId, schemaStr)).when(admin)
        .addValueSchema(cluster, store, schemaStr, schemaId, schemaCompatType);

    SchemaRoutes schemaRoutes = new SchemaRoutes(false, Optional.empty(), requestHandler);
    Route route = schemaRoutes.addValueSchema(admin);
    route.handle(request, response);
    verify(response, times(0)).status(anyInt()); // no error
  }

}
