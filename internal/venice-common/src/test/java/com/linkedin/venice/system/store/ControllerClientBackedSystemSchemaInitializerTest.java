package com.linkedin.venice.system.store;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ControllerClientBackedSystemSchemaInitializerTest {
  @Test
  public void testCreateSystemStoreAndRegisterSchema() {
    ControllerClientBackedSystemSchemaInitializer initializer;
    try {
      initializer = new ControllerClientBackedSystemSchemaInitializer(
          AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE,
          "testCluster",
          null,
          null,
          false,
          Optional.empty(),
          "",
          "",
          "",
          false);
      initializer.execute();
      Assert.fail("Exception should be thrown when neither controller url nor d2 config is provided");
    } catch (VeniceException e) {
      // expected
    }
    initializer = new ControllerClientBackedSystemSchemaInitializer(
        AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE,
        "testCluster",
        null,
        null,
        false,
        Optional.empty(),
        "",
        "d2Service",
        "d2ZkHost",
        false);
    ControllerClient controllerClient = mock(ControllerClient.class);
    doReturn("leaderControllerUrl").when(controllerClient).getLeaderControllerUrl();
    D2ServiceDiscoveryResponse discoveryResponse = mock(D2ServiceDiscoveryResponse.class);
    doReturn(true).when(discoveryResponse).isError();
    doReturn(ErrorType.STORE_NOT_FOUND).when(discoveryResponse).getErrorType();
    doReturn(discoveryResponse).when(controllerClient).discoverCluster(any());
    StoreResponse storeResponse = mock(StoreResponse.class);
    doReturn(true).when(storeResponse).isError();
    doReturn(ErrorType.STORE_NOT_FOUND).when(storeResponse).getErrorType();
    doReturn(storeResponse).when(controllerClient).getStore(any());
    NewStoreResponse newStoreResponse = mock(NewStoreResponse.class);
    doReturn(newStoreResponse).when(controllerClient).createNewSystemStore(any(), any(), any(), any());
    MultiSchemaResponse multiSchemaResponse = mock(MultiSchemaResponse.class);
    doReturn(new MultiSchemaResponse.Schema[0]).when(multiSchemaResponse).getSchemas();
    doReturn(multiSchemaResponse).when(controllerClient).getAllValueSchema(any());
    SchemaResponse schemaResponse = mock(SchemaResponse.class);
    doReturn(schemaResponse).when(controllerClient).addValueSchema(any(), any(), anyInt());
    doCallRealMethod().when(controllerClient).retryableRequest(anyInt(), any(), any());
    doCallRealMethod().when(controllerClient).retryableRequest(anyInt(), any());
    initializer.setControllerClient(controllerClient);
    initializer.execute();
    verify(controllerClient, times(1)).createNewSystemStore(any(), any(), any(), any());
    verify(controllerClient, times(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion()))
        .addValueSchema(any(), any(), anyInt());
  }
}
