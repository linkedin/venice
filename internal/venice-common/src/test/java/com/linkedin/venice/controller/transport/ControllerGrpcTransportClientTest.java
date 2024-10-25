package com.linkedin.venice.controller.transport;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.venice.controller.requests.CreateStoreRequest;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.VeniceControllerGrpcServiceGrpc;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.mockito.ArgumentMatcher;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class ControllerGrpcTransportClientTest {
  private static final String STORE_CLUSTER_NAME = "test-cluster";
  private static final String SERVER_URL = "localhost:1234";
  private static final String STORE_NAME = "test-store";
  private static final String STORE_KEY_SCHEMA = "test-key-schema";
  private static final String STORE_VALUE_SCHEMA = "test-value-schema";
  private static final String STORE_OWNER = "test-owner";
  private ControllerGrpcTransportClient controllerGrpcTransportClient;

  @BeforeTest
  public void setup() {
    controllerGrpcTransportClient = spy(new ControllerGrpcTransportClient(SERVER_URL));
  }

  @Test
  public void testCreateStoreRequestSend() {
    CreateStoreGrpcResponse mockResponse = mock(CreateStoreGrpcResponse.class);
    when(mockResponse.getOwner()).thenReturn(STORE_OWNER);

    VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceBlockingStub mockClientStub =
        mock(VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceBlockingStub.class);

    doReturn(mockClientStub).when(controllerGrpcTransportClient).buildStub();
    doReturn(mockResponse).when(mockClientStub).createStore(any());
    CreateStoreRequest storeRequest = CreateStoreRequest.newBuilder()
        .setStoreName(STORE_NAME)
        .setKeySchema(STORE_KEY_SCHEMA)
        .setValueSchema(STORE_VALUE_SCHEMA)
        .setOwner(STORE_OWNER)
        .setClusterName(STORE_CLUSTER_NAME)
        .build();

    try {
      NewStoreResponse response = controllerGrpcTransportClient.request(storeRequest);
      verify(mockClientStub).createStore(argThat(expectedCreateStoreRequest()));
      assertEquals(response.getOwner(), STORE_OWNER);
    } catch (ExecutionException | TimeoutException e) {
      fail();
    }
  }

  ArgumentMatcher<CreateStoreGrpcRequest> expectedCreateStoreRequest() {
    return argument -> STORE_OWNER.equalsIgnoreCase(argument.getOwner())
        && STORE_NAME.equalsIgnoreCase(argument.getStoreName())
        && STORE_KEY_SCHEMA.equalsIgnoreCase(argument.getKeySchema())
        && STORE_VALUE_SCHEMA.equalsIgnoreCase(argument.getValueSchema());
  }
}
