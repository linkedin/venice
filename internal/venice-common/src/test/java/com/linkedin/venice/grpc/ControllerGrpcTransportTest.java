package com.linkedin.venice.grpc;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.venice.controllerapi.MultiStoreInfoResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.QueryParams;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.GetStoresInClusterGrpcRequest;
import com.linkedin.venice.protocols.GetStoresInClusterGrpcResponse;
import com.linkedin.venice.protocols.VeniceControllerGrpcServiceGrpc;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.testng.annotations.Test;


public class ControllerGrpcTransportTest {
  private static final String STORE_CLUSTER_NAME = "test-cluster";
  private static final String SERVER_URL = "localhost:9000";
  private static final String STORE_NAME = "test-store";
  private static final String STORE_KEY_SCHEMA = "test-key-schema";
  private static final String STORE_VALUE_SCHEMA = "test-value-schema";
  private static final String STORE_OWNER = "test-owner";
  private static final int STORE_PARTITION_COUNT = 10;
  private static final boolean STORE_ENABLE_READS = true;
  private static final boolean STORE_ENABLE_WRITES = false;
  private static final int STORE_CURRENT_VERSION = 1;
  @Mock
  private ControllerGrpcTransport controllerGrpcTransport;

  @Test
  public void testCreateStoreRequestSend() {
    VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceStub mockClientStub =
        mock(VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceStub.class);

    controllerGrpcTransport = spy(new ControllerGrpcTransport(Optional.empty()));

    doReturn(mockClientStub).when(controllerGrpcTransport).getOrCreateStub(any());
    QueryParams params = new QueryParams();
    params.add(NAME, STORE_NAME)
        .add(KEY_SCHEMA, STORE_KEY_SCHEMA)
        .add(VALUE_SCHEMA, STORE_VALUE_SCHEMA)
        .add(OWNER, STORE_OWNER);

    controllerGrpcTransport.request(SERVER_URL, params, NewStoreResponse.class, GrpcControllerRoute.CREATE_STORE);
    verify(mockClientStub).createStore(argThat(expectedCreateStoreRequest()), any());
  }

  @Test
  public void testCreateStoreResponseReceived() {
    CreateStoreGrpcResponse mockResponse = mock(CreateStoreGrpcResponse.class);
    CompletableFuture<NewStoreResponse> responseFuture = new CompletableFuture<>();

    when(mockResponse.getOwner()).thenReturn(STORE_OWNER);

    ControllerGrpcTransport.ControllerGrpcObserver<CreateStoreGrpcResponse, NewStoreResponse> observer =
        new ControllerGrpcTransport.ControllerGrpcObserver<>(
            responseFuture,
            NewStoreResponse.class,
            GrpcControllerRoute.CREATE_STORE);

    observer.onNext(mockResponse);

    assertTrue(responseFuture.isDone());
    try {
      NewStoreResponse actualResponse = responseFuture.get();
      assertEquals(actualResponse.getOwner(), STORE_OWNER);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testGetStoresInClusterRequestSend() {
    VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceStub mockClientStub =
        mock(VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceStub.class);

    controllerGrpcTransport = spy(new ControllerGrpcTransport(Optional.empty()));

    doReturn(mockClientStub).when(controllerGrpcTransport).getOrCreateStub(any());
    QueryParams params = new QueryParams();
    params.add(CLUSTER, STORE_CLUSTER_NAME);

    controllerGrpcTransport
        .request(SERVER_URL, params, MultiStoreInfoResponse.class, GrpcControllerRoute.GET_STORES_IN_CLUSTER);
    verify(mockClientStub).getStoresInCluster(argThat(expectedGetStoreInClusterRequest()), any());
  }

  @Test
  public void testGetStoresInClusterResponseReceived() {
    GetStoresInClusterGrpcResponse mockResponse = mock(GetStoresInClusterGrpcResponse.class);
    CompletableFuture<MultiStoreInfoResponse> responseFuture = new CompletableFuture<>();

    GetStoresInClusterGrpcResponse.StoreInfo mockStoreInfo = GetStoresInClusterGrpcResponse.StoreInfo.newBuilder()
        .setName(STORE_NAME)
        .setOwner(STORE_OWNER)
        .setPartitionCount(STORE_PARTITION_COUNT)
        .setEnableReads(STORE_ENABLE_READS)
        .setEnableWrites(STORE_ENABLE_WRITES)
        .setCurrentVersion(STORE_CURRENT_VERSION)
        .build();

    when(mockResponse.getStoresList()).thenReturn(Collections.singletonList(mockStoreInfo));

    ControllerGrpcTransport.ControllerGrpcObserver<GetStoresInClusterGrpcResponse, MultiStoreInfoResponse> observer =
        new ControllerGrpcTransport.ControllerGrpcObserver<>(
            responseFuture,
            MultiStoreInfoResponse.class,
            GrpcControllerRoute.GET_STORES_IN_CLUSTER);

    observer.onNext(mockResponse);

    assertTrue(responseFuture.isDone());
    try {
      MultiStoreInfoResponse actualResponse = responseFuture.get();
      assertNotNull(actualResponse);
      assertEquals(actualResponse.getStoreInfoList().size(), 1);
      StoreInfo info = actualResponse.getStoreInfoList().get(0);

      assertEquals(info.getOwner(), STORE_OWNER);
      assertEquals(info.getName(), STORE_NAME);
      assertEquals(info.getPartitionCount(), STORE_PARTITION_COUNT);
      assertEquals(info.getCurrentVersion(), STORE_CURRENT_VERSION);
      assertEquals(info.isEnableStoreReads(), STORE_ENABLE_READS);
      assertEquals(info.isEnableStoreWrites(), STORE_ENABLE_WRITES);
    } catch (Exception e) {
      fail();
    }
  }

  ArgumentMatcher<CreateStoreGrpcRequest> expectedCreateStoreRequest() {
    return argument -> STORE_OWNER.equalsIgnoreCase(argument.getOwner())
        && STORE_NAME.equalsIgnoreCase(argument.getStoreName())
        && STORE_KEY_SCHEMA.equalsIgnoreCase(argument.getKeySchema())
        && STORE_VALUE_SCHEMA.equalsIgnoreCase(argument.getValueSchema());
  }

  ArgumentMatcher<GetStoresInClusterGrpcRequest> expectedGetStoreInClusterRequest() {
    return argument -> STORE_CLUSTER_NAME.equalsIgnoreCase(argument.getClusterName());
  }
}
