package com.linkedin.venice.fastclient.transport;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.HttpMethod;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.fastclient.GrpcClientConfig;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;


public class GrpcTransportClientTest {
  private static final int PARTITION = 1;
  private static final String URI = "localhost:1690";

  private static final String GRPC_URI = "localhost:1691";
  private static final String STORAGE = "storage";
  private static final String RESOURCE_NAME = "test_store_v1";

  private static final String PROTOCOL = "https";
  private static final String PARTITION_STRING = String.valueOf(PARTITION);

  private static final String KEY_STRING = "test_key";

  private static final String[] DEFAULT_REQUEST_PATH =
      { PROTOCOL, ":", URI, STORAGE, RESOURCE_NAME, PARTITION_STRING, KEY_STRING };

  @Mock
  private GrpcClientConfig mockClientConfig;

  @Mock
  private Client mockClient;

  private Map<String, String> nettyServerToGrpcAddress = Collections.emptyMap();

  private GrpcTransportClient grpcTransportClient;

  @BeforeTest
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(mockClientConfig.getR2Client()).thenReturn(mockClient);
    when(mockClientConfig.getNettyServerToGrpcAddress()).thenReturn(nettyServerToGrpcAddress);
    when(mockClientConfig.getPort()).thenReturn(23900);
    grpcTransportClient = new GrpcTransportClient(mockClientConfig);
  }

  @Test
  public void testBuildVeniceClientRequestForSingleGet() {
    VeniceClientRequest clientRequest =
        grpcTransportClient.buildVeniceClientRequest(DEFAULT_REQUEST_PATH, new byte[0], true);

    assertFalse(clientRequest.getIsBatchRequest());
    assertEquals(clientRequest.getKeyString(), KEY_STRING);
    assertEquals(clientRequest.getPartition(), PARTITION);
    assertEquals(clientRequest.getResourceName(), RESOURCE_NAME);
    assertEquals(clientRequest.getMethod(), HttpMethod.GET.name());
  }

  @Test
  public void testBuildVeniceClientRequestForBatchGet() {
    VeniceClientRequest clientRequest =
        grpcTransportClient.buildVeniceClientRequest(DEFAULT_REQUEST_PATH, new byte[0], false);

    assertTrue(clientRequest.getIsBatchRequest());
    assertEquals(clientRequest.getResourceName(), RESOURCE_NAME);
    assertEquals(clientRequest.getMethod(), HttpMethod.POST.name());
    assertNotNull(clientRequest.getKeyBytes());
  }

  @Test
  public void testGetGrpcAddressFromServerAddress() {
    when(mockClientConfig.getNettyServerToGrpcAddress()).thenReturn(ImmutableMap.of(URI, GRPC_URI));
    grpcTransportClient = new GrpcTransportClient(mockClientConfig);

    // Entry found in the mapping should be returned and is validated by checking port which is not the default GRPC
    // port
    assertEquals(grpcTransportClient.getGrpcAddressFromServerAddress(URI), GRPC_URI);

    final String TEST_URI = "localhost:1234";
    final String FALLBACK_URI = "localhost:23900";
    // No entry in the map should result in generating GRPC address with default port
    assertEquals(grpcTransportClient.getGrpcAddressFromServerAddress(TEST_URI), FALLBACK_URI);

    // Ensure subsequent mapping leverages cache and doesn't run the logic to compute grpc fallback address
    Map<String, String> nettyToGrpcAddress = grpcTransportClient.getNettyServerToGrpcAddress();
    assertEquals(nettyToGrpcAddress.get(TEST_URI), FALLBACK_URI);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGetGrpcAddressFromServerAddressWithInvalidServerAddress() {
    grpcTransportClient.getGrpcAddressFromServerAddress("");
  }

  @Test
  public void testNonStorageQueries() {
    String[] nonStorageValidRequestPath = { PROTOCOL, ":", URI, "metadata" };
    // type of method should not impact the validation
    assertTrue(grpcTransportClient.isValidRequest(nonStorageValidRequestPath, false));
    assertTrue(grpcTransportClient.isValidRequest(nonStorageValidRequestPath, true));

    // valid non storage query parts with greater than 4 parts
    String[] nonStorageValidRequestPath1 = { PROTOCOL, ":", URI, "metadata", "junk_field" };
    assertTrue(grpcTransportClient.isValidRequest(nonStorageValidRequestPath1, false));
    assertTrue(grpcTransportClient.isValidRequest(nonStorageValidRequestPath1, true));

    // invalid request path
    assertFalse(grpcTransportClient.isValidRequest(new String[0], true));
    assertFalse(grpcTransportClient.isValidRequest(new String[0], false));
  }

  @Test
  public void testInvalidStorageQueriesRequest() {
    String[] invalidStorageRequestPath = { PROTOCOL, ":", URI, STORAGE, RESOURCE_NAME, PARTITION_STRING };
    assertFalse(grpcTransportClient.isValidRequest(invalidStorageRequestPath, false));

    assertFalse(grpcTransportClient.isValidRequest(invalidStorageRequestPath, true));
    assertFalse(grpcTransportClient.isValidRequest(DEFAULT_REQUEST_PATH, false));
  }

  @Test
  public void testValidStorageQueriesRequest() {
    // single get validation
    assertTrue(grpcTransportClient.isValidRequest(DEFAULT_REQUEST_PATH, true));

    // batch get validation
    String[] validBatchRequestPath = { PROTOCOL, ":", URI, STORAGE, RESOURCE_NAME };
    assertTrue(grpcTransportClient.isValidRequest(validBatchRequestPath, false));
  }

  @Ignore(value = "Disabling the tests due to mockito limitation")
  @Test
  public void testHandleStorageGetQuery() {
    final VeniceClientRequest mockClientRequest = buildMockClientRequest();
    final VeniceReadServiceGrpc.VeniceReadServiceStub mockClientStub =
        mock(VeniceReadServiceGrpc.VeniceReadServiceStub.class);

    grpcTransportClient = spy(new GrpcTransportClient(mockClientConfig));
    doReturn(mockClientRequest).when(grpcTransportClient).buildVeniceClientRequest(any(), any(), anyBoolean());
    doReturn(mockClientStub).when(grpcTransportClient).getOrCreateStub(any());

    grpcTransportClient.handleStorageQueries(DEFAULT_REQUEST_PATH, new byte[0], true);
    verify(mockClientStub).get(eq(mockClientRequest), any());
  }

  @Ignore(value = "Disabling the tests due to mockito limitation")
  @Test
  public void testHandleStorageBatchQuery() {
    final VeniceClientRequest mockClientRequest = buildMockClientRequest();
    final VeniceReadServiceGrpc.VeniceReadServiceStub mockClientStub =
        mock(VeniceReadServiceGrpc.VeniceReadServiceStub.class);

    grpcTransportClient = spy(new GrpcTransportClient(mockClientConfig));
    doReturn(mockClientRequest).when(grpcTransportClient).buildVeniceClientRequest(any(), any(), anyBoolean());
    doReturn(mockClientStub).when(grpcTransportClient).getOrCreateStub(any());

    grpcTransportClient.handleStorageQueries(DEFAULT_REQUEST_PATH, new byte[0], false);
    verify(mockClientStub).batchGet(eq(mockClientRequest), any());
  }

  @Test
  public void testHandleNonStorageQueries() {
    TransportClient mockTransportClient = mock(TransportClient.class);
    GrpcTransportClient transportClient =
        spy(new GrpcTransportClient(mockTransportClient, ImmutableMap.of(), 23900, null));

    Map<String, String> headers = Collections.emptyMap();
    transportClient.handleNonStorageQueries(URI, headers, new byte[0], true);
    verify(mockTransportClient).get(eq(URI), eq(headers));

    byte[] body = new byte[0];
    transportClient.handleNonStorageQueries(URI, headers, body, false);
    verify(mockTransportClient).post(eq(URI), eq(headers), eq(body));
  }

  @Test(dataProvider = "error-code-error-message")
  public void testHandleResponseError(int errorCode, String errorMessage) {
    CompletableFuture<TransportClientResponse> responseFuture = new CompletableFuture<>();
    GrpcTransportClient.VeniceGrpcStreamObserver veniceGrpcStreamObserver =
        new GrpcTransportClient.VeniceGrpcStreamObserver(responseFuture);

    VeniceServerResponse mockResponse = buildMockVeniceServerResponse(errorCode, errorMessage);
    veniceGrpcStreamObserver.handleResponseError(mockResponse);

    assertTrue(responseFuture.isDone());
    if (errorCode != 101) {
      assertTrue(responseFuture.isCompletedExceptionally());
    }
  }

  @DataProvider(name = "error-code-error-message")
  public static Object[][] generateErrorCode() {
    return new Object[][] { { 400, "bad request" }, { 501, "too many request" }, { 101, "key not found" } };
  }

  /*
   * A workaround due to limitation on mocking final classes with the current version of mockito
   */
  private static VeniceClientRequest buildMockClientRequest() {
    return VeniceClientRequest.newBuilder().build();
  }

  private static VeniceServerResponse buildMockVeniceServerResponse(int errorCode, String errorMessage) {
    return VeniceServerResponse.newBuilder().setErrorCode(errorCode).setErrorMessage(errorMessage).build();
  }
}
