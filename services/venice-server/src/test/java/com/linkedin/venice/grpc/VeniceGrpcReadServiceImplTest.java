package com.linkedin.venice.grpc;

import static com.linkedin.venice.listener.StorageReadRequestHandler.VENICE_STORAGE_NODE_HARDWARE_IS_NOT_HEALTHY_MSG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.davinci.listener.response.TopicPartitionIngestionContextResponse;
import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.QuotaEnforcementHandler;
import com.linkedin.venice.listener.StorageReadRequestHandler;
import com.linkedin.venice.listener.response.BinaryResponse;
import com.linkedin.venice.meta.ServerAdminAction;
import com.linkedin.venice.protocols.AdminRequest;
import com.linkedin.venice.protocols.AdminResponse;
import com.linkedin.venice.protocols.CompressionDictionaryRequest;
import com.linkedin.venice.protocols.CompressionDictionaryResponse;
import com.linkedin.venice.protocols.CurrentVersionInfoRequest;
import com.linkedin.venice.protocols.CurrentVersionInfoResponse;
import com.linkedin.venice.protocols.HealthCheckRequest;
import com.linkedin.venice.protocols.HealthCheckResponse;
import com.linkedin.venice.protocols.IngestionContextRequest;
import com.linkedin.venice.protocols.IngestionContextResponse;
import com.linkedin.venice.protocols.MetadataRequest;
import com.linkedin.venice.protocols.MetadataResponse;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc.VeniceReadServiceBlockingStub;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceGrpcReadServiceImplTest {
  private Server grpcServer;
  private ManagedChannel grpcChannel;
  private GrpcServiceDependencies grpcServiceDependencies;
  private DiskHealthCheckService diskHealthCheckService;
  private StorageReadRequestHandler storageReadRequestHandler;
  private QuotaEnforcementHandler quotaEnforcementHandler;
  private AggServerHttpRequestStats singleGetStats;
  private AggServerHttpRequestStats multiGetStats;
  private AggServerHttpRequestStats computeStats;

  @BeforeMethod
  public void setUp() throws IOException {
    diskHealthCheckService = mock(DiskHealthCheckService.class);
    storageReadRequestHandler = mock(StorageReadRequestHandler.class);
    quotaEnforcementHandler = mock(QuotaEnforcementHandler.class);
    singleGetStats = mock(AggServerHttpRequestStats.class);
    multiGetStats = mock(AggServerHttpRequestStats.class);
    computeStats = mock(AggServerHttpRequestStats.class);

    grpcServiceDependencies = new GrpcServiceDependencies.Builder().setDiskHealthCheckService(diskHealthCheckService)
        .setStorageReadRequestHandler(storageReadRequestHandler)
        .setQuotaEnforcementHandler(quotaEnforcementHandler)
        .setSingleGetStats(singleGetStats)
        .setMultiGetStats(multiGetStats)
        .setComputeStats(computeStats)
        .build();

    // Create a unique server name for the in-process server
    String serverName = InProcessServerBuilder.generateName();

    // Start the gRPC server in-process
    grpcServer = InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(new VeniceGrpcReadServiceImpl(grpcServiceDependencies))
        .build()
        .start();

    // Create a channel to communicate with the server
    grpcChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
  }

  @AfterMethod
  public void cleanUp() {
    // Shut down the channel and server after each test
    if (grpcChannel != null) {
      grpcChannel.shutdownNow();
    }
    if (grpcServer != null) {
      grpcServer.shutdownNow();
    }
  }

  @Test
  public void testIsServerHealthy() {
    VeniceReadServiceBlockingStub blockingStub = VeniceReadServiceGrpc.newBlockingStub(grpcChannel);

    when(diskHealthCheckService.isDiskHealthy()).thenReturn(true);
    HealthCheckRequest request = HealthCheckRequest.newBuilder().build();
    assertEquals(blockingStub.isServerHealthy(request).getStatusCode(), VeniceReadResponseStatus.OK.getCode());

    when(diskHealthCheckService.isDiskHealthy()).thenReturn(false);
    HealthCheckResponse response = blockingStub.isServerHealthy(request);
    assertEquals(response.getStatusCode(), VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
    assertNotNull(response.getMessage());
    assertTrue(response.getMessage().contains(VENICE_STORAGE_NODE_HARDWARE_IS_NOT_HEALTHY_MSG));
  }

  @Test
  public void testGetCompressionDictionary() {
    // Case 1: Non-empty buffer response means dictionary found
    VeniceReadServiceBlockingStub blockingStub = VeniceReadServiceGrpc.newBlockingStub(grpcChannel);
    CompressionDictionaryRequest request =
        CompressionDictionaryRequest.newBuilder().setStoreName("testStore").setStoreVersion(1).build();
    BinaryResponse binaryResponse = new BinaryResponse(ByteBuffer.wrap(new byte[] { 4, 5, 6 }));
    when(storageReadRequestHandler.handleDictionaryFetchRequest(any())).thenReturn(binaryResponse);

    CompressionDictionaryResponse actualResponse = blockingStub.getCompressionDictionary(request);
    assertEquals(actualResponse.getStatusCode(), VeniceReadResponseStatus.OK.getCode());
    assertEquals(actualResponse.getValue().asReadOnlyByteBuffer(), ByteBuffer.wrap(new byte[] { 4, 5, 6 }));
    assertEquals(actualResponse.getContentType(), HttpConstants.BINARY);
    assertEquals(actualResponse.getContentLength(), 3);

    // Case 2: Empty buffer response means dictionary not found
    BinaryResponse emptyBufferResponse = new BinaryResponse((ByteBuffer) null);
    when(storageReadRequestHandler.handleDictionaryFetchRequest(any())).thenReturn(emptyBufferResponse);
    CompressionDictionaryResponse emptyBufferActualResponse = blockingStub.getCompressionDictionary(request);
    assertEquals(emptyBufferActualResponse.getStatusCode(), VeniceReadResponseStatus.KEY_NOT_FOUND.getCode());

    // Case 3: Exception was thrown when handling the request hence return INTERNAL_SERVER_ERROR
    when(storageReadRequestHandler.handleDictionaryFetchRequest(any()))
        .thenThrow(new VeniceException("Test exception"));
    CompressionDictionaryResponse exceptionActualResponse = blockingStub.getCompressionDictionary(request);
    assertEquals(exceptionActualResponse.getStatusCode(), VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
  }

  @Test
  public void testHandleAdminRequest() {
    // Case 1: Admin request is handled successfully with a response
    AdminRequest adminRequest = AdminRequest.newBuilder()
        .setResourceName("testStore_v1")
        .setServerAdminAction(ServerAdminAction.DUMP_SERVER_CONFIGS.name())
        .build();
    VeniceReadServiceBlockingStub blockingStub = VeniceReadServiceGrpc.newBlockingStub(grpcChannel);
    com.linkedin.davinci.listener.response.AdminResponse adminResponse =
        mock(com.linkedin.davinci.listener.response.AdminResponse.class);
    when(adminResponse.isError()).thenReturn(false);
    when(storageReadRequestHandler.handleServerAdminRequest(any())).thenReturn(adminResponse);
    when(adminResponse.getResponseBody()).thenReturn(Unpooled.wrappedBuffer(new byte[] { 1, 2, 3 }));

    AdminResponse grpcAdminResponse = blockingStub.handleAdminRequest(adminRequest);
    assertEquals(grpcAdminResponse.getStatusCode(), VeniceReadResponseStatus.OK.getCode());
    assertEquals(grpcAdminResponse.getValue().asReadOnlyByteBuffer(), ByteBuffer.wrap(new byte[] { 1, 2, 3 }));
    assertEquals(grpcAdminResponse.getContentType(), HttpConstants.AVRO_BINARY);
    assertEquals(grpcAdminResponse.getContentLength(), 3);

    // Case 2: Admin request is handled successfully with error response
    when(adminResponse.isError()).thenReturn(true);
    when(adminResponse.getMessage()).thenReturn("Test error message");
    when(adminResponse.getResponseBody()).thenReturn(Unpooled.EMPTY_BUFFER);
    AdminResponse grpcAdminErrorResponse = blockingStub.handleAdminRequest(adminRequest);
    assertEquals(grpcAdminErrorResponse.getStatusCode(), VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
    assertEquals(grpcAdminErrorResponse.getValue().asReadOnlyByteBuffer(), ByteBuffer.wrap(new byte[0]));
    assertEquals(grpcAdminErrorResponse.getContentType(), HttpConstants.TEXT_PLAIN);
    assertEquals(grpcAdminErrorResponse.getContentLength(), 0);

    // Case 3: Exception was thrown when handling the request hence return INTERNAL_SERVER_ERROR
    when(storageReadRequestHandler.handleServerAdminRequest(any())).thenThrow(new VeniceException("Test exception"));
    AdminResponse exceptionActualResponse = blockingStub.handleAdminRequest(adminRequest);
    assertEquals(exceptionActualResponse.getStatusCode(), VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
  }

  @Test
  public void testGetMetadata() {
    // Case 1: Metadata request is handled successfully with a response
    MetadataRequest request = MetadataRequest.newBuilder().setStoreName("testStore").build();
    VeniceReadServiceBlockingStub blockingStub = VeniceReadServiceGrpc.newBlockingStub(grpcChannel);

    com.linkedin.davinci.listener.response.MetadataResponse metadataResponseMock =
        mock(com.linkedin.davinci.listener.response.MetadataResponse.class);
    when(metadataResponseMock.isError()).thenReturn(false);
    when(metadataResponseMock.getResponseBody()).thenReturn(Unpooled.wrappedBuffer(new byte[] { 1, 2, 3 }));
    when(metadataResponseMock.getResponseSchemaIdHeader()).thenReturn(-11);
    when(storageReadRequestHandler.handleMetadataFetchRequest(any())).thenReturn(metadataResponseMock);

    MetadataResponse metadataResponse = blockingStub.getMetadata(request);
    assertEquals(metadataResponse.getStatusCode(), VeniceReadResponseStatus.OK.getCode());
    assertEquals(metadataResponse.getValue().asReadOnlyByteBuffer(), ByteBuffer.wrap(new byte[] { 1, 2, 3 }));
    assertEquals(metadataResponse.getContentType(), HttpConstants.AVRO_BINARY);
    assertEquals(metadataResponse.getContentLength(), 3);
    assertEquals(metadataResponse.getSchemaId(), -11);

    // Case 2: Metadata request is handled successfully with error response
    when(metadataResponseMock.isError()).thenReturn(true);
    when(metadataResponseMock.getMessage()).thenReturn("Test error message");
    when(metadataResponseMock.getResponseBody()).thenReturn(Unpooled.EMPTY_BUFFER);
    MetadataResponse grpcMetadataErrorResponse = blockingStub.getMetadata(request);
    assertEquals(grpcMetadataErrorResponse.getStatusCode(), VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
    assertEquals(grpcMetadataErrorResponse.getValue().asReadOnlyByteBuffer(), ByteBuffer.wrap(new byte[0]));
    assertEquals(grpcMetadataErrorResponse.getContentType(), HttpConstants.TEXT_PLAIN);
    assertEquals(grpcMetadataErrorResponse.getContentLength(), 0);

    // Case 3: Exception was thrown when handling the request hence return INTERNAL_SERVER_ERROR
    when(storageReadRequestHandler.handleMetadataFetchRequest(any())).thenThrow(new VeniceException("Test exception"));
    MetadataResponse exceptionActualResponse = blockingStub.getMetadata(request);
    assertEquals(exceptionActualResponse.getStatusCode(), VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
  }

  @Test
  public void testGetCurrentVersionInfo() {
    // Case 1: Current version info request is handled successfully with a response
    CurrentVersionInfoRequest request = CurrentVersionInfoRequest.newBuilder().setStoreName("testStore").build();
    VeniceReadServiceBlockingStub blockingStub = VeniceReadServiceGrpc.newBlockingStub(grpcChannel);
    ServerCurrentVersionResponse serverCurrentVersionResponseMock = new ServerCurrentVersionResponse();
    serverCurrentVersionResponseMock.setCurrentVersion(2);
    serverCurrentVersionResponseMock.setError(false);
    when(storageReadRequestHandler.handleCurrentVersionRequest(any())).thenReturn(serverCurrentVersionResponseMock);
    CurrentVersionInfoResponse response = blockingStub.getCurrentVersionInfo(request);
    assertEquals(response.getStatusCode(), VeniceReadResponseStatus.OK.getCode());
    assertEquals(response.getCurrentVersion(), 2);

    // Case 2: Current version info request is handled successfully with error response
    serverCurrentVersionResponseMock.setError(true);
    serverCurrentVersionResponseMock.setCurrentVersion(-1);
    serverCurrentVersionResponseMock.setMessage("Test error message");
    CurrentVersionInfoResponse errorResponse = blockingStub.getCurrentVersionInfo(request);
    assertEquals(errorResponse.getStatusCode(), VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
    assertEquals(errorResponse.getErrorMessage(), "Test error message");

    // Case 3: Exception was thrown when handling the request hence return INTERNAL_SERVER_ERROR
    when(storageReadRequestHandler.handleCurrentVersionRequest(any())).thenThrow(new VeniceException("Test exception"));
    CurrentVersionInfoResponse exceptionActualResponse = blockingStub.getCurrentVersionInfo(request);
    assertEquals(exceptionActualResponse.getStatusCode(), VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
  }

  @Test
  public void testGetIngestionContext() {
    // Case 1: Ingestion context request is handled successfully with a response
    IngestionContextRequest request =
        IngestionContextRequest.newBuilder().setTopicName("testStore_v1").setPartition(34).build();
    VeniceReadServiceBlockingStub blockingStub = VeniceReadServiceGrpc.newBlockingStub(grpcChannel);
    String jsonStr = "{\n" + "\"kafkaUrl\" : {\n" + "  TP(topic: \"testStore_v1\", partition: 34) " + ": {\n"
        + "      \"latestOffset\" : 0,\n" + "      \"offsetLag\" : 1,\n" + "      \"msgRate\" : 2.0,\n"
        + "      \"byteRate\" : 4.0,\n" + "      \"consumerIdx\" : 6,\n"
        + "      \"elapsedTimeSinceLastPollInMs\" : 7\n" + "    }\n" + "  }\n" + "}";
    byte[] ingestionCtxBytes = jsonStr.getBytes();
    TopicPartitionIngestionContextResponse responseMock = new TopicPartitionIngestionContextResponse();
    responseMock.setTopicPartitionIngestionContext(ingestionCtxBytes);
    responseMock.setError(false);
    when(storageReadRequestHandler.handleTopicPartitionIngestionContextRequest(any())).thenReturn(responseMock);
    IngestionContextResponse response = blockingStub.getIngestionContext(request);
    assertEquals(response.getStatusCode(), VeniceReadResponseStatus.OK.getCode());
    assertEquals(response.getValue().asReadOnlyByteBuffer(), ByteBuffer.wrap(ingestionCtxBytes));
    assertEquals(response.getContentLength(), ingestionCtxBytes.length);

    // Case 2: Ingestion context request is handled successfully with error response
    responseMock.setError(true);
    responseMock.setMessage("Test error message");
    responseMock.setTopicPartitionIngestionContext(null);
    IngestionContextResponse errorResponse = blockingStub.getIngestionContext(request);
    assertEquals(errorResponse.getStatusCode(), VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
    assertEquals(errorResponse.getErrorMessage(), "Test error message");

    // Case 3: Exception was thrown when handling the request hence return INTERNAL_SERVER_ERROR
    when(storageReadRequestHandler.handleTopicPartitionIngestionContextRequest(any()))
        .thenThrow(new VeniceException("Test exception"));
    IngestionContextResponse exceptionActualResponse = blockingStub.getIngestionContext(request);
    assertEquals(exceptionActualResponse.getStatusCode(), VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
  }
}
