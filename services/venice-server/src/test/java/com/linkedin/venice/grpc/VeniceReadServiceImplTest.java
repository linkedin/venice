package com.linkedin.venice.grpc;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.storage.ReadMetadataRetriever;
import com.linkedin.venice.listener.grpc.VeniceReadServiceImpl;
import com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcRequestProcessor;
import com.linkedin.venice.metadata.response.VersionProperties;
import com.linkedin.venice.protocols.VeniceMetadataRequest;
import com.linkedin.venice.protocols.VeniceMetadataResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceReadServiceImplTest {
  private ReadMetadataRetriever readMetadataRetriever;
  private VeniceServerGrpcRequestProcessor requestProcessor;
  private VeniceReadServiceImpl serviceImpl;

  @BeforeMethod
  public void setUp() {
    readMetadataRetriever = mock(ReadMetadataRetriever.class);
    requestProcessor = mock(VeniceServerGrpcRequestProcessor.class);
    serviceImpl = new VeniceReadServiceImpl(requestProcessor, readMetadataRetriever);
  }

  @Test
  public void testGetMetadataReturnsSuccessfulResponse() {
    String storeName = "test_store";
    Map<CharSequence, CharSequence> keySchema = Collections.singletonMap("1", "{\"type\":\"string\"}");
    Map<CharSequence, CharSequence> valueSchemas = Collections.singletonMap("1", "{\"type\":\"string\"}");

    MetadataResponse expectedResponse = new MetadataResponse();
    VersionProperties versionProperties = new VersionProperties(
        1,
        0,
        4,
        "com.linkedin.venice.partitioner.DefaultVenicePartitioner",
        Collections.singletonMap("amplificationFactor", "1"),
        1);
    expectedResponse.setVersionMetadata(versionProperties);
    expectedResponse.setVersions(Collections.singletonList(1));
    expectedResponse.setKeySchema(keySchema);
    expectedResponse.setValueSchemas(valueSchemas);
    expectedResponse.setLatestSuperSetValueSchemaId(1);
    expectedResponse.setBatchGetLimit(150);

    doReturn(expectedResponse).when(readMetadataRetriever).getMetadata(storeName);

    VeniceMetadataRequest request = VeniceMetadataRequest.newBuilder().setStoreName(storeName).build();

    ArgumentCaptor<VeniceMetadataResponse> responseCaptor = ArgumentCaptor.forClass(VeniceMetadataResponse.class);
    @SuppressWarnings("unchecked")
    StreamObserver<VeniceMetadataResponse> responseObserver = mock(StreamObserver.class);

    serviceImpl.getMetadata(request, responseObserver);

    verify(responseObserver).onNext(responseCaptor.capture());
    verify(responseObserver).onCompleted();

    VeniceMetadataResponse response = responseCaptor.getValue();
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.OK);
    assertFalse(response.getMetadata().isEmpty());
    assertEquals(response.getResponseSchemaId(), expectedResponse.getResponseSchemaIdHeader());
  }

  @Test
  public void testGetMetadataReturnsErrorWhenQuotaNotEnabled() {
    String storeName = "test_store";
    doThrow(new UnsupportedOperationException("Read quota not enabled for store: " + storeName))
        .when(readMetadataRetriever)
        .getMetadata(storeName);

    VeniceMetadataRequest request = VeniceMetadataRequest.newBuilder().setStoreName(storeName).build();

    ArgumentCaptor<VeniceMetadataResponse> responseCaptor = ArgumentCaptor.forClass(VeniceMetadataResponse.class);
    @SuppressWarnings("unchecked")
    StreamObserver<VeniceMetadataResponse> responseObserver = mock(StreamObserver.class);

    serviceImpl.getMetadata(request, responseObserver);

    verify(responseObserver).onNext(responseCaptor.capture());
    verify(responseObserver).onCompleted();

    VeniceMetadataResponse response = responseCaptor.getValue();
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("Read quota not enabled"));
  }

  @Test
  public void testGetMetadataReturnsInternalErrorOnException() {
    String storeName = "test_store";
    doThrow(new RuntimeException("Unexpected failure")).when(readMetadataRetriever).getMetadata(storeName);

    VeniceMetadataRequest request = VeniceMetadataRequest.newBuilder().setStoreName(storeName).build();

    ArgumentCaptor<VeniceMetadataResponse> responseCaptor = ArgumentCaptor.forClass(VeniceMetadataResponse.class);
    @SuppressWarnings("unchecked")
    StreamObserver<VeniceMetadataResponse> responseObserver = mock(StreamObserver.class);

    serviceImpl.getMetadata(request, responseObserver);

    verify(responseObserver).onNext(responseCaptor.capture());
    verify(responseObserver).onCompleted();

    VeniceMetadataResponse response = responseCaptor.getValue();
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.INTERNAL_ERROR);
    assertTrue(response.getErrorMessage().contains("Unexpected failure"));
  }

  @Test
  public void testGetMetadataHandlesErrorResponse() {
    String storeName = "test_store";

    MetadataResponse errorResponse = new MetadataResponse();
    errorResponse.setError(true);
    errorResponse.setMessage("Store not found");

    doReturn(errorResponse).when(readMetadataRetriever).getMetadata(storeName);

    VeniceMetadataRequest request = VeniceMetadataRequest.newBuilder().setStoreName(storeName).build();

    ArgumentCaptor<VeniceMetadataResponse> responseCaptor = ArgumentCaptor.forClass(VeniceMetadataResponse.class);
    @SuppressWarnings("unchecked")
    StreamObserver<VeniceMetadataResponse> responseObserver = mock(StreamObserver.class);

    serviceImpl.getMetadata(request, responseObserver);

    verify(responseObserver).onNext(responseCaptor.capture());
    verify(responseObserver).onCompleted();

    VeniceMetadataResponse response = responseCaptor.getValue();
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.INTERNAL_ERROR);
    assertEquals(response.getErrorMessage(), "Store not found");
  }
}
