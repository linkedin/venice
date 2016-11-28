package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceServerException;
import com.linkedin.venice.client.serializer.RecordDeserializer;
import com.linkedin.venice.client.store.transport.TransportClientCallback;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

import org.apache.http.HttpStatus;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestTransportClientCallback {
  private static String SCHEMA_ID = "1";
  private static String RESPONSE_BODY_STR = "serialized-body";
  private static String DESERIALIZED_BODY_STR = "body";

  private byte[] mockResponseBody;
  private ClientCallback mockClientCallback;
  private CompletableFuture mockFuture;
  private DeserializerFetcher mockDeserializerFetcher;
  private RecordDeserializer mockRecordDeserializer;
  private TransportClientCallback transportClientCallback;


  @BeforeClass
  public void setUp() {
    mockResponseBody = RESPONSE_BODY_STR.getBytes();
    mockClientCallback = mock(ClientCallback.class);
    mockFuture = mock(CompletableFuture.class);

    mockDeserializerFetcher = mock(DeserializerFetcher.class);
    mockRecordDeserializer = mock(RecordDeserializer.class);
    doReturn(mockRecordDeserializer).when(mockDeserializerFetcher).fetch(Integer.parseInt(SCHEMA_ID));
    doReturn(DESERIALIZED_BODY_STR).when(mockRecordDeserializer).deserialize(mockResponseBody);

    transportClientCallback =
        new TransportClientCallback(mockFuture, mockDeserializerFetcher, mockClientCallback);
  }

  @Test
  void testRawResponse() {
    TransportClientCallback rawResponseCallback = new TransportClientCallback(mockFuture);
    rawResponseCallback.completeFuture(HttpStatus.SC_OK, mockResponseBody, SCHEMA_ID);

    verify(mockFuture).complete(mockResponseBody);
  }

  @Test
  void TestNormalResponse() {
    transportClientCallback.completeFuture(HttpStatus.SC_OK, mockResponseBody, SCHEMA_ID);
    verify(mockFuture).complete(DESERIALIZED_BODY_STR);

    transportClientCallback.completeFuture(HttpStatus.SC_NOT_FOUND, mockResponseBody, SCHEMA_ID);
    verify(mockFuture).complete(null);
  }

  @Test
  void testNormalResponseWithException() {
    ClassCastException e = new ClassCastException("callbackException");

    doThrow(e).when(mockRecordDeserializer).deserialize(mockResponseBody);
    transportClientCallback.completeFuture(HttpStatus.SC_OK, mockResponseBody, SCHEMA_ID);
    verify(mockFuture).completeExceptionally(e);

    doReturn(DESERIALIZED_BODY_STR).when(mockRecordDeserializer).deserialize(mockResponseBody);
  }

  @Test
  void testErrorResponse() {
    byte[] emptyByteArray = new byte[0];
    ArgumentCaptor<VeniceServerException> serverExceptionArgumentCaptor
        = ArgumentCaptor.forClass(VeniceServerException.class);
    ArgumentCaptor<VeniceClientException> clientExceptionArgumentCaptor
        = ArgumentCaptor.forClass(VeniceClientException.class);

    transportClientCallback.completeFuture(HttpStatus.SC_INTERNAL_SERVER_ERROR, emptyByteArray, SCHEMA_ID);
    verify(mockFuture).completeExceptionally(serverExceptionArgumentCaptor.capture());
    Assert.assertEquals(serverExceptionArgumentCaptor.getValue().getMessage(), "");

    transportClientCallback.completeFuture(HttpStatus.SC_SERVICE_UNAVAILABLE, mockResponseBody, SCHEMA_ID);
    verify(mockFuture, times(2)).completeExceptionally(serverExceptionArgumentCaptor.capture());
    Assert.assertEquals(serverExceptionArgumentCaptor.getValue().getMessage(), RESPONSE_BODY_STR);

    transportClientCallback.completeFuture(HttpStatus.SC_BAD_REQUEST, emptyByteArray, SCHEMA_ID);
    verify(mockFuture, times(3)).completeExceptionally(clientExceptionArgumentCaptor.capture());
    Assert.assertEquals(clientExceptionArgumentCaptor.getValue().getMessage(), "");
  }
}
