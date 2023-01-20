package com.linkedin.venice.client.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.store.transport.TransportClientCallback;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import java.util.concurrent.CompletableFuture;
import org.apache.http.HttpStatus;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestTransportClientHttpCallback {
  private static int SCHEMA_ID = 1;
  private static String RESPONSE_BODY_STR = "serialized-body";
  private byte[] mockResponseBody;

  private CompletableFuture<TransportClientResponse> mockFuture;
  private TransportClientCallback transportClientCallback;

  @BeforeMethod
  public void setUp() {
    mockResponseBody = RESPONSE_BODY_STR.getBytes();
    mockFuture = mock(CompletableFuture.class);

    transportClientCallback = new TransportClientCallback(mockFuture);
  }

  @Test
  void testRawResponse() {
    TransportClientCallback rawResponseCallback = new TransportClientCallback(mockFuture);
    rawResponseCallback.completeFuture(HttpStatus.SC_OK, SCHEMA_ID, CompressionStrategy.NO_OP, mockResponseBody);

    verify(mockFuture).complete(new TransportClientResponse(SCHEMA_ID, CompressionStrategy.NO_OP, mockResponseBody));
  }

  @Test
  void testNormalResponse() {
    transportClientCallback.completeFuture(HttpStatus.SC_OK, SCHEMA_ID, CompressionStrategy.NO_OP, mockResponseBody);
    verify(mockFuture).complete(new TransportClientResponse(SCHEMA_ID, CompressionStrategy.NO_OP, mockResponseBody));

    transportClientCallback
        .completeFuture(HttpStatus.SC_NOT_FOUND, SCHEMA_ID, CompressionStrategy.NO_OP, mockResponseBody);
    verify(mockFuture).complete(null);
  }

  @Test
  void testErrorResponse() {
    byte[] emptyByteArray = new byte[0];
    ArgumentCaptor<VeniceClientHttpException> serverExceptionArgumentCaptor =
        ArgumentCaptor.forClass(VeniceClientHttpException.class);
    ArgumentCaptor<VeniceClientException> clientExceptionArgumentCaptor =
        ArgumentCaptor.forClass(VeniceClientException.class);

    transportClientCallback
        .completeFuture(HttpStatus.SC_INTERNAL_SERVER_ERROR, SCHEMA_ID, CompressionStrategy.NO_OP, emptyByteArray);
    verify(mockFuture).completeExceptionally(serverExceptionArgumentCaptor.capture());
    Assert.assertEquals(serverExceptionArgumentCaptor.getValue().getMessage(), "http status: 500");

    transportClientCallback
        .completeFuture(HttpStatus.SC_SERVICE_UNAVAILABLE, SCHEMA_ID, CompressionStrategy.NO_OP, mockResponseBody);
    verify(mockFuture, times(2)).completeExceptionally(serverExceptionArgumentCaptor.capture());
    Assert
        .assertEquals(serverExceptionArgumentCaptor.getValue().getMessage(), "http status: 503, " + RESPONSE_BODY_STR);

    transportClientCallback
        .completeFuture(HttpStatus.SC_BAD_REQUEST, SCHEMA_ID, CompressionStrategy.NO_OP, emptyByteArray);
    verify(mockFuture, times(3)).completeExceptionally(clientExceptionArgumentCaptor.capture());
    Assert.assertEquals(clientExceptionArgumentCaptor.getValue().getMessage(), "http status: 400");
  }
}
