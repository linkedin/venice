package com.linkedin.venice.client.store;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

import com.linkedin.common.callback.Callback;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestResponseBuilder;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.HttpTransportClient;
import java.net.URISyntaxException;
import java.util.concurrent.Future;
import org.apache.hc.client5.http.ContextBuilder;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.nio.AsyncPushConsumer;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.AsyncResponseConsumer;
import org.apache.hc.core5.http.nio.HandlerFactory;
import org.apache.hc.core5.http.nio.RequestChannel;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestTransportClient {
  private static String SERVICE_NAME = "test-service";
  private static String UPDATED_SERVICE_NAME = "test-service-2";
  private static String TEST_REQUEST = "storage/test-store";

  private static String D2_PREFIX = "d2://";
  private static String HTTP_PREFIX = "http://";

  private D2TransportClient d2TransportClient;
  private D2Client mockD2Client;

  private HttpTransportClient httpTransportClient;
  private MockableCloseableHttpAsyncClient mockHttpClient;

  private static abstract class MockableCloseableHttpAsyncClient extends CloseableHttpAsyncClient {
    @Override
    public <T> Future<T> doExecute(
        HttpHost httpHost,
        AsyncRequestProducer asyncRequestProducer,
        AsyncResponseConsumer<T> asyncResponseConsumer,
        HandlerFactory<AsyncPushConsumer> handlerFactory,
        HttpContext httpContext,
        FutureCallback<T> futureCallback) {
      return null;
    }

  }

  @BeforeMethod
  public void setUpTransportClient() {
    mockD2Client = mock(D2Client.class);
    d2TransportClient = new D2TransportClient(SERVICE_NAME, mockD2Client);

    mockHttpClient = mock(MockableCloseableHttpAsyncClient.class);
    httpTransportClient = new HttpTransportClient(HTTP_PREFIX + SERVICE_NAME, mockHttpClient);
  }

  @AfterMethod
  public void cleanUp() {
    d2TransportClient.close();
    httpTransportClient.close();
  }

  @Test
  public void testGetByPath() throws Exception {
    // test D2TransportClient
    d2TransportClient.get(TEST_REQUEST);
    ArgumentCaptor<RestRequest> d2RequestCaptor = ArgumentCaptor.forClass(RestRequest.class);

    verify(mockD2Client).restRequest(d2RequestCaptor.capture(), any(), (Callback<RestResponse>) any());
    Assert.assertEquals(D2_PREFIX + SERVICE_NAME + "/" + TEST_REQUEST, d2RequestCaptor.getValue().getURI().toString());

    ArgumentCaptor<AsyncRequestProducer> httpRequestCaptor = ArgumentCaptor.forClass(AsyncRequestProducer.class);

    when(mockHttpClient.doExecute(any(), httpRequestCaptor.capture(), any(), any(), any(), any())).thenReturn(null);

    // test HttpTransportClient
    httpTransportClient.get(TEST_REQUEST);

    httpRequestCaptor.getValue().sendRequest(new RequestChannel() {
      @Override
      public void sendRequest(HttpRequest httpRequest, EntityDetails entityDetails, HttpContext httpContext) {
        try {
          Assert.assertEquals(HTTP_PREFIX + SERVICE_NAME + "/" + TEST_REQUEST, httpRequest.getUri().toString());
        } catch (URISyntaxException e) {
          fail();
        }
      }
    }, ContextBuilder.create().build());
  }

  @Test
  public void testD2TransportClientHandlesRedirect() {
    RestResponse restResponse = new RestResponseBuilder().setStatus(HttpStatus.SC_MOVED_PERMANENTLY)
        .setHeader(HttpHeaders.LOCATION, D2_PREFIX + UPDATED_SERVICE_NAME + "/" + TEST_REQUEST)
        .build();
    RestException restException = new RestException(restResponse);
    doAnswer(invocation -> {
      Callback<RestResponse> callback = invocation.getArgument(2);
      callback.onError(restException);
      return null;
    }).when(mockD2Client).restRequest(any(), any(), any());

    d2TransportClient.get(TEST_REQUEST);
    ArgumentCaptor<RestRequest> d2RequestCaptor = ArgumentCaptor.forClass(RestRequest.class);

    verify(mockD2Client, times(2)).restRequest(d2RequestCaptor.capture(), any(), any());
    Assert.assertEquals(
        D2_PREFIX + SERVICE_NAME + "/" + TEST_REQUEST,
        d2RequestCaptor.getAllValues().get(0).getURI().toString());
    Assert.assertTrue(
        d2RequestCaptor.getAllValues().get(0).getHeaders().containsKey(HttpConstants.VENICE_ALLOW_REDIRECT));
    Assert.assertEquals(
        D2_PREFIX + UPDATED_SERVICE_NAME + "/" + TEST_REQUEST,
        d2RequestCaptor.getAllValues().get(1).getURI().toString());
    Assert.assertFalse(
        d2RequestCaptor.getAllValues().get(1).getHeaders().containsKey(HttpConstants.VENICE_ALLOW_REDIRECT));
  }
}
