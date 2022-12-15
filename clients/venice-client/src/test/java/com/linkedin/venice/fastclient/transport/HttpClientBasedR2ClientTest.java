package com.linkedin.venice.fastclient.transport;

import com.linkedin.common.callback.Callback;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.fastclient.utils.ClientTestUtils;
import com.linkedin.venice.utils.SslUtils;
import java.net.URI;
import org.testng.annotations.Test;


public class HttpClientBasedR2ClientTest {
  /**
   * should use the default thread count and succeed
   * @throws Exception
   */
  @Test
  public void getClientWithoutThreadCount() throws Exception {
    Client r2Client = HttpClient5BasedR2Client.getR2Client(SslUtils.getVeniceLocalSslFactory().getSSLContext());
    r2Client.shutdown(null);
  }

  /**
   * should pass as 5 is a random valid ioThreadCount
   * @throws Exception
   */
  @Test
  public void getClientWithValidThreadCount() throws Exception {
    Client r2Client = HttpClient5BasedR2Client.getR2Client(SslUtils.getVeniceLocalSslFactory().getSSLContext(), 5);
    r2Client.shutdown(null);
  }

  /**
   * should throw an exception if ioThreadCount is invalid
   * @throws Exception
   */
  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "ioThreadCount should be greater than 0")
  public void getClientWithInvalidThreadCount() throws Exception {
    HttpClient5BasedR2Client.getR2Client(SslUtils.getVeniceLocalSslFactory().getSSLContext(), 0);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = ".*is not supported")
  public void testClientRestRequestAPIWithInvalidMethodName() throws Exception {
    Client r2Client = null;
    try {
      r2Client = ClientTestUtils.getR2Client(ClientTestUtils.FastClientHTTPVariant.HTTP_2_BASED_HTTPCLIENT5);

      URI requestUri = new URI("dummy");
      RestRequest request = new RestRequestBuilder(requestUri).setMethod("invalidMethod").build();
      r2Client.restRequest(request).get();
    } finally {
      if (r2Client != null) {
        r2Client.shutdown(null);
      }
    }
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testClientRestRequestAPIWithRequestContext() throws Exception {
    Client r2Client = null;
    try {
      r2Client = ClientTestUtils.getR2Client(ClientTestUtils.FastClientHTTPVariant.HTTP_2_BASED_HTTPCLIENT5);
      URI requestUri = new URI("dummy");
      RequestContext requestContext = new RequestContext();
      RestRequest request = new RestRequestBuilder(requestUri).setMethod("get").build();
      r2Client.restRequest(request, requestContext).get();
    } finally {
      if (r2Client != null) {
        r2Client.shutdown(null);
      }
    }
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testClientRestRequestAPIWithRequestContextAndCallback() throws Exception {
    Client r2Client = null;
    try {
      r2Client = ClientTestUtils.getR2Client(ClientTestUtils.FastClientHTTPVariant.HTTP_2_BASED_HTTPCLIENT5);
      URI requestUri = new URI("dummy");
      RequestContext requestContext = new RequestContext();
      RestRequest request = new RestRequestBuilder(requestUri).setMethod("get").build();
      r2Client.restRequest(request, requestContext, new Callback<RestResponse>() {
        @Override
        public void onError(Throwable e) {
        }

        @Override
        public void onSuccess(RestResponse result) {
        }
      });
    } finally {
      if (r2Client != null) {
        r2Client.shutdown(null);
      }
    }
  }

}
