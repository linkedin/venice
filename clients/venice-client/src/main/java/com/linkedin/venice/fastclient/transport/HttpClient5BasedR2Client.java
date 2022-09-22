package com.linkedin.venice.fastclient.transport;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_TYPE;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_PASSWORD;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestResponseBuilder;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.httpclient5.HttpClient5Utils;
import com.linkedin.venice.security.DefaultSSLFactory;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.io.CloseMode;


/**
 * This utility class could construct a HttpClient5 based R2 Client.
 *
 * TODO: get rid of R2 Client inferface completely from venice-client.
 */
public class HttpClient5BasedR2Client {
  public static class SSLConfig {
    // SSL related config
    private String sslKeyStoreType;
    private String sslKeyStoreLocation;
    private String sslKeyStorePassword;
    private String sslTrustStoreLocation;
    private String sslTrustStorePassword;
  }

  public static final String HTTP_METHOD_GET_LOWER_CASE = "get";
  public static final String HTTP_METHOD_POST_LOWER_CASE = "post";

  /**
   * The default total number of IO threads will be used by the Http client.
   */
  private static final int DEFAULT_IO_THREAD_COUNT = 48;

  private static final byte[] EMPTY_RESPONSE = new byte[0];

  public static Client getR2Client(SSLConfig sslConfig) throws Exception {
    return getR2Client(sslConfig, DEFAULT_IO_THREAD_COUNT);
  }

  public static Client getR2Client(SSLConfig sslConfig, int ioThreadCount) throws Exception {
    // Build ssl properties to generate SSLContext
    Properties sslProp = new Properties();
    sslProp.put(SSL_ENABLED, "true");
    sslProp.put(SSL_KEYSTORE_TYPE, sslConfig.sslKeyStoreType);
    sslProp.put(SSL_KEYSTORE_LOCATION, sslConfig.sslKeyStoreLocation);
    sslProp.put(SSL_KEYSTORE_PASSWORD, sslConfig.sslKeyStorePassword);
    sslProp.put(SSL_TRUSTSTORE_LOCATION, sslConfig.sslTrustStoreLocation);
    sslProp.put(SSL_TRUSTSTORE_PASSWORD, sslConfig.sslTrustStorePassword);
    SSLContext sslContext = new DefaultSSLFactory(sslProp).getSSLContext();

    return getR2Client(sslContext, ioThreadCount);
  }

  public static Client getR2Client(SSLContext sslContext, int ioThreadCount) throws Exception {
    final CloseableHttpAsyncClient client = new HttpClient5Utils.HttpClient5Builder().setIoThreadCount(ioThreadCount)
        .setSslContext(sslContext)
        // Disable cipher check for now.
        .setSkipCipherCheck(true)
        .buildAndStart();

    return new Client() {
      @Override
      public Future<RestResponse> restRequest(RestRequest request) {
        CompletableFuture<RestResponse> future = new CompletableFuture<>();
        restRequest(request, new Callback<RestResponse>() {
          @Override
          public void onError(Throwable e) {
            future.completeExceptionally(e);
          }

          @Override
          public void onSuccess(RestResponse result) {
            future.complete(result);
          }
        });
        return future;
      }

      @Override
      public Future<RestResponse> restRequest(RestRequest request, RequestContext requestContext) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void restRequest(RestRequest request, Callback<RestResponse> callback) {
        String method = request.getMethod();
        boolean isGetRequest = false;
        if (HTTP_METHOD_GET_LOWER_CASE.equalsIgnoreCase(method)) {
          isGetRequest = true;
        } else if (!HTTP_METHOD_POST_LOWER_CASE.equalsIgnoreCase(method)) {
          throw new UnsupportedOperationException("Method: " + method + " is not supported");
        }

        final SimpleRequestBuilder simpleRequestBuilder;
        if (isGetRequest) {
          simpleRequestBuilder = SimpleRequestBuilder.create(Method.GET).setUri(request.getURI());
        } else {
          simpleRequestBuilder = SimpleRequestBuilder.create(Method.POST)
              .setUri(request.getURI())
              /**
               * TODO: this is not efficient, and later, we need to provide a native HttpClient5 impl to avoid the copy.
               */
              .setBody(request.getEntity().copyBytes(), ContentType.DEFAULT_BINARY);
        }
        request.getHeaders().forEach((k, v) -> simpleRequestBuilder.addHeader(k, v));

        client.execute(simpleRequestBuilder.build(), new FutureCallback<SimpleHttpResponse>() {
          @Override
          public void completed(SimpleHttpResponse result) {
            RestResponseBuilder restResponseBuilder = new RestResponseBuilder();
            byte[] bodyBytes = result.getBodyBytes();
            if (bodyBytes == null) {
              bodyBytes = EMPTY_RESPONSE;
            }
            restResponseBuilder.setEntity(bodyBytes);
            Arrays.stream(result.getHeaders())
                .forEach(header -> restResponseBuilder.setHeader(header.getName(), header.getValue()));
            restResponseBuilder.setStatus(result.getCode());
            callback.onSuccess(restResponseBuilder.build());
          }

          @Override
          public void failed(Exception ex) {
            callback.onError(ex);
          }

          @Override
          public void cancelled() {
            callback.onError(new VeniceException("Request got cancelled"));
          }
        });
      }

      @Override
      public void restRequest(RestRequest request, RequestContext requestContext, Callback<RestResponse> callback) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void shutdown(Callback<None> callback) {
        client.close(CloseMode.GRACEFUL);
        if (callback != null) {
          callback.onSuccess(null);
        }
      }
    };
  }
}
