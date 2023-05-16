package com.linkedin.venice.router.httpclient;

import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.httpclient5.HttpClient5Utils;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.io.CloseMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class HttpClient5StorageNodeClient implements StorageNodeClient {
  private static final Logger LOGGER = LogManager.getLogger(HttpClient5StorageNodeClient.class);

  private final Random random = new Random();
  private final List<CloseableHttpAsyncClient> clientList = new ArrayList<>();

  public HttpClient5StorageNodeClient(Optional<SSLFactory> sslFactory, VeniceRouterConfig routerConfig) {
    sslFactory.orElseThrow(
        () -> new VeniceException("Param 'sslFactory' must be present while using " + this.getClass().getSimpleName()));
    /**
     * HttpClient5 needs to use JDK11 to support HTTP/2, so this class will fail fast if the Java version is below JDK11.
     */
    if (Utils.getJavaMajorVersion() < 11) {
      throw new VeniceException(
          "To enable HTTP/2 with " + this.getClass().getSimpleName()
              + ", the current process needs to use JDK11 or above");
    }
    int poolSize = routerConfig.getHttpClient5PoolSize();
    int totalIOThreadCount = routerConfig.getHttpClient5TotalIOThreadCount();
    int ioThreadCountPerClient = totalIOThreadCount / poolSize;
    for (int cur = 0; cur < poolSize; ++cur) {
      clientList.add(
          new HttpClient5Utils.HttpClient5Builder().setSslContext(sslFactory.get().getSSLContext())
              .setIoThreadCount(ioThreadCountPerClient)
              .setRequestTimeOutInMilliseconds(routerConfig.getSocketTimeout())
              .setSkipCipherCheck(routerConfig.isHttpClient5SkipCipherCheck())
              .buildAndStart());
    }
    LOGGER.info(
        "Constructing HttpClient5StorageNodeClient with pool size: {}, total io thread count: {}",
        poolSize,
        totalIOThreadCount);
  }

  @Override
  public void start() {

  }

  @Override
  public void close() {
    clientList.forEach(client -> client.close(CloseMode.GRACEFUL));
  }

  @Override
  public void query(
      Instance host,
      VenicePath path,
      Consumer<PortableHttpResponse> completedCallBack,
      Consumer<Throwable> failedCallBack,
      BooleanSupplier cancelledCallBack) throws RouterException {
    // Compose the request
    String uri = host.getHostUrl(true) + path.getLocation();
    Method method = Method.normalizedValueOf(path.getHttpMethod().name());
    SimpleRequestBuilder simpleRequestBuilder = SimpleRequestBuilder.create(method).setUri(uri);
    // Setup additional headers
    path.setupVeniceHeaders((k, v) -> simpleRequestBuilder.addHeader(k, v));
    byte[] body = path.getBody();
    if (body != null) {
      simpleRequestBuilder.setBody(body, ContentType.DEFAULT_BINARY);
    }

    getRandomClient().execute(simpleRequestBuilder.build(), new FutureCallback<SimpleHttpResponse>() {
      @Override
      public void completed(SimpleHttpResponse result) {
        completedCallBack.accept(new HttpClient5Response(result));
      }

      @Override
      public void failed(Exception ex) {
        failedCallBack.accept(ex);
      }

      @Override
      public void cancelled() {
        cancelledCallBack.getAsBoolean();
      }
    });
  }

  private CloseableHttpAsyncClient getRandomClient() {
    return clientList.get(random.nextInt(clientList.size()));
  }

  private static final class HttpClient5Response implements PortableHttpResponse {
    private final SimpleHttpResponse response;

    public HttpClient5Response(SimpleHttpResponse response) {
      this.response = response;
    }

    @Override
    public int getStatusCode() {
      return response.getCode();
    }

    @Override
    public ByteBuf getContentInByteBuf() throws IOException {
      /**
       * {@link SimpleHttpResponse#getBodyBytes()} will return null if the content length is 0.
       */
      byte[] body = response.getBodyBytes();
      return body == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(body);
    }

    @Override
    public boolean containsHeader(String headerName) {
      return response.containsHeader(headerName);
    }

    @Override
    public String getFirstHeader(String headerName) {
      Header header = response.getFirstHeader(headerName);
      return header != null ? header.getValue() : null;
    }
  }

  @Override
  public void sendRequest(VeniceMetaDataRequest request, CompletableFuture<PortableHttpResponse> responseFuture) {
    String uri = request.getUrl() + request.getQuery();
    Method method = Method.normalizedValueOf(request.getMethod());
    SimpleRequestBuilder simpleRequestBuilder = SimpleRequestBuilder.create(method).setUri(uri);
    if (request.hasTimeout()) {
      simpleRequestBuilder.setRequestConfig(
          RequestConfig.custom().setResponseTimeout(request.getTimeout(), TimeUnit.MILLISECONDS).build());
    }
    getRandomClient().execute(simpleRequestBuilder.build(), new FutureCallback<SimpleHttpResponse>() {
      @Override
      public void completed(SimpleHttpResponse result) {
        responseFuture.complete(new HttpClient5Response(result));
      }

      @Override
      public void failed(Exception ex) {
        responseFuture.completeExceptionally(ex);
      }

      @Override
      public void cancelled() {
        responseFuture.cancel(false);
      }
    });
  }
}
