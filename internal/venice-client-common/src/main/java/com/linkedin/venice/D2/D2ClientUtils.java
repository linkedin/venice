package com.linkedin.venice.D2;

import static com.linkedin.venice.HttpConstants.HTTPS_POST;
import static com.linkedin.venice.HttpConstants.HTTP_GET;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.exceptions.VeniceException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class D2ClientUtils {
  private static final Logger LOGGER = LogManager.getLogger(D2ClientUtils.class);
  private static long DEFAULT_D2_STARTUP_TIMEOUT_MS = 60_000;
  private static long DEFAULT_D2_SHUTDOWN_TIMEOUT_MS = 60_000;

  public static void startClient(D2Client client) {
    startClient(client, DEFAULT_D2_STARTUP_TIMEOUT_MS);
  }

  public static void startClient(D2Client client, long timeoutInMs) {
    CompletableFuture<Void> future = new CompletableFuture<>();

    client.start(new Callback<None>() {
      @Override
      public void onError(Throwable e) {
        future.completeExceptionally(e);
      }

      @Override
      public void onSuccess(None result) {
        future.complete(null);
      }
    });

    try {
      future.get(timeoutInMs, TimeUnit.MILLISECONDS);
    } catch (Throwable e) {
      String msg = "D2 client startup failed.";
      LOGGER.error(msg, e);
      throw new VeniceException(msg, e);
    }
  }

  public static void shutdownClient(D2Client client) {
    shutdownClient(client, DEFAULT_D2_SHUTDOWN_TIMEOUT_MS);
  }

  public static void shutdownClient(D2Client client, long timeoutInMs) {
    long startTime = System.currentTimeMillis();
    CompletableFuture<Void> future = new CompletableFuture<>();

    try {
      client.shutdown(new Callback<None>() {
        @Override
        public void onError(Throwable e) {
          future.completeExceptionally(e);
        }

        @Override
        public void onSuccess(None result) {
          future.complete(null);
        }
      });
    } catch (RejectedExecutionException e) {
      future.completeExceptionally(e);
    }

    try {
      future.get(timeoutInMs, TimeUnit.MILLISECONDS);
      LOGGER.info("D2 client shutdown took {} ms.", System.currentTimeMillis() - startTime);
    } catch (ExecutionException e) {
      LOGGER.warn("D2 client shutdown failed.", e.getCause());
    } catch (TimeoutException e) {
      LOGGER.warn("D2 client shutdown timed out after {} ms.", System.currentTimeMillis() - startTime);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public static RestRequest createD2GetRequest(String requestPath) {
    return createD2GetRequest(requestPath, Collections.EMPTY_MAP);
  }

  public static RestRequest createD2GetRequest(String requestPath, Map<String, String> headers) {
    URI requestUri;
    try {
      requestUri = new URI(requestPath);
    } catch (URISyntaxException e) {
      throw new VeniceException("Failed to create URI for path " + requestPath, e);
    }

    return new RestRequestBuilder(requestUri).setMethod(HTTP_GET).setHeaders(headers).build();
  }

  public static RestResponse sendD2GetRequest(String requestPath, D2Client client) {
    RestResponse response;
    try {
      response = client.restRequest(createD2GetRequest(requestPath)).get();
    } catch (Exception e) {
      if (e.getCause() instanceof RestException) {
        response = ((RestException) e.getCause()).getResponse();
      } else {
        throw new VeniceException("D2 client failed to send request, " + requestPath, e);
      }
    }

    return response;
  }

  public static RestRequest createD2PostRequest(String requestPath, Map<String, String> headers, byte[] body) {
    URI requestUri;
    try {
      requestUri = new URI(requestPath);
    } catch (URISyntaxException e) {
      throw new VeniceException("Failed to create URI for path " + requestPath, e);
    }

    RestRequestBuilder builder =
        new RestRequestBuilder(requestUri).setMethod(HTTPS_POST).setHeaders(headers).setEntity(body);
    return builder.build();
  }
}
