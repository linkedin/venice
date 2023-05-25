package com.linkedin.venice.D2;

import static com.linkedin.venice.HttpConstants.HTTPS_POST;
import static com.linkedin.venice.HttpConstants.HTTP_GET;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLFactory;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class D2ClientUtils {
  private static final Logger LOGGER = LogManager.getLogger(D2ClientUtils.class);
  private static long DEFAULT_D2_STARTUP_TIMEOUT_MS = 5_000;
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
      throw new VeniceException("D2 client failed to sent request, " + requestPath, e);
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

  /**
   * Utility function for building and starting a d2 client.
   *
   * @param d2ZkHost d2 zk host url
   * @param d2ZkHostToClientEnvelopeMap a map that can be passed in and stored that keeps track of existing d2 clients
   *                                    to avoid redundant client builds.
   * @param sslFactory optional for setting up ssl parameters for the built client.  Won't take effect if the client was already
   *                   built for a given zk host and is in the d2ZkHostToClientEnvelopeMap
   * @return the built d2 client
   */
  public static D2Client getStartedD2Client(
      String d2ZkHost,
      Map<String, D2ClientEnvelope> d2ZkHostToClientEnvelopeMap,
      Optional<SSLFactory> sslFactory) {
    D2ClientEnvelope d2ClientEnvelope = d2ZkHostToClientEnvelopeMap.computeIfAbsent(d2ZkHost, zkHost -> {
      String fsBasePath = getUniqueTempPath("d2");
      D2Client d2Client = new D2ClientBuilder().setZkHosts(d2ZkHost)
          .setSSLContext(sslFactory.map(SSLFactory::getSSLContext).orElse(null))
          .setIsSSLEnabled(sslFactory.isPresent())
          .setSSLParameters(sslFactory.map(SSLFactory::getSSLParameters).orElse(null))
          .setFsBasePath(fsBasePath)
          .setEnableSaveUriDataOnDisk(true)
          .build();
      D2ClientUtils.startClient(d2Client);
      return new D2ClientEnvelope(d2Client, fsBasePath);
    });
    return d2ClientEnvelope.d2Client;
  }

  // TODO: This code is copied from venice-common, but bringing it in introduces a circular dependency. Need to take
  // time
  // to sort that out.
  public static String getUniqueTempPath(String prefix) {
    return Paths.get(FileUtils.getTempDirectoryPath(), getUniqueString(prefix)).toAbsolutePath().toString();
  }

  public static String getUniqueString(String prefix) {
    return String.format("%s_%x_%x", prefix, System.nanoTime(), ThreadLocalRandom.current().nextInt());
  }

  public static final class D2ClientEnvelope implements Closeable {
    D2Client d2Client;
    String fsBasePath;

    D2ClientEnvelope(D2Client d2Client, String fsBasePath) {
      this.d2Client = d2Client;
      this.fsBasePath = fsBasePath;
    }

    @Override
    public void close() throws IOException {
      D2ClientUtils.shutdownClient(d2Client);
      try {
        FileUtils.deleteDirectory(new File(fsBasePath));
      } catch (IOException e) {
        LOGGER.info("Error in cleaning up: {}", fsBasePath);
      }
    }
  }
}
