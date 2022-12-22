package com.linkedin.davinci.ingestion;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.deserializeIngestionActionResponse;
import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.serializeIngestionActionRequest;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceTimeoutException;
import com.linkedin.venice.httpclient.HttpClientUtils;
import com.linkedin.venice.ingestion.protocol.enums.IngestionAction;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class HttpClientTransport implements AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(HttpClientTransport.class);
  private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 30 * Time.MS_PER_SECOND;
  private static final int DEFAULT_SOCKET_TIMEOUT_MS = 30 * Time.MS_PER_SECOND;
  private static final int DEFAULT_REQUEST_RETRY_WAIT_TIME_MS = 1 * Time.MS_PER_SECOND;

  private static final int DEFAULT_REQUEST_RETRY_COUNT = 10;
  private static final int DEFAULT_MAX_CONNECTION_PER_ROUTE = 2;
  private static final int DEFAULT_MAX_CONNECTION_TOTAL = 10;
  private static final int DEFAULT_IDLE_CONNECTION_CLEANUP_THRESHOLD_IN_MINUTES = 3 * Time.MINUTES_PER_HOUR;
  private static final int DEFAULT_IO_THREAD_COUNT = 16;
  private static final String HTTP = "http";
  private static final String HTTPS = "https";

  private final CloseableHttpAsyncClient httpClient;
  private final String forkedProcessRequestUrl;
  private final int requestTimeoutInSeconds;

  public HttpClientTransport(Optional<SSLFactory> sslFactory, int port, int requestTimeoutInSeconds) {
    this.forkedProcessRequestUrl = (sslFactory.isPresent() ? HTTPS : HTTP) + "://" + Utils.getHostName() + ":" + port;
    this.requestTimeoutInSeconds = requestTimeoutInSeconds;
    this.httpClient =
        HttpClientUtils
            .getMinimalHttpClientWithConnManager(
                DEFAULT_IO_THREAD_COUNT,
                DEFAULT_MAX_CONNECTION_PER_ROUTE,
                DEFAULT_MAX_CONNECTION_TOTAL,
                DEFAULT_SOCKET_TIMEOUT_MS,
                DEFAULT_CONNECTION_TIMEOUT_MS,
                sslFactory,
                Optional.empty(),
                Optional.empty(),
                true,
                DEFAULT_IDLE_CONNECTION_CLEANUP_THRESHOLD_IN_MINUTES)
            .getClient();
    httpClient.start();
  }

  @Override
  public void close() {
    Utils.closeQuietlyWithErrorLogged(this.httpClient);
  }

  /**
   * This method shoves the POST string query params into the URL so the body will only contain the byte array data
   * to make processing/deserializing easier. Please make sure the query params doesn't exceed the URL limit of 2048 chars.
   */
  public <T extends SpecificRecordBase, S extends SpecificRecordBase> T sendRequest(
      IngestionAction action,
      S param,
      int requestTimeoutInSeconds) {
    HttpPost request = new HttpPost(forkedProcessRequestUrl + "/" + action.toString());
    try {
      byte[] requestPayload = serializeIngestionActionRequest(action, param);
      request.setEntity(new ByteArrayEntity(requestPayload));
    } catch (Exception e) {
      throw new VeniceException("Unable to encode the provided byte array data", e);
    }

    HttpResponse response;
    try {
      response = this.httpClient.execute(request, null).get(requestTimeoutInSeconds, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new VeniceTimeoutException(
          "Unable to finish isolated ingestion request in given " + requestTimeoutInSeconds + " s.",
          e);
    } catch (InterruptedException e) {
      // Keep the interruption flag.
      Thread.currentThread().interrupt();
      throw new VeniceException("Isolated ingestion request was interrupted", e);
    } catch (Exception e) {
      throw new VeniceException("Encounter exception when submitting isolated ingestion request", e);
    } finally {
      request.abort();
    }

    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode != HttpStatus.SC_OK) {
      throw new VeniceHttpException(statusCode, "Isolated ingestion server returned unexpected status");
    }

    byte[] responseContent;
    try {
      responseContent = EntityUtils.toByteArray(response.getEntity());
    } catch (Exception e) {
      throw new VeniceException("Unable to read response content", e);
    }
    return deserializeIngestionActionResponse(action, responseContent);
  }

  public <T extends SpecificRecordBase, S extends SpecificRecordBase> T sendRequest(IngestionAction action, S param) {
    return sendRequestWithRetry(action, param, DEFAULT_REQUEST_RETRY_COUNT);
  }

  public <T extends SpecificRecordBase, S extends SpecificRecordBase> T sendRequestWithRetry(
      IngestionAction action,
      S param,
      int maxAttempt) {
    // Sanity check for maxAttempt argument.
    if (maxAttempt <= 0) {
      throw new IllegalArgumentException("maxAttempt must be a positive integer");
    }
    T result;
    int retryCount = 0;
    final long startTimeIsMs = System.currentTimeMillis();
    while (true) {
      try {
        result = sendRequest(action, param, requestTimeoutInSeconds);
        break;
      } catch (VeniceException e) {
        retryCount++;
        if (retryCount != maxAttempt) {
          LOGGER.warn("Encounter exception when sending request, will retry for {} / {} time.", retryCount, maxAttempt);
        } else {
          long totalTimeInMs = System.currentTimeMillis() - startTimeIsMs;
          throw new VeniceException(
              "Failed to send request to remote forked process after " + maxAttempt
                  + " attempts, total time spent in millis: " + totalTimeInMs,
              e);
        }
      }
      try {
        Thread.sleep(DEFAULT_REQUEST_RETRY_WAIT_TIME_MS);
      } catch (InterruptedException e) {
        throw new VeniceException(e);
      }
    }
    return result;
  }
}
