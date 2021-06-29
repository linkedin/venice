package com.linkedin.davinci.ingestion;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceTimeoutException;
import com.linkedin.venice.ingestion.protocol.enums.IngestionAction;
import com.linkedin.venice.utils.Time;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.*;


public class HttpClientTransport implements AutoCloseable {
  private static final Logger logger = Logger.getLogger(HttpClientTransport.class);
  private static final int CONNECTION_TIMEOUT_MS = 30 * Time.MS_PER_SECOND;
  private static final int DEFAULT_REQUEST_TIMEOUT_MS = 60 * Time.MS_PER_SECOND;
  private static final int DEFAULT_REQUEST_RETRY_WAIT_TIME_MS = 30 * Time.MS_PER_SECOND;
  private static final int DEFAULT_IO_THREAD_COUNT = 16;

  private static final RequestConfig requestConfig;

  static {
    requestConfig = RequestConfig.custom()
        .setConnectTimeout(CONNECTION_TIMEOUT_MS)
        .setConnectionRequestTimeout(CONNECTION_TIMEOUT_MS)
        .build();
  }

  private final CloseableHttpAsyncClient httpClient;
  private final String forkedProcessRequestUrl;

  public HttpClientTransport(int port) {
    this.forkedProcessRequestUrl = "http://localhost:" + port;
    IOReactorConfig ioReactorConfig = IOReactorConfig.custom().setIoThreadCount(DEFAULT_IO_THREAD_COUNT).build();
    this.httpClient = HttpAsyncClients.custom()
        .setDefaultIOReactorConfig(ioReactorConfig)
        .setDefaultRequestConfig(requestConfig)
        .build();
    this.httpClient.start();
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(this.httpClient);
  }

  /**
   * This method shoves the POST string query params into the URL so the body will only contain the byte array data
   * to make processing/deserializing easier. Please make sure the query params doesn't exceed the URL limit of 2048 chars.
   */
  public <T extends SpecificRecordBase, S extends SpecificRecordBase> T sendRequest(IngestionAction action, S param, int timeoutMs) {
    HttpPost request = new HttpPost(forkedProcessRequestUrl + "/" + action.toString());
    try {
      byte[] requestPayload = serializeIngestionActionRequest(action, param);
      request.setEntity(new ByteArrayEntity(requestPayload));
    } catch (Exception e) {
      throw new VeniceException("Unable to encode the provided byte array data", e);
    }

    HttpResponse response;
    try {
      response = this.httpClient.execute(request, null).get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new VeniceTimeoutException("Unable to finish isolated ingestion request in given " + timeoutMs + " ms.", e);
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
    return sendRequest(action, param, DEFAULT_REQUEST_TIMEOUT_MS);
  }

  public <T extends SpecificRecordBase, S extends SpecificRecordBase> T sendRequestWithRetry(IngestionAction action, S param, int timeoutMs, int maxAttempt) {
    // Sanity check for maxAttempt argument.
    if (maxAttempt <= 0) {
      throw new IllegalArgumentException("maxAttempt must be a positive integer");
    }
    T result = null;
    int retryCount = 0;
    final long startTimeIsMs = System.currentTimeMillis();
    while (true) {
      try {
        result = sendRequest(action, param, timeoutMs);
        break;
      } catch (VeniceException e) {
        retryCount++;
        if (retryCount != maxAttempt) {
          logger.warn("Encounter exception when sending request, will retry for " + retryCount + "/" + maxAttempt + " time.");
        } else {
          long totalTimeInMs = System.currentTimeMillis() - startTimeIsMs;
          throw new VeniceException("Failed to send request to remote forked process after " + maxAttempt + " attempts, total time spent in millis: " + totalTimeInMs , e);
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

  public <T extends SpecificRecordBase, S extends SpecificRecordBase> T sendRequestWithRetry(IngestionAction action, S param, int maxAttempt) {
    return sendRequestWithRetry(action, param, DEFAULT_REQUEST_TIMEOUT_MS, maxAttempt);
  }
}
