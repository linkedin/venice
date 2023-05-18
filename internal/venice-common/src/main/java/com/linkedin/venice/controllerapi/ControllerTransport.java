package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.HttpMethod;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ControllerTransport implements AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(ControllerTransport.class);
  private static final int CONNECTION_TIMEOUT_MS = 30 * Time.MS_PER_SECOND;
  private static final int DEFAULT_REQUEST_TIMEOUT_MS = 60 * Time.MS_PER_SECOND;

  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private static final RequestConfig REQUEST_CONFIG = getDefaultRequestConfig();

  private static RequestConfig getDefaultRequestConfig() {
    return RequestConfig.custom()
        .setConnectTimeout(CONNECTION_TIMEOUT_MS)
        .setConnectionRequestTimeout(CONNECTION_TIMEOUT_MS)
        .build();
  }

  private final CloseableHttpAsyncClient httpClient;

  public ControllerTransport(Optional<SSLFactory> sslFactory) {
    this.httpClient = HttpAsyncClients.custom()
        .setDefaultRequestConfig(this.REQUEST_CONFIG)
        .setSSLStrategy(sslFactory.isPresent() ? new SSLIOSessionStrategy(sslFactory.get().getSSLContext()) : null)
        .build();
    this.httpClient.start();
  }

  @Override
  public void close() {
    Utils.closeQuietlyWithErrorLogged(this.httpClient);
  }

  public <T extends ControllerResponse> T request(
      String controllerUrl,
      ControllerRoute route,
      QueryParams params,
      Class<T> responseType) throws ExecutionException, TimeoutException {
    return request(controllerUrl, route, params, responseType, DEFAULT_REQUEST_TIMEOUT_MS, null);
  }

  public <T extends ControllerResponse> T request(
      String controllerUrl,
      ControllerRoute route,
      QueryParams params,
      Class<T> responseType,
      int timeoutMs,
      byte[] data) throws ExecutionException, TimeoutException {
    HttpMethod httpMethod = route.getHttpMethod();
    if (httpMethod.equals(HttpMethod.GET)) {
      return executeGet(controllerUrl, route.getPath(), params, responseType, timeoutMs);
    } else if (httpMethod.equals(HttpMethod.POST)) {
      return data == null
          ? executePost(controllerUrl, route.getPath(), params, responseType, timeoutMs)
          : executePost(controllerUrl, route.getPath(), params, responseType, timeoutMs, data);
    }
    throw new VeniceException("Controller route specifies unsupported http method: " + httpMethod);
  }

  public <T extends ControllerResponse> T executeGet(
      String controllerUrl,
      String path,
      QueryParams params,
      Class<T> responseType) throws ExecutionException, TimeoutException {
    return executeGet(controllerUrl, path, params, responseType, DEFAULT_REQUEST_TIMEOUT_MS);
  }

  public <T extends ControllerResponse> T executeGet(
      String controllerUrl,
      String path,
      QueryParams params,
      Class<T> responseType,
      int timeoutMs) throws ExecutionException, TimeoutException {
    String encodedParams = URLEncodedUtils.format(params.getNameValuePairs(), StandardCharsets.UTF_8);
    HttpGet request = new HttpGet(controllerUrl + "/" + StringUtils.stripStart(path, "/") + "?" + encodedParams);
    return executeRequest(request, responseType, timeoutMs);
  }

  public <T extends ControllerResponse> T executePost(
      String controllerUrl,
      String path,
      QueryParams params,
      Class<T> responseType) throws ExecutionException, TimeoutException {
    return executePost(controllerUrl, path, params, responseType, DEFAULT_REQUEST_TIMEOUT_MS);
  }

  public <T extends ControllerResponse> T executePost(
      String controllerUrl,
      String path,
      QueryParams params,
      Class<T> responseType,
      int timeoutMs) throws ExecutionException, TimeoutException {
    HttpPost request = new HttpPost(controllerUrl + "/" + StringUtils.stripStart(path, "/"));
    try {
      request.setEntity(new UrlEncodedFormEntity(params.getNameValuePairs()));
    } catch (Exception e) {
      throw new VeniceException("Unable to encode controller query params", e);
    }
    return executeRequest(request, responseType, timeoutMs);
  }

  /**
   * This method shoves the POST string query params into the URL so the body will only contain the byte array data
   * to make processing/deserializing easier. Please make sure the query params doesn't exceed the URL limit of 2048 chars.
   */
  public <T extends ControllerResponse> T executePost(
      String controllerUrl,
      String path,
      QueryParams params,
      Class<T> responseType,
      int timeoutMs,
      byte[] data) throws TimeoutException, ExecutionException {
    String encodedParams = URLEncodedUtils.format(params.getNameValuePairs(), StandardCharsets.UTF_8);
    HttpPost request = new HttpPost(controllerUrl + "/" + path + "?" + encodedParams);
    try {
      request.setEntity(new ByteArrayEntity(data));
    } catch (Exception e) {
      throw new VeniceException("Unable to encode the provided byte array data", e);
    }
    return executeRequest(request, responseType, timeoutMs);
  }

  protected <T extends ControllerResponse> T executeRequest(
      HttpRequestBase request,
      Class<T> responseType,
      int timeoutMs) throws ExecutionException, TimeoutException {
    HttpResponse response;
    try {
      response = this.httpClient.execute(request, null).get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | TimeoutException e) {
      throw e;
    } catch (InterruptedException e) {
      throw new ExecutionException(e);
    } catch (Exception e) {
      throw new VeniceException("Unable to submit controller request", e);
    } finally {
      request.abort();
    }

    String content = null;
    try {
      content = EntityUtils.toString(response.getEntity());
    } catch (Exception e) {
      LOGGER.warn("Unable to read response content", e);
    }

    int statusCode = response.getStatusLine().getStatusCode();
    ContentType contentType = ContentType.getOrDefault(response.getEntity());
    if (contentType.getMimeType().equals(ContentType.TEXT_PLAIN.getMimeType())) {
      LOGGER.warn(
          "Controller returned unexpected response, request={}, response={}, content={}",
          request,
          response,
          content);
      ErrorType errorType = ErrorType.GENERAL_ERROR;
      if (statusCode >= 400 && statusCode < 500) {
        errorType = ErrorType.BAD_REQUEST;
      }
      throw new VeniceHttpException(statusCode, content, errorType);
    } else if (!contentType.getMimeType().equals(ContentType.APPLICATION_JSON.getMimeType())) {
      LOGGER.warn("Bad controller response, request={}, response={}, content={}", request, response, content);
      throw new VeniceHttpException(
          statusCode,
          "Controller returned unsupported content-type: " + contentType + " with content: " + content,
          ErrorType.BAD_REQUEST);
    }

    T result;
    try {
      result = OBJECT_MAPPER.readValue(content, responseType);
    } catch (Exception e) {
      LOGGER.warn("Bad controller response, request={}, response={}, content={}", request, response, content);
      throw new VeniceHttpException(statusCode, "Unable to deserialize controller response", e);
    }

    if (result.isError()) {
      throw new VeniceHttpException(statusCode, result.getError(), result.getErrorType());
    }

    if (statusCode != HttpStatus.SC_OK) {
      LOGGER.warn("Bad controller response, request={}, response={}, content={}", request, response, content);
      throw new VeniceHttpException(statusCode, "Controller returned unexpected status");
    }
    return result;
  }
}
