package com.linkedin.venice.client;

import com.google.common.net.HttpHeaders;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;


/**
 * Created by mwise on 5/16/16.
 */
public interface VeniceThinClient extends Closeable {

  static final Base64.Encoder encoder = Base64.getUrlEncoder();
  static final String STORAGE_TYPE = "storage";
  static final String B64_FORMAT = "?f=b64";

  public Future<byte[]> get(String storeName, byte[] key);

  @Override
  public void close(); /* removes exception that Closeable can throw */

  static void useResultToCompleteFuture(int statusCode, String contentType, byte[] body, CompletableFuture<byte[]> valueFuture){

    String msg = null;
    if (contentType.startsWith("text")) { // support text/plain and text/html
      msg = new String(body, StandardCharsets.UTF_8);
    }

    switch (statusCode) {
      case HttpStatus.SC_OK:
        valueFuture.complete(body);
        break;
      case HttpStatus.SC_NOT_FOUND:
        if (msg != null) {
          valueFuture.completeExceptionally(new VeniceNotFoundException(msg));
        } else {
          valueFuture.completeExceptionally(new VeniceNotFoundException());
        }
        break;
      case HttpStatus.SC_INTERNAL_SERVER_ERROR:
      case HttpStatus.SC_SERVICE_UNAVAILABLE:
        if (msg != null) {
          valueFuture.completeExceptionally(new VeniceServerErrorException(msg));
        } else {
          valueFuture.completeExceptionally(new VeniceServerErrorException());
        }
        break;
      case HttpStatus.SC_BAD_REQUEST:
      default:
        if (msg != null) {
          valueFuture.completeExceptionally(new VeniceClientException(msg));
        } else {
          valueFuture
              .completeExceptionally(new VeniceClientException("Router responds with status code: " + statusCode));
        }
    }
  }
}
