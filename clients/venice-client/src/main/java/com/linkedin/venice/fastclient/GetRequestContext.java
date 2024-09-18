package com.linkedin.venice.fastclient;

import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.utils.EncodingUtils;


public class GetRequestContext extends RequestContext {
  public static final String URI_SEPARATOR = "/";
  public static final String STORAGE_QUERY_ACTION = QueryAction.STORAGE.toString().toLowerCase();

  int partitionId;
  /**
   * This field is used to store the request uri to the backend.
   */
  String requestUri;
  RetryContext retryContext; // initialize if needed for retry

  String resourceName = null;
  KeyEncodingType keyEncodingType = KeyEncodingType.NONE;
  byte[] keyBytes = null;

  GetRequestContext() {
    partitionId = -1;
    requestUri = null;
    retryContext = null;
  }

  static class RetryContext {
    boolean longTailRetryRequestTriggered;
    boolean errorRetryRequestTriggered;

    // TODO Explore whether adding a new boolean named originalWin to properly differentiate and
    // maybe add more strict tests around these 2 flags will be helpful.
    boolean retryWin;

    RetryContext() {
      longTailRetryRequestTriggered = false;
      errorRetryRequestTriggered = false;
      retryWin = false;
    }
  }

  public String computeRequestUri() {
    if (requestUri != null) {
      return requestUri;
    }
    requestUri = URI_SEPARATOR + STORAGE_QUERY_ACTION + URI_SEPARATOR + resourceName + URI_SEPARATOR + partitionId
        + URI_SEPARATOR + getEncodedKey() + getKeyEncodingSuffix();
    return requestUri;
  }

  public String getEncodedKey() {
    if (keyEncodingType == KeyEncodingType.BASE64) {
      return EncodingUtils.base64EncodeToString(keyBytes);
    }

    throw new IllegalArgumentException("Unsupported key encoding type: " + keyEncodingType);
  }

  private String getKeyEncodingSuffix() {
    if (keyEncodingType == KeyEncodingType.BASE64) {
      return "?f=b64";
    }
    return "";
  }

  public String getResourceName() {
    return resourceName;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public String getKeyEncodingType() {
    return keyEncodingType.name();
  }
}
