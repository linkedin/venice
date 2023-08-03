package com.linkedin.venice.listener.request;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.request.RequestHelper;
import io.netty.handler.codec.http.HttpRequest;


/**
 * {@code DictionaryFetchRequest} encapsulates a GET request to storage/storename/version on the storage node to
 * fetch the compression dictionary for that version.
 */
public class DictionaryFetchRequest {
  private final String storeName;
  private final String resourceName;

  private DictionaryFetchRequest(String storeName, String resourceName) {
    this.storeName = storeName;
    this.resourceName = resourceName;
  }

  public static DictionaryFetchRequest parseGetHttpRequest(HttpRequest request) {
    String uri = request.uri();
    String[] requestParts = RequestHelper.getRequestParts(uri);

    if (requestParts.length == 4) {
      // [0]""/[1]"action"/[2]"store"/[3]"version"
      String storeName = requestParts[2];
      int storeVersion = Integer.parseInt(requestParts[3]);
      String topicName = Version.composeKafkaTopic(storeName, storeVersion);
      return new DictionaryFetchRequest(storeName, topicName);
    } else {
      throw new VeniceException("Not a valid request for a DICTIONARY action: " + uri);
    }
  }

  public String getResourceName() {
    return resourceName;
  }

  public String getStoreName() {
    return storeName;
  }
}
