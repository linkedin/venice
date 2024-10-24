package com.linkedin.venice.listener.request;

import com.linkedin.venice.exceptions.VeniceException;


public class IngestionContextRequest {
  private IngestionContextRequest() {
  }

  public static IngestionContextRequest parseGetHttpRequest(String uri, String[] requestParts) {
    if (requestParts.length == 2) {
      return new IngestionContextRequest();
    } else {
      throw new VeniceException("not a valid request for a TopicPartitionIngestionContext action: " + uri);
    }
  }

}
