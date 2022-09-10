package com.linkedin.venice.exceptions;

import com.linkedin.venice.meta.Version;
import org.apache.http.HttpStatus;


public class VeniceNoHelixResourceException extends VeniceRouterException {
  private final String resource;
  private final int version;

  public VeniceNoHelixResourceException(String resource) {
    super(getMessageFromResourceName(resource));
    this.resource = resource;
    this.version = getVersionFromResourceName(resource);
  }

  public String getResource() {
    return resource;
  }

  public int getVersion() {
    return version;
  }

  @Override
  public int getHttpStatusCode() {
    if (version == 0) { // A request for version 0 means there has not been a push to a store
      return HttpStatus.SC_BAD_REQUEST;
    } else { // If we resolve to a non-zero version, but can't find it then something went wrong
      return HttpStatus.SC_INTERNAL_SERVER_ERROR;
    }
  }

  private static String getMessageFromResourceName(String resource) {
    if (Version.isVersionTopicOrStreamReprocessingTopic(resource)
        && (Version.parseVersionFromKafkaTopicName(resource) == 0)) {
      return "There is no version for store '" + Version.parseStoreFromKafkaTopicName(resource)
          + "'.  Please push data to that store";
    } else {
      return "Resource '" + resource + "' does not exist";
    }
  }

  private static int getVersionFromResourceName(String resource) {
    if (Version.isVersionTopicOrStreamReprocessingTopic(resource)) {
      return Version.parseVersionFromKafkaTopicName(resource);
    } else {
      return -1;
    }
  }
}
