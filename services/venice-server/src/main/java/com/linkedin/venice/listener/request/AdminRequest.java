package com.linkedin.venice.listener.request;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ServerAdminAction;
import com.linkedin.venice.meta.Version;
import io.netty.handler.codec.http.HttpRequest;
import java.net.URI;


/**
 * {@code AdminRequest} encapsulates an admin request from server admin tools.
 */
public class AdminRequest {
  private final String storeVersion;
  private final String storeName;
  private final ServerAdminAction serverAdminAction;
  private final Integer partition;

  private AdminRequest(String storeVersion, ServerAdminAction action, Integer partition) {
    this.storeVersion = storeVersion;
    this.storeName = Version.parseStoreFromKafkaTopicName(storeVersion);
    this.serverAdminAction = action;
    this.partition = partition;
  }

  public static AdminRequest parseAdminHttpRequest(HttpRequest request, URI fullUri) {
    String[] requestParts = fullUri.getRawPath().split("/");
    // [0]""/[1]"action"/[2]"store_version"/[3]"admin_action"/[4](optional)"partition_id"
    if (requestParts.length >= 4 && requestParts.length <= 5) {
      String topicName = requestParts[2];
      if (!Version.isVersionTopic(topicName)) {
        throw new VeniceException("Invalid store version for an ADMIN action: " + request.uri());
      }
      ServerAdminAction serverAdminAction = ServerAdminAction.valueOf(requestParts[3].toUpperCase());
      Integer partition = (requestParts.length > 4) ? Integer.valueOf(requestParts[4]) : null;

      return new AdminRequest(topicName, serverAdminAction, partition);
    } else {
      throw new VeniceException("Not a valid request for an ADMIN action: " + request.uri());
    }
  }

  public static AdminRequest parseAdminGrpcRequest(com.linkedin.venice.protocols.AdminRequest request) {
    try {
      String topicName = request.getResourceName();
      ServerAdminAction serverAdminAction = ServerAdminAction.valueOf(request.getServerAdminAction().toUpperCase());
      Integer partition = request.hasPartition() ? request.getPartition() : null;
      if (!Version.isVersionTopic(topicName)) {
        throw new VeniceException("Invalid store version for an ADMIN action: " + serverAdminAction);
      }
      return new AdminRequest(topicName, serverAdminAction, partition);
    } catch (IllegalArgumentException e) {
      throw new VeniceException("Invalid server admin action: " + request.getServerAdminAction());
    }
  }

  public String getStoreName() {
    return this.storeName;
  }

  public String getStoreVersion() {
    return this.storeVersion;
  }

  public ServerAdminAction getServerAdminAction() {
    return this.serverAdminAction;
  }

  public Integer getPartition() {
    return this.partition;
  }
}
