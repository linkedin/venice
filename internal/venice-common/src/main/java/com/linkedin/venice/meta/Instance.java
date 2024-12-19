package com.linkedin.venice.meta;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Utils;


/**
 * Class defines the Instance in Venice.
 *
 * // TODO: Bad name. Too generic. Let's find a better one.
 */
public class Instance {
  /**
   * Id of the node who holds this replica.
   */
  private final String nodeId;
  /**
   * Host of the node.
   */
  private final String host;
  /**
  * Port of the node used to accept the request. For controller it's admin request, for storage node it's data request.
  */
  private final int port;

  /**
   * sslPort will be the same as the HTTP port number by default, unless the sslPort number is configured intentionally.
   */
  private final int sslPort;

  private final String url;
  private final String sUrl;

  private final int grpcPort;
  private final int grpcSslPort;
  private final String grpcUrl;
  private final String grpcSslUrl;

  // TODO: generate nodeId from host and port, should be "host_port", or generate host and port from id.
  public Instance(String nodeId, String host, int port) {
    this(nodeId, host, port, port, -1, -1);
  }

  public Instance(String nodeId, String host, int port, int grpcPort, int grpcSslPort) {
    this(nodeId, host, port, port, grpcPort, grpcSslPort);
  }

  public Instance(
      @JsonProperty("nodeId") String nodeId,
      @JsonProperty("host") String host,
      @JsonProperty("port") int port,
      @JsonProperty("sslPort") int sslPort,
      @JsonProperty("grpcPort") int grpcPort,
      @JsonProperty("grpcSslPort") int grpcSslPort) {
    this.nodeId = nodeId;
    this.host = host;
    this.port = validatePort("port", port);
    this.sslPort = sslPort;
    this.url = "http://" + host + ":" + port + "/";
    this.sUrl = "https://" + host + ":" + sslPort + "/";

    this.grpcPort = grpcPort;
    this.grpcSslPort = grpcSslPort;
    this.grpcUrl = host + ":" + grpcPort;
    this.grpcSslUrl = host + ":" + grpcSslPort;
  }

  public static Instance fromHostAndPort(String hostName, int port) {
    return Instance.fromNodeId(hostName + "_" + port);
  }

  public String getHostUrl(boolean isSSL) {
    return isSSL ? sUrl : url;
  }

  public static Instance fromNodeId(String nodeId) {
    try {
      String[] parts = nodeId.split("_");
      if (parts.length != 2) {
        throw new VeniceException(); /* gets caught and new message is thrown */
      }
      String host = parts[0];
      int port = Utils.parseIntFromString(parts[1], "Port");
      return new Instance(nodeId, host, port);
    } catch (Exception e) {
      throw new VeniceException("nodeId or instanceId must be of form 'host_port', found: " + nodeId);
    }
  }

  public String getNodeId() {
    return nodeId;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public int getSslPort() {
    return sslPort;
  }

  public int getGrpcPort() {
    return grpcPort;
  }

  public int getGrpcSslPort() {
    return grpcSslPort;
  }

  public String getGrpcUrl() {
    return grpcUrl;
  }

  public String getGrpcSslUrl() {
    return grpcSslUrl;
  }

  /***
   * Convenience method for getting a host and port based url.
   * Wraps IPv6 host strings in square brackets
   * @param https sets the scheme: false for http, true for https
   * @return http(s):// + host + : + port
   */
  @JsonIgnore
  public String getUrl(boolean https) {
    String scheme = https ? HttpConstants.HTTPS : HttpConstants.HTTP;
    int portNumber = https ? sslPort : port;
    return host.contains(":") ? /* for IPv6 support per https://www.ietf.org/rfc/rfc2732.txt */
        scheme + "://[" + host + "]:" + portNumber : scheme + "://" + host + ":" + portNumber;
  }

  @JsonIgnore
  @Deprecated
  public String getUrl() {
    return getUrl(false);
  }

  private int validatePort(String name, int port) {
    if (port < 0 || port > 65535) {
      throw new IllegalArgumentException("Invalid " + name + ": " + port);
    }
    return port;
  }

  // Autogen except for .toLowerCase()
  @Override
  public int hashCode() {
    int result = nodeId != null ? nodeId.hashCode() : 0;
    result = 31 * result + host.toLowerCase().hashCode();
    result = 31 * result + port;
    return result;
  }

  // Autogen, except for the equalsIgnoreCase
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Instance instance = (Instance) o;

    if (getPort() != instance.getPort()) {
      return false;
    }
    if (getNodeId() != null ? !getNodeId().equals(instance.getNodeId()) : instance.getNodeId() != null) {
      return false;
    }
    return !(getHost() != null ? !getHost().equalsIgnoreCase(instance.getHost()) : instance.getHost() != null);
  }

  @Override
  public String toString() {
    return nodeId;
  }
}
