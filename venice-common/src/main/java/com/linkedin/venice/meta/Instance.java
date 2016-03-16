package com.linkedin.venice.meta;

import javax.validation.constraints.NotNull;


/**
 * Class defines the Instance in Venice.
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
     * Port of the node used to accept the admin request.
     */
    private final int adminPort;
    /**
     * Port of the node used to accept the read request.
     */
    private final int httpPort;

    public Instance(@NotNull String nodeId, @NotNull String host, int adminPort, int httpPort) {
        this.nodeId = nodeId;
        this.host = host;
        validatePort("http port", httpPort);
        validatePort("admin port",adminPort);
        this.adminPort = adminPort;
        this.httpPort = httpPort;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getAdminPort() {
        return adminPort;
    }

    public int getHttpPort() {
        return httpPort;
    }

    private void validatePort(String name, int port) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid " + name + ": " + port);
        }
    }

  //Autogen
  @Override
  public int hashCode() {
    int result = nodeId != null ? nodeId.hashCode() : 0;
    result = 31 * result + host.hashCode();
    result = 31 * result + adminPort;
    result = 31 * result + httpPort;
    return result;
  }

  //Autogen, except for the equalsIgnoreCase
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Instance instance = (Instance) o;

    if (getAdminPort() != instance.getAdminPort()) {
      return false;
    }
    if (getHttpPort() != instance.getHttpPort()) {
      return false;
    }
    if (getNodeId() != null ? !getNodeId().equals(instance.getNodeId()) : instance.getNodeId() != null) {
      return false;
    }
    return !(getHost() != null ? !getHost().equalsIgnoreCase(instance.getHost()) : instance.getHost() != null);
  }
}
