package com.linkedin.venice.meta;

import javax.validation.constraints.NotNull;


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

    public Instance(@NotNull String nodeId, @NotNull String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        validatePort("port", port);
        this.port = port;
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

  private void validatePort(String name, int port) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid " + name + ": " + port);
        }
    }

  //Autogen except for .toLowerCase()
  @Override
  public int hashCode() {
    int result = nodeId != null ? nodeId.hashCode() : 0;
    result = 31 * result + host.toLowerCase().hashCode();
    result = 31 * result + port;
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

    if (getPort() != instance.getPort()) {
      return false;
    }
    if (getNodeId() != null ? !getNodeId().equals(instance.getNodeId()) : instance.getNodeId() != null) {
      return false;
    }
    return !(getHost() != null ? !getHost().equalsIgnoreCase(instance.getHost()) : instance.getHost() != null);
  }
}
