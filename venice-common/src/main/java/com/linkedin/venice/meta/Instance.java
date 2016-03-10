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

    @Override
    public boolean equals(Object other){
      if (null==other){
        return false;
      }
      if (!other.getClass().equals(this.getClass())) {
        return false;
      }
      Instance o = (Instance) other;
      if (  !(this.getHost().equals(o.getHost()))  ) {
        return false;
      }
      if (  !(this.getHttpPort()==o.getHttpPort())  ){
        return false;
      }
      return true;
    }
}
