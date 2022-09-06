package com.linkedin.venice.controllerapi;

public class MultiNodeResponse
    extends ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  private String[] nodes;

  public String[] getNodes() {
    return nodes;
  }

  public void setNodes(String[] nodes) {
    this.nodes = nodes;
  }
}
