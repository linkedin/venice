package com.linkedin.venice.acl;

public enum VeniceComponent {
  CONTROLLER("VeniceController"), ROUTER("VeniceRouter"), SERVER("VeniceServer");

  private final String name;

  VeniceComponent(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
