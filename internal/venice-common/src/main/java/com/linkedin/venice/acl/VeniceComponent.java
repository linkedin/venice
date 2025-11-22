package com.linkedin.venice.acl;

public enum VeniceComponent {
  CONTROLLER("VeniceController"), ROUTER("VeniceRouter"), SERVER("VeniceServer"), ADMIN_TOOL("VeniceAdminTool"),
  CHANGELOG_CONSUMER("VeniceChangelogConsumer"), ONLINE_PRODUCER("VeniceOnlineProducer"), PUSH_JOB("VenicePushJob"),
  DAVINCI_CLIENT("DaVinciClient"), UNSPECIFIED("Unspecified");

  private final String name;

  VeniceComponent(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
