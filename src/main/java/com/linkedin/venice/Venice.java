package com.linkedin.venice;

import com.linkedin.venice.client.VeniceClient;
import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.server.VeniceServer;

public class Venice {

  public static final String DEFAULT_TOPIC = "default_topic";
  public static final String DEFAULT_KEY = "default_key";

  public static void main(String[] args) {

    // TODO: implement file input for config
    GlobalConfiguration.initialize("");

    VeniceClient mainClient = new VeniceClient();
    mainClient.getInput();

  }

}
