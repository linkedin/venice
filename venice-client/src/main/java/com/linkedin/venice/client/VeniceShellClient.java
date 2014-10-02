package com.linkedin.venice.client;

import com.linkedin.venice.Venice;

/**
 * Class which acts as the primary interface when calling Venice from the run-client.sh script.
 * This class will become the primary main function for the client once the code base is divided between client/server.
 */
public class VeniceShellClient {

  public static void main(String[] args) {

    Venice.execute(args);

  }

}
