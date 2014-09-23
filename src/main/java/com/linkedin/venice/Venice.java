package com.linkedin.venice;

import com.linkedin.venice.client.VeniceClient;

public class Venice {

  public static void main(String[] args) {

    System.out.println("Hello Venice!");

    // Things we will need to do in workflow:
    /*
        - Set up config
        - Listen for input
        - Get input
        - Get key from input
        - Map key to partition
        - Put
    */

    VeniceClient mainClient = new VeniceClient();
    mainClient.getInput();

  }

}
