package com.linkedin.venice.client;

import java.util.Scanner;

/**
 * Class which acts as the primary interface when calling Venice from the run-client.sh script.
 */
public class VeniceShellClient {

  static VeniceClient mainClient = new VeniceClient();

  /*
  * Main method for running the class in an IDE.
  * The preferred method will be to use the VeniceShellClient via the client script.
  * */
  public static void main(String[] args) {

    // Running in IDE; use interactive shell
    if (args.length < 2) {

      Scanner reader = new Scanner(System.in);

      while (true) {

        System.out.println("Ready for input: ");
        String input = reader.nextLine();
        String[] commandArgs = input.split(" ");

        execute(commandArgs);

      }

    } else {

      // executed from the venice-client.sh script: simply pass the arguments onwards
      execute(args);

    }

  }

  /**
   * A function used to send client inputs into the Venice Server.
   * This method is used by both the Venice main class and the VeniceShellClient
   * */
  public static void execute(String[] commandArgs) {

    if (commandArgs[0].equals("put")) {

      mainClient.put(commandArgs[1], commandArgs[2]);

    } else if (commandArgs[0].equals(("get"))) {

      System.out.println("Got: " + mainClient.get(commandArgs[1]));

    } else if (commandArgs[0].equals("delete")) {

      mainClient.delete(commandArgs[1]);

    }

  }

}
