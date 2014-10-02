package com.linkedin.venice;

import com.linkedin.venice.client.VeniceClient;
import java.util.Scanner;

/**
 * Main class for Venice in IDE Settings. Will be removed once Venice Client and Server are split.
 * */
public class Venice {

  static VeniceClient mainClient = new VeniceClient();

  /*
  * Temporary main method for running the class in an IDE.
  * The preferred method will be to use the VeniceShellClient via the client script.
  * */
  public static void main(String[] args) {

    Scanner reader = new Scanner(System.in);

    while (true) {

      System.out.println("Ready for input: ");
      String input = reader.nextLine();
      String[] commandArgs = input.split(" ");

      Venice.execute(commandArgs);

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
