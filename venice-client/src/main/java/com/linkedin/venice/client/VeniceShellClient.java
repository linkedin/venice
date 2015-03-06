package com.linkedin.venice.client;

import java.util.Scanner;


/**
 * Class which acts as the primary interface when calling Venice from the run-client.sh script.
 */
public class VeniceShellClient {

  static VeniceClient mainClient = new VeniceClient();

  private static final String PUT_COMMAND = "put";
  private static final String DEL_COMMAND = "delete";
  private static final String GET_COMMAND = "get";
  private static final String EXIT_COMMAND = "exit";

  /*
  * Main method for running the class in the interactive mode.
  * The preferred method will be to use the VeniceShellClient via the client script.
  * */
  public static void main(String[] args) {
    // Use interactive shell
    if (args.length < 2) {
      System.out.println("Using interactive shell...");
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
   * Error checking is required for commands coming from the interactive shell
   * */
  public static void execute(String[] commandArgs) {

    switch (commandArgs[0]) {

      case PUT_COMMAND: // PUT

        if (commandArgs.length > 2) {
          mainClient.put(commandArgs[1].getBytes(), commandArgs[2].getBytes());
        } else {
          System.out.println("Must supply both a key and value for " + PUT_COMMAND + " operations.");
          System.out.println("USAGE");
          System.out.println(PUT_COMMAND + " key value");
        }

        break;

      case GET_COMMAND: // GET

        if (commandArgs.length > 1) {
          System.out.println("Got: " + mainClient.get(commandArgs[1].getBytes()));
        } else {
          System.out.println("Must supply a key for " + GET_COMMAND + " operations.");
          System.out.println("USAGE");
          System.out.println(GET_COMMAND + " key");
        }

        break;

      case DEL_COMMAND: // DEL

        if (commandArgs.length > 1) {
          mainClient.delete(commandArgs[1].getBytes());
        } else {
          System.out.println("Must supply a key for " + DEL_COMMAND + " operations.");
          System.out.println("USAGE");
          System.out.println(DEL_COMMAND + " key");
        }

        break;

      case EXIT_COMMAND:

        System.out.println("Goodbye!");
        System.exit(0);

        break;

      default:

        System.out.println("Command not recognized!");
        System.out.println("Must be one of: put, get, delete, exit.");

        break;
    }
  }
}
