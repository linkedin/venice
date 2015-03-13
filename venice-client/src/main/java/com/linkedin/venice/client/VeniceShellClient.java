package com.linkedin.venice.client;

import com.linkedin.venice.serialization.Serializer;
import com.linkedin.venice.serialization.StringSerializer;
import kafka.utils.VerifiableProperties;

import java.util.Scanner;


/**
 * Class which acts as the primary interface when calling Venice from the run-client.sh script.
 * TODO: the shell client cli only supports String as key and value, for demo purposes.
 */
public class VeniceShellClient {

  static final Serializer<String> keySerializer = new StringSerializer(new VerifiableProperties());
  static final Serializer<String> valueSerializer = new StringSerializer(new VerifiableProperties());

  static VeniceReader<String, String> reader = new VeniceReader<String, String>(keySerializer, valueSerializer);
  static VeniceWriter<String, String> writer = new VeniceWriter<String, String>(keySerializer, valueSerializer);

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

    if (commandArgs[0].equals(PUT_COMMAND)) {
      if (commandArgs.length > 2) {
        writer.put(commandArgs[1], commandArgs[2]);
      } else {
        System.out.println("Must supply both a key and value for " + PUT_COMMAND + " operations.");
        System.out.println("USAGE");
        System.out.println(PUT_COMMAND + " key value");
      }


    } else if (commandArgs[0].equals(GET_COMMAND)) {
      if (commandArgs.length > 1) {
        System.out.println("Got: " + reader.get(commandArgs[1]));
      } else {
        System.out.println("Must supply a key for " + GET_COMMAND + " operations.");
        System.out.println("USAGE");
        System.out.println(GET_COMMAND + " key");
      }


    } else if (commandArgs[0].equals(DEL_COMMAND)) {
      if (commandArgs.length > 1) {
        writer.delete(commandArgs[1]);
      } else {
        System.out.println("Must supply a key for " + DEL_COMMAND + " operations.");
        System.out.println("USAGE");
        System.out.println(DEL_COMMAND + " key");
      }


    } else if (commandArgs[0].equals(EXIT_COMMAND)) {
      System.out.println("Goodbye!");
      System.exit(0);


    } else {
      System.out.println("Command not recognized!");
      System.out.println("Must be one of: put, get, delete, exit.");


    }
  }
}
