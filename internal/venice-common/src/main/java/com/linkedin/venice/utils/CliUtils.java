package com.linkedin.venice.utils;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;


/**
 * Shared utility class for Venice CLI tools (AdminTool, DuckVinciTool, etc.)
 * Contains common functionality for command-line parsing, error handling, SSL configuration, and output formatting.
 */
public class CliUtils {
  private static final String STATUS = "status";
  private static final String ERROR = "error";
  private static final String SUCCESS = "success";

  /**
   * Build SSL factory from command line SSL configuration path.
   *
   * @param cmd CommandLine containing SSL configuration arguments
   * @param sslConfigPathArg The argument name for SSL config path (e.g., "ssl-config-path")
   * @return Optional SSL factory, empty if no SSL config provided
   * @throws IOException if SSL configuration cannot be loaded
   */
  public static Optional<SSLFactory> buildSslFactory(CommandLine cmd, String sslConfigPathArg) throws IOException {
    if (cmd.hasOption(sslConfigPathArg)) {
      String sslConfigPath = cmd.getOptionValue(sslConfigPathArg);
      Properties sslProperties = SslUtils.loadSSLConfig(sslConfigPath);
      String sslFactoryClassName = sslProperties.getProperty(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
      return Optional.of(SslUtils.getSSLFactory(sslProperties, sslFactoryClassName));
    }
    return Optional.empty();
  }

  /**
   * Get required argument from command line with error context.
   */
  public static String getRequiredArgument(CommandLine cmd, String argName, String errorClause) {
    if (!cmd.hasOption(argName)) {
      printErrAndExit(argName + " is a required argument " + errorClause);
    }
    return cmd.getOptionValue(argName);
  }

  /**
   * Get optional argument from command line with default value.
   */
  public static String getOptionalArgument(CommandLine cmd, String argName, String defaultValue) {
    if (!cmd.hasOption(argName)) {
      return defaultValue;
    } else {
      return cmd.getOptionValue(argName);
    }
  }

  /**
   * Get optional argument from command line.
   */
  public static String getOptionalArgument(CommandLine cmd, String argName) {
    return getOptionalArgument(cmd, argName, null);
  }

  /**
   * Print error message and exit the application.
   */
  public static void printErrAndExit(String errorMessage) {
    Map<String, String> errMap = new HashMap<>();
    printErrAndExit(errorMessage, errMap);
  }

  /**
   * Print error message with custom messages and exit the application.
   */
  public static void printErrAndExit(String errorMessage, Map<String, String> customMessages) {
    printErr(errorMessage, customMessages);
    Utils.exit("CLI tool encountered an error, exiting now.");
  }

  /**
   * Print error message and throw exception.
   */
  public static void printErrAndThrow(Exception e, String errorMessage, Map<String, String> customMessages)
      throws Exception {
    printErr(errorMessage, customMessages);
    throw e;
  }

  /**
   * Print error message in JSON format.
   */
  public static void printErr(String errorMessage, Map<String, String> customMessages) {
    Map<String, String> errMap = new HashMap<>();
    if (customMessages != null) {
      for (Map.Entry<String, String> messagePair: customMessages.entrySet()) {
        errMap.put(messagePair.getKey(), messagePair.getValue());
      }
    }
    if (errMap.keySet().contains(ERROR)) {
      errMap.put(ERROR, errMap.get(ERROR) + " " + errorMessage);
    } else {
      errMap.put(ERROR, errorMessage);
    }
    try {
      ObjectWriter jsonWriter = ObjectMapperFactory.getInstance().writerWithDefaultPrettyPrinter();
      System.out.println(jsonWriter.writeValueAsString(errMap));
    } catch (IOException e) {
      System.out.println("{\"" + ERROR + "\":\"" + e.getMessage() + "\"}");
    }
  }

  /**
   * Print object in JSON format with pretty printing.
   */
  public static void printObject(Object obj) {
    try {
      ObjectWriter jsonWriter = ObjectMapperFactory.getInstance().writerWithDefaultPrettyPrinter();
      System.out.println(jsonWriter.writeValueAsString(obj));
    } catch (Exception e) {
      System.out.println("{\"" + ERROR + "\":\"Failed to serialize object: " + e.getMessage() + "\"}");
    }
  }

  /**
   * Print object in JSON format with optional flat formatting.
   */
  public static void printObject(Object obj, boolean flatJson) {
    try {
      ObjectWriter jsonWriter = flatJson
          ? ObjectMapperFactory.getInstance().writer()
          : ObjectMapperFactory.getInstance().writerWithDefaultPrettyPrinter();
      System.out.println(jsonWriter.writeValueAsString(obj));
    } catch (Exception e) {
      System.out.println("{\"" + ERROR + "\":\"Failed to serialize object: " + e.getMessage() + "\"}");
    }
  }

  /**
   * Print success response.
   */
  public static void printSuccess(Object response) {
    Map<String, Object> successMap = new HashMap<>();
    successMap.put(STATUS, SUCCESS);
    successMap.put("response", response);
    printObject(successMap);
  }

  /**
   * Create a CLI option and add it to the options.
   */
  public static void createOpt(String longOpt, String shortOpt, boolean hasArg, String help, Options options) {
    options.addOption(new Option(shortOpt, longOpt, hasArg, help));
  }

  /**
   * Create a command option and add it to the option group.
   */
  public static void createCommandOpt(String commandName, String description, OptionGroup group) {
    group.addOption(OptionBuilder.withLongOpt(commandName).withDescription(description).create());
  }

  /**
   * Ensure only one command is specified in the command line.
   */
  // public static <T extends Enum<T>> T ensureOnlyOneCommand(CommandLine cmd, Enum<T> commandEnum) {
  // String foundCommand = null;
  // T[] commands = commandEnumClass.getEnumConstants();
  //
  // for (T command : commands) {
  // String commandName = command.toString().toLowerCase().replace("_", "-");
  // if (cmd.hasOption(commandName)) {
  // if (foundCommand == null) {
  // foundCommand = commandName;
  // } else {
  // throw new VeniceException("Can only specify one of --" + foundCommand + " and --" + commandName);
  // }
  // }
  // }
  //
  // if (foundCommand == null) {
  // throw new VeniceException("Must supply a command");
  // }
  //
  // // Find and return the matching enum constant
  // for (T command : commands) {
  // String commandName = command.toString().toLowerCase().replace("_", "-");
  // if (commandName.equals(foundCommand)) {
  // return command;
  // }
  // }
  //
  // throw new VeniceException("Unknown command: " + foundCommand);
  // }

  /**
   * Print enhanced help with better formatting.
   */
  public static void printUsageAndExit(String toolName, OptionGroup commandGroup, Options parameterOptions) {
    String command = "java -jar " + toolName + ".jar";

    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(140);
    helpFormatter
        .printHelp(command + " --<command> [parameters]\n\nCommands:", new Options().addOptionGroup(commandGroup));

    helpFormatter.printHelp("Parameters: ", parameterOptions);

    System.out.println("\nFor detailed examples, please refer to the tool documentation.");
    Utils.exit("printUsageAndExit");
  }

  /**
   * Validate SSL configuration and print warnings if needed.
   */
  public static void validateSslConfiguration(CommandLine cmd, String sslConfigPathArg, String toolName) {
    if (!cmd.hasOption(sslConfigPathArg)) {
      System.out.println("[WARN] Running " + toolName + " without SSL.");
    }
  }

  /**
   * Read file content with home directory expansion support.
   * Expands ~ to user home directory and reads the entire file as UTF-8 string.
   */
  public static String readFile(String path) throws IOException {
    String fullPath = path.replace("~", System.getProperty("user.home"));
    byte[] encoded = Files.readAllBytes(Paths.get(fullPath));
    return new String(encoded, StandardCharsets.UTF_8).trim();
  }

  /**
   * Load properties from a file specified by a command line argument.
   * Returns empty Properties if the argument is not provided.
   */
  public static Properties loadProperties(CommandLine cmd, String argName) throws VeniceException {
    Properties properties = new Properties();
    if (cmd.hasOption(argName)) {
      String configFilePath = cmd.getOptionValue(argName);
      try (FileInputStream fis = new FileInputStream(configFilePath)) {
        properties.load(fis);
      } catch (IOException e) {
        throw new VeniceException("Cannot read file: " + configFilePath + " specified by: " + argName);
      }
    }
    return properties;
  }

  /**
   * Print object with custom print function support (enhanced version).
   */
  public static void printObject(Object response, Consumer<String> printFunction) {
    try {
      ObjectWriter jsonWriter = ObjectMapperFactory.getInstance().writerWithDefaultPrettyPrinter();
      printFunction.accept(jsonWriter.writeValueAsString(response));
      printFunction.accept("\n");
    } catch (IOException e) {
      printFunction.accept("{\"" + ERROR + "\":\"" + e.getMessage() + "\"}");
      Utils.exit("printObject");
    }
  }

  /**
   * Print success response for ControllerResponse objects.
   */
  public static void printSuccess(ControllerResponse response) {
    if (response.isError()) {
      printErrAndExit(response.getError());
    } else {
      System.out.println("{\"" + STATUS + "\":\"" + SUCCESS + "\"}");
    }
  }

  /**
   * Interface to abstract CLI argument enum functionality.
   * This allows VeniceCliUtils to work with different CLI argument enums
   * without direct dependency on specific implementations.
   */
  public interface CliArg {
    /**
     * @return the short option name (single character)
     */
    String first();

    /**
     * @return the long option name
     */
    String toString();

    /**
     * @return the argument name for display in help/error messages
     */
    String getArgName();
  }

  /**
   * Gets a required argument value using CliArg interface.
   * This adapter method allows working with different CLI argument enum implementations.
   *
   * @param cmd CommandLine object containing parsed arguments
   * @param arg CliArg implementation (e.g., AdminTool.Arg)
   * @return the argument value
   * @throws RuntimeException if argument is missing
   */
  public static String getRequiredArgument(CommandLine cmd, CliArg arg) {
    return getRequiredArgument(cmd, arg, "");
  }

  /**
   * Gets a required argument value using CliArg interface with command context.
   *
   * @param cmd CommandLine object containing parsed arguments
   * @param arg CliArg implementation (e.g., AdminTool.Arg)
   * @param command Command context for error messages
   * @return the argument value
   * @throws RuntimeException if argument is missing
   */
  public static String getRequiredArgument(CommandLine cmd, CliArg arg, Object command) {
    return getRequiredArgument(cmd, arg, "when using --" + command.toString());
  }

  /**
   * Gets a required argument value using CliArg interface with custom error clause.
   *
   * @param cmd CommandLine object containing parsed arguments
   * @param arg CliArg implementation (e.g., AdminTool.Arg)
   * @param errorClause Additional context for error messages
   * @return the argument value
   * @throws RuntimeException if argument is missing
   */
  public static String getRequiredArgument(CommandLine cmd, CliArg arg, String errorClause) {
    if (!cmd.hasOption(arg.first())) {
      printErrAndExit(arg.toString() + " is a required argument " + errorClause);
    }
    return cmd.getOptionValue(arg.first());
  }

  /**
   * Gets an optional argument value using CliArg interface.
   *
   * @param cmd CommandLine object containing parsed arguments
   * @param arg CliArg implementation (e.g., AdminTool.Arg)
   * @return the argument value or null if not present
   */
  public static String getOptionalArgument(CommandLine cmd, CliArg arg) {
    return getOptionalArgument(cmd, arg, null);
  }

  /**
   * Gets an optional argument value using CliArg interface with default value.
   *
   * @param cmd CommandLine object containing parsed arguments
   * @param arg CliArg implementation (e.g., AdminTool.Arg)
   * @param defaultValue default value to return if argument is not present
   * @return the argument value or default value if not present
   */
  public static String getOptionalArgument(CommandLine cmd, CliArg arg, String defaultValue) {
    return cmd.hasOption(arg.first()) ? cmd.getOptionValue(arg.first()) : defaultValue;
  }
}
