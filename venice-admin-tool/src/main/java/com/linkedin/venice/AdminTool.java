package com.linkedin.venice;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.MultiVersionResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.StringJoiner;
import org.apache.avro.Schema;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


/**
 * Created by mwise on 5/17/16.
 */
public class AdminTool {

  public static void main(String args[])
      throws ParseException, IOException {

    Options options = new Options();
    OptionGroup commandGroup = new OptionGroup();
    for (Command c : Command.values()){
      createCommandOpt(c, commandGroup);
    }

    createOpt(Arg.ROUTER, true, "Venice router url, eg. http://localhost:54333", options);
    createOpt(Arg.CLUSTER, true, "Name of Venice cluster", options);
    createOpt(Arg.STORE, true, "Name of Venice store", options);
    createOpt(Arg.VERSION, true, "Venice store version number", options);
    createOpt(Arg.KEY_SCHEMA, true, "Path to text file with key schema", options);
    createOpt(Arg.VALUE_SCHEMA, true, "Path to text file with value schema", options);
    createOpt(Arg.OWNER, true, "Owner email for new store creation", options);

    createOpt(Arg.HELP, false, "Show usage", options);

    Options parameterOptionsForHelp = new Options();
    for (Object obj : options.getOptions()){
      Option o = (Option) obj;
      parameterOptionsForHelp.addOption(o);
    }

    options.addOptionGroup(commandGroup);

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);

    try {
      if (cmd.hasOption(Arg.HELP.first())) {
        printUsageAndExit(commandGroup, parameterOptionsForHelp);
      }


      cmd = parser.parse(options, args);

      ensureOnlyOneCommand(cmd);

      String routerHosts = getRequiredArgument(cmd, Arg.ROUTER);
      String clusterName = getRequiredArgument(cmd, Arg.CLUSTER);

      if (cmd.hasOption(Command.LIST_STORES.toString())){
        MultiStoreResponse storeResponse = ControllerClient.queryStoreList(routerHosts, clusterName);
        System.out.println(String.join("\n", storeResponse.getStores()));
      } else if (cmd.hasOption(Command.DESCRIBE_STORE.toString())){
        String storeName = getRequiredArgument(cmd, Arg.STORE, Command.DESCRIBE_STORE);
        for (String store : storeName.split(",")) {
          printStoreDescription(routerHosts, clusterName, store);
        }
      } else if (cmd.hasOption(Command.DESCRIBE_STORES.toString())){
        MultiStoreResponse storeResponse = ControllerClient.queryStoreList(routerHosts, clusterName);
        for (String store : storeResponse.getStores()) {
          printStoreDescription(routerHosts, clusterName, store);
        }
      } else if (cmd.hasOption(Command.NEXT_VERSION.toString())){
        String storeName = getRequiredArgument(cmd, Arg.STORE, Command.NEXT_VERSION);
        for (String store : storeName.split(",")) {
          VersionResponse nextResponse = ControllerClient.queryNextVersion(routerHosts, clusterName, store);
          System.out.println(nextResponse.getVersion());
        }
      } else if (cmd.hasOption(Command.CURRENT_VERSION.toString())){
        String storeName = getRequiredArgument(cmd, Arg.STORE, Command.CURRENT_VERSION);
        for (String store : storeName.split(",")) {
          VersionResponse currentResponse = ControllerClient.queryCurrentVersion(routerHosts, clusterName, store);
          System.out.println(currentResponse.getVersion());
        }
      } else if (cmd.hasOption(Command.AVAILABLE_VERSIONS.toString())){
        String storeName = getRequiredArgument(cmd, Arg.STORE, Command.AVAILABLE_VERSIONS);
        for (String store : storeName.split(",")) {
          MultiVersionResponse availableResponse = ControllerClient.queryActiveVersions(routerHosts, clusterName, store);
          System.out.println(Arrays.toString(availableResponse.getVersions()));
        }
      } else if (cmd.hasOption(Command.JOB_STATUS.toString())){
        String storeName = getRequiredArgument(cmd, Arg.STORE, Command.JOB_STATUS);
        String versionString = getRequiredArgument(cmd, Arg.VERSION, Command.JOB_STATUS);
        int version = Integer.parseInt(versionString);
        String topicName = new Version(storeName, version).kafkaTopicName();
        JobStatusQueryResponse jobStatus = ControllerClient.queryJobStatus(routerHosts, clusterName, topicName);
        System.out.println(jobStatus.getStatus());
      } else if (cmd.hasOption(Command.NEW_STORE.toString())){
        createNewStore(cmd, routerHosts, clusterName);
      } else if (cmd.hasOption(Command.SET_VERSION.toString())){
        applyVersionToStore(cmd, routerHosts, clusterName);
      } else if (cmd.hasOption(Command.ADD_SCHEMA.toString())){
        applyValueSchemaToStore(cmd, routerHosts, clusterName);
      } else {
        StringJoiner availableCommands = new StringJoiner(", ");
        for (Command c : Command.values()){
          availableCommands.add("--" + c.toString());
        }
        throw new VeniceException("Must supply one of the following commands: " + availableCommands.toString());
      }

    } catch (VeniceException e){
      printErrAndExit(e.getMessage());
    }
  }

  private static void ensureOnlyOneCommand(CommandLine cmd){
    String foundCommand = null;
    for (Command c : Command.values()){
      if (cmd.hasOption(c.toString())){
        if (null == foundCommand) {
          foundCommand = c.toString();
        } else {
          throw new VeniceException("Can only specify one of --" + foundCommand + " and --" + c.toString());
        }
      }
    }
  }


  private static void createNewStore(CommandLine cmd, String routerHosts, String clusterName)
      throws IOException {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.NEW_STORE);
    String keySchemaFile = getRequiredArgument(cmd, Arg.KEY_SCHEMA, Command.NEW_STORE);
    String keySchema = readFile(keySchemaFile);
    String valueSchemaFile = getRequiredArgument(cmd, Arg.VALUE_SCHEMA, Command.NEW_STORE);
    String valueSchema = readFile(valueSchemaFile);
    String owner = getRequiredArgument(cmd, Arg.OWNER, Command.NEW_STORE);
    verifyValidSchema(keySchema);
    verifyValidSchema(valueSchema);
    verifyConnection(routerHosts, clusterName);
    verifyStoreExistence(routerHosts, clusterName, store, false);
          /* TODO: createNewStore should be modified to require a key and value schema */
    NewStoreResponse newStore = ControllerClient.createNewStore(routerHosts, clusterName, store, owner);
    if (newStore.isError()) {
      throw new VeniceException("Error creating store " + store + ": " + newStore.getError());
    }
    System.out.println("Created Store: " + store);
    SchemaResponse keyResponse = ControllerClient.initKeySchema(routerHosts, clusterName, store, keySchema);
    SchemaResponse valueResponse = ControllerClient.addValueSchema(routerHosts, clusterName, store, valueSchema);
    for (SchemaResponse response : Arrays.asList(keyResponse, valueResponse)) {
      if (response.isError()) {
        System.err.println("Error initializing store with schema: " + response.getError());
      }
    }
  }

  private static void applyVersionToStore(CommandLine cmd, String routerHosts, String clusterName){
    String store = getRequiredArgument(cmd, Arg.STORE, Command.SET_VERSION);
    String version = getRequiredArgument(cmd, Arg.VERSION, Command.SET_VERSION);
    int intVersion = Utils.parseIntFromString(version, Arg.VERSION.name());
    verifyConnection(routerHosts, clusterName);
    verifyStoreExistence(routerHosts, clusterName, store, true);

    boolean versionExists = false;
    MultiVersionResponse allVersions = ControllerClient.queryActiveVersions(routerHosts, clusterName, store);
    if (allVersions.isError()){
      throw new VeniceException("Error querying versions for store: " + store + " -- " + allVersions.getError());
    }
    for (int v : allVersions.getVersions()){
      if (v == intVersion){
        versionExists = true;
        break;
      }
    }
    if (!versionExists){
      throw new VeniceException("Version " + version + " does not exist for store " + store + ".  Store only has versions: " + Arrays.toString(allVersions.getVersions()));
    }
    ControllerClient.overrideSetActiveVersion(routerHosts, clusterName, store, intVersion);
    System.out.println("SUCCESS");
  }

  private static void applyValueSchemaToStore(CommandLine cmd, String routerHosts, String clusterName)
      throws IOException {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.ADD_SCHEMA);
    String valueSchemaFile = getRequiredArgument(cmd, Arg.VALUE_SCHEMA, Command.ADD_SCHEMA);
    String valueSchema = readFile(valueSchemaFile);
    verifyValidSchema(valueSchema);
    verifyConnection(routerHosts, clusterName);
    verifyStoreExistence(routerHosts, clusterName, store, true);
    SchemaResponse valueResponse = ControllerClient.addValueSchema(routerHosts, clusterName, store, valueSchema);
    if (valueResponse.isError()) {
      throw new VeniceException("Error updating store with schema: " + valueResponse.getError());
    }
    System.out.println("Uploaded schema has ID: " + valueResponse.getId());
  }

  private static void printStoreDescription(String routerHosts, String clusterName, String storeName){
    VersionResponse currentResponse = ControllerClient.queryCurrentVersion(routerHosts, clusterName, storeName);
    MultiVersionResponse availableResponse = ControllerClient.queryActiveVersions(routerHosts, clusterName, storeName);
    VersionResponse nextResponse = ControllerClient.queryNextVersion(routerHosts, clusterName, storeName);
    StringBuilder output = new StringBuilder().append("Store: " + storeName + "\t");
    boolean isError = false;
    for (ControllerResponse response : Arrays.asList(currentResponse, availableResponse, nextResponse)){
      if (response.isError()){
        output.append("Error: " + response.getError());
        isError = true;
        break;
      }
    }
    if (!isError) {
      output.append("Available versions: " + Arrays.toString(availableResponse.getVersions()) + "\t")
          .append("Current version: " + currentResponse.getVersion() + "\t")
          .append("Next version: " + nextResponse.getVersion());
    }
    System.out.println(output.toString());
  }

  private static void printUsageAndExit(OptionGroup commandGroup, Options options){

    /* Commands */
    String command = "java -jar " + new java.io.File(AdminTool.class.getProtectionDomain()
        .getCodeSource()
        .getLocation()
        .getPath())
        .getName();
    new HelpFormatter().printHelp(command + " --<command> [parameters]\n\nCommands:",
        new Options().addOptionGroup(commandGroup));

    /* Parameters */
    new HelpFormatter().printHelp("Parameters: ", options);

    /* Examples */
    System.err.println("\nExamples:");
    for (Command c : Command.values()){
      StringJoiner exampleArgs = new StringJoiner(" ");
      for (Arg a : c.getRequiredArgs()){
        exampleArgs.add("--" + a.toString());
        exampleArgs.add("<" + a.toString() + ">");
      }
      System.err.println(command + " --" + c.toString() + " " + exampleArgs.toString());
    }
    System.exit(1);
  }

  private static String getRequiredArgument(CommandLine cmd, Arg arg){
    return getRequiredArgument(cmd, arg, "");
  }
  private static String getRequiredArgument(CommandLine cmd, Arg arg, Command command){
    return getRequiredArgument(cmd, arg, "when using --" + command.toString());
  }

  private static String getRequiredArgument(CommandLine cmd, Arg arg, String errorClause){
    if (!cmd.hasOption(arg.first())){
      printErrAndExit(arg.toString() + " is a required argument " + errorClause);
    }
    return cmd.getOptionValue(arg.first());
  }

  private static void verifyConnection(String routerHosts, String cluster){
    MultiStoreResponse response = ControllerClient.queryStoreList(routerHosts, cluster);
    if (response.isError()){
      throw new VeniceException("Cannot connect to cluster " + cluster + " with routers " + routerHosts + ": " + response.getError());
    }
  }

  private static void verifyStoreExistence(String routerHosts, String cluster, String storename, boolean desiredExistence){
    MultiStoreResponse storeResponse = ControllerClient.queryStoreList(routerHosts, cluster);
    if (storeResponse.isError()){
      throw new VeniceException("Error verifying store exists: " + storeResponse.getError());
    }
    boolean storeExists = false;
    for (String s : storeResponse.getStores()){
      if (s.equals(storename)){
        storeExists = true;
        break;
      }
    }
    if (storeExists != desiredExistence) {
      throw new VeniceException("Store " + storename +
          (storeExists ? " already exists" : " does not exist"));
    }
  }

  private static void verifyValidSchema(String schema){
    try {
      Schema.parse(schema);
    } catch (Exception e){
      printErrAndExit("Invalid Schema: " + schema + " -- " + e.getMessage());
    }
  }

  private static void printErrAndExit(String err){
    System.err.println(err);
    System.err.println("--help for usage");
    System.exit(1);
  }

  private static void createOpt(Arg name, boolean hasArg, String help, Options options){
    options.addOption(new Option(name.first(), name.toString(), hasArg, help));
  }


  private static void createCommandOpt(Command command, OptionGroup group){
    StringJoiner arguments = new StringJoiner(", ");
    for (Arg arg : command.getRequiredArgs()){
      arguments.add("--" + arg.toString());
    }

    group.addOption(
        OptionBuilder
            .withLongOpt(command.toString())
            .withDescription("Requires: " + arguments.toString())
            .create()
    );
  }

  static String readFile(String path) throws IOException {
    String fullPath = path.replace("~", System.getProperty("user.home"));
    byte[] encoded = Files.readAllBytes(Paths.get(fullPath));
    return new String(encoded, StandardCharsets.UTF_8).trim();
  }

}
