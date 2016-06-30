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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


/**
 * Created by mwise on 5/17/16.
 */
public class AdminTool {

  /* query actions */
  public static final String NEXT = "next"; /* The next version number for the store that is available */
  public static final String AVAILABLE = "available"; /* All versions that the storage nodes could serve */
  public static final String CURRENT = "current"; /* the version which the router will serve from */
  public static final String JOB = "job"; /* job status (store and version ( */
  public static final String LIST = "list"; /* list all stores */
  public static final String DESCRIBE = "describe"; /* details about specified store or all stores */
  public static final List<String> QUERY_OPTIONS = Arrays.asList(NEXT, AVAILABLE, CURRENT, JOB, LIST, DESCRIBE);

  /* create options */
  public static final String STORE = "store";
  public static final List<String> CREATE_OPTIONS = Arrays.asList(STORE);

  public static void main(String args[])
      throws ParseException, IOException {

    Options options = new Options();
    createOpt(Arg.ROUTER, true, "REQUIRED: Venice router url, eg. http://localhost:54333", options);
    createOpt(Arg.CLUSTER, true, "REQUIRED: Name of Venice cluster", options);
    createOpt(Arg.STORE, true, "Name of Venice store", options);
    createOpt(Arg.VERSION, true, "Venice store version number", options);
    createOpt(Arg.QUERY, true, "OPTIONAL: Query one of: " + String.join(", ", QUERY_OPTIONS), options);
    createOpt(Arg.NEW, true, "OPTIONAL: Create one of: " + String.join(", ", CREATE_OPTIONS), options);
    createOpt(Arg.KEY_SCHEMA, true, "Path to text file with key schema", options);
    createOpt(Arg.VALUE_SCHEMA, true, "Path to text file with value schema", options);
    createOpt(Arg.OWNER, true, "Owner email for new store creation", options);
    createOpt(Arg.HELP, false, "Show usage", options);

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);

    try {
      if (cmd.hasOption(Arg.HELP.first())) {
        printUsageAndExit(options);
      }
      if (cmd.hasOption(Arg.QUERY.first()) && cmd.hasOption(Arg.NEW.first())) {
        throw new VeniceException("Must only specify one of --" + Arg.QUERY + " or --" + Arg.NEW);
      }
      String routerHosts = getRequiredArgument(cmd, Arg.ROUTER);
      String clusterName = getRequiredArgument(cmd, Arg.CLUSTER);

      if (cmd.hasOption(Arg.QUERY.first())) {
        handleQueryAction(cmd, routerHosts, clusterName);
      } else if (cmd.hasOption(Arg.NEW.first())) {
        String newAction = cmd.getOptionValue(Arg.NEW.first());
        switch (newAction.toLowerCase()) {
          case STORE:
            createNewStore(cmd, routerHosts, clusterName);
            break;
          default:
            throw new VeniceException(newAction + " NOT IMPLEMENTED" + " for --" + Arg.NEW.toString());
        }
      } else { /* not --query or --new */
        throw new VeniceException("--query or --new is a required argument");
      }
    } catch (VeniceException e){
      printErrAndExit(e.getMessage());
    }
  }

  private static void handleQueryAction(CommandLine cmd, String routerHosts, String clusterName){
    String queryAction = cmd.getOptionValue(Arg.QUERY.first());
    String queryClause = " when using --" + Arg.QUERY.toString();
    String storeName;
    switch (queryAction.toLowerCase()) {
      case NEXT:
        storeName = getRequiredArgument(cmd, Arg.STORE, queryClause);
        for (String store : storeName.split(",")) {
          VersionResponse nextResponse = ControllerClient.queryNextVersion(routerHosts, clusterName, store);
          System.out.println(nextResponse.getVersion());
        }
        break;
      case JOB:
        storeName = getRequiredArgument(cmd, Arg.STORE, queryClause);
        getRequiredArgument(cmd, Arg.VERSION, "querying " + JOB + " status");
        int version = Integer.parseInt(cmd.getOptionValue(Arg.VERSION.first()));
        String topicName = new Version(storeName, version).kafkaTopicName();
        JobStatusQueryResponse jobStatus = ControllerClient.queryJobStatus(routerHosts, clusterName, topicName);
        System.out.println(jobStatus.getStatus());
        break;
      case CURRENT:
        storeName = getRequiredArgument(cmd, Arg.STORE, queryClause);
        for (String store : storeName.split(",")) {
          VersionResponse currentResponse = ControllerClient.queryCurrentVersion(routerHosts, clusterName, store);
          System.out.println(currentResponse.getVersion());
        }
        break;
      case AVAILABLE:
        storeName = getRequiredArgument(cmd, Arg.STORE, queryClause);
        for (String store : storeName.split(",")) {
          MultiVersionResponse availableResponse = ControllerClient.queryActiveVersions(routerHosts, clusterName, store);
          System.out.println(Arrays.toString(availableResponse.getVersions()));
        }
        break;
      case DESCRIBE:
        if (cmd.hasOption(Arg.STORE.first())) {
          storeName = cmd.getOptionValue(Arg.STORE.first());
          for (String store : storeName.split(",")) {
            printStoreDescription(routerHosts, clusterName, store);
          }
        } else { /* describe all stores */
          MultiStoreResponse storeResponse = ControllerClient.queryStoreList(routerHosts, clusterName);
          for (String store : storeResponse.getStores()) {
            printStoreDescription(routerHosts, clusterName, store);
          }
        }
        break;
      case LIST:
        MultiStoreResponse storeResponse = ControllerClient.queryStoreList(routerHosts, clusterName);
        System.out.println(String.join("\n", storeResponse.getStores()));
        break;
      default:
        throw new VeniceException(queryAction + " NOT IMPLEMENTED" + " for --" + Arg.QUERY.toString());
    }
  }


  private static void createNewStore(CommandLine cmd, String routerHosts, String clusterName)
      throws IOException {
    String newClause = " when using --" + Arg.NEW.toString();
    String store = getRequiredArgument(cmd, Arg.STORE, newClause);
    String keySchemaFile = getRequiredArgument(cmd, Arg.KEY_SCHEMA, newClause);
    String keySchema = readFile(keySchemaFile);
    String valueSchemaFile = getRequiredArgument(cmd, Arg.VALUE_SCHEMA, newClause);
    String valueSchema = readFile(valueSchemaFile);
    String owner = getRequiredArgument(cmd, Arg.OWNER, newClause);
    verifyValidSchema(keySchema);
    verifyValidSchema(valueSchema);
    verifyConnection(routerHosts, clusterName);
    boolean storeExists = !ControllerClient.queryNextVersion(routerHosts, clusterName, store).isError();
    if (storeExists) {
      throw new VeniceException("Store " + store + " already exists, cannot create it");
    }
          /* TODO: createNewStore should be modified to require a key and value schema */
    NewStoreResponse newStore = ControllerClient.createNewStore(routerHosts, clusterName, store, owner);
    if (newStore.isError()) {
      throw new VeniceException("Error creating store " + store + ": " + newStore.getError());
    }
    SchemaResponse keyResponse = ControllerClient.initKeySchema(routerHosts, clusterName, store, keySchema);
    SchemaResponse valueResponse = ControllerClient.addValueSchema(routerHosts, clusterName, store, valueSchema);
    for (SchemaResponse response : Arrays.asList(keyResponse, valueResponse)) {
      if (response.isError()) {
        System.err.println("Error initializing store with schema: " + response.getError());
      }
    }
    System.out.println("Created Store: " + store);
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

  private static void printUsageAndExit(Options options){
    String command = "java -jar " + new java.io.File(AdminTool.class.getProtectionDomain()
        .getCodeSource()
        .getLocation()
        .getPath())
        .getName();
    new HelpFormatter().printHelp(command + " --router <router_uri> --cluster <cluster_name> (--query|--new) <arg> [options]\n\nOptions:",
        options);
    System.err.println(
            "\nExamples:\n"
            + "--query list (no other arguments required)\n"
            + "--query describe (optionally takes --store to only describe one store.  Otherwise describes all stores)\n"
            + "--query next, --query available, --query current (Requires --store)\n"
            + "--query job (Requires --store, --version)\n"
            + "--new store (Requires --store, --owner, --key-schema-file, --value-schema-file)"
    );

    System.exit(1);
  }

  private static String getRequiredArgument(CommandLine cmd, Arg arg){
    return getRequiredArgument(cmd, arg, "");
  }

  private static String getRequiredArgument(CommandLine cmd, Arg arg, String errorClause){
    if (!cmd.hasOption(arg.first())){
      printErrAndExit(arg.toString() + " is a required argument " + errorClause);
    }
    return cmd.getOptionValue(arg.first());
  }

  private static void verifyConnection(String routerHosts, String cluster){
    boolean canConnect = !ControllerClient.queryStoreList(routerHosts, cluster).isError();
    if (!canConnect){
      printErrAndExit("Cannot connect to cluster " + cluster + " with routers " + routerHosts);
    }
  }

  private static void verifyValidSchema(String schema){
    try {
      Schema.parse(schema);
    } catch (Exception e){
      printErrAndExit("Invalid Schema: " + schema);
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

  static String readFile(String path) throws IOException {
    String fullPath = path.replace("~", System.getProperty("user.home"));
    byte[] encoded = Files.readAllBytes(Paths.get(fullPath));
    return new String(encoded, StandardCharsets.UTF_8).trim();
  }

}
