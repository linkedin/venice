package com.linkedin.venice;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.MultiVersionResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.meta.Version;
import java.util.Arrays;
import java.util.StringJoiner;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import static com.linkedin.venice.Arg.*;


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

  public static void main(String args[]) throws ParseException {

    Option routerOpt = createOpt(ROUTER, true, "REQUIRED: Venice router url, eg. http://localhost:54333");
    Option clusterOpt = createOpt(CLUSTER, true, "REQUIRED: Name of Venice cluster");
    Option storeOpt = createOpt(STORE, true, "Name of Venice store");
    Option versionOpt = createOpt(VERSION, true, "Venice store version number");
    Option queryOpt = createOpt(QUERY, true, "REQUIRED: Query one of next, available, current, job, list, describe");
    Options options = new Options()
        .addOption(routerOpt)
        .addOption(clusterOpt)
        .addOption(storeOpt)
        .addOption(versionOpt)
        .addOption(queryOpt)
        .addOption(createOpt(HELP, false, "Show usage"));

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption(HELP.first())){
      printUsageAndExit(options);
    }

    validateCommand(cmd, ROUTER);
    validateCommand(cmd, CLUSTER);
    String routerHosts = cmd.getOptionValue(ROUTER.first());
    String clusterName = cmd.getOptionValue(CLUSTER.first());

    if (cmd.hasOption(QUERY.first())){
      String queryAction = cmd.getOptionValue(QUERY.first());
      String queryClause = " using --" + QUERY.toString();
      String storeName;
      switch (queryAction.toLowerCase()){
        case NEXT:
          validateCommand(cmd, STORE, queryClause);
          storeName = cmd.getOptionValue(STORE.first());
          VersionResponse nextResponse = ControllerClient.queryNextVersion(routerHosts, clusterName, storeName);
          System.out.println(nextResponse.getVersion());
          break;
        case JOB:
          validateCommand(cmd, STORE, queryClause);
          storeName = cmd.getOptionValue(STORE.first());
          validateCommand(cmd, VERSION, "querying " + JOB + " status");
          int version = Integer.parseInt(cmd.getOptionValue(VERSION.first()));
          String topicName = new Version(storeName, version).kafkaTopicName();
          JobStatusQueryResponse jobStatus = ControllerClient.queryJobStatus(routerHosts, clusterName, topicName);
          System.out.println(jobStatus.getStatus());
          break;
        case CURRENT:
          validateCommand(cmd, STORE, queryClause);
          storeName = cmd.getOptionValue(STORE.first());
          VersionResponse currentResponse = ControllerClient.queryCurrentVersion(routerHosts, clusterName, storeName);
          System.out.println(currentResponse.getVersion());
          break;
        case AVAILABLE:
          validateCommand(cmd, STORE, queryClause);
          storeName = cmd.getOptionValue(STORE.first());
          MultiVersionResponse availableResponse = ControllerClient.queryActiveVersions(routerHosts, clusterName, storeName);
          System.out.println(Arrays.toString(availableResponse.getVersions()));
          break;
        case DESCRIBE:
          if (cmd.hasOption(STORE.first())) {
            storeName = cmd.getOptionValue(STORE.first());
            for (String store : storeName.split(",")) {
              printStoreDescription(routerHosts, clusterName, store);
            }
          } else { /* describe all stores */
            MultiStoreResponse storeResponse = ControllerClient.queryStoreList(routerHosts, clusterName);
            for (String store : storeResponse.getStores()){
              printStoreDescription(routerHosts, clusterName, store);
            }
          }
          break;
        case LIST:
          MultiStoreResponse storeResponse = ControllerClient.queryStoreList(routerHosts, clusterName);
          System.out.println(String.join("\n", storeResponse.getStores()));
          break;
        default:
          System.err.println(queryAction + " NOT IMPLEMENTED");
          System.exit(1);
      }
    } else { /* not --query */
      validateCommand(cmd, QUERY); /* because right now we can only query */
    }
  }

  private static void printStoreDescription(String routerHosts, String clusterName, String storeName){
    VersionResponse currentResponse = ControllerClient.queryCurrentVersion(routerHosts, clusterName, storeName);
    MultiVersionResponse availableResponse = ControllerClient.queryActiveVersions(routerHosts, clusterName, storeName);
    VersionResponse nextResponse = ControllerClient.queryNextVersion(routerHosts, clusterName, storeName);
    StringBuilder output = new StringBuilder()
        .append("Store: " + storeName + "\t")
        .append("Available versions: " + Arrays.toString(availableResponse.getVersions()) + "\t")
        .append("Current version: " + currentResponse.getVersion() + "\t")
        .append("Next version: " + nextResponse.getVersion());
    System.out.println(output.toString());
  }

  private static void printUsageAndExit(Options options){
    String command = "java -jar " + new java.io.File(AdminTool.class.getProtectionDomain()
        .getCodeSource()
        .getLocation()
        .getPath())
        .getName();
    new HelpFormatter().printHelp(command + " [options]", options);
    System.exit(1);
  }

  private static void validateCommand(CommandLine cmd, Arg arg){
    if (!cmd.hasOption(arg.first())){
      printErrAndExit(arg.toString() + " is a required argument");
    }
  }
  private static void validateCommand(CommandLine cmd, Arg arg, String clause){
    if (!cmd.hasOption(arg.first())){
      printErrAndExit(arg.toString() + " is a required argument when " + clause);
    }
  }

  private static void printErrAndExit(String err){
    System.err.println(err);
    System.err.println("--help for usage");
    System.exit(1);
  }

  private static Option createOpt(Arg name, boolean hasArg, String help){
    return new Option(name.first(), name.toString(), hasArg, help);
  }

}
