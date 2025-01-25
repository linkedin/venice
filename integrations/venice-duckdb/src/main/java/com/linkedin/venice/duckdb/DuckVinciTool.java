package com.linkedin.venice.duckdb;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_TYPE;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEY_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_SECURE_RANDOM_IMPLEMENTATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_TYPE;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.KAFKA_SECURITY_PROTOCOL;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.LogConfigurator;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLConfig;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DuckVinciTool {
  private static final Logger LOGGER = LogManager.getLogger(DuckVinciTool.class);
  private static ObjectWriter jsonWriter = ObjectMapperFactory.getInstance().writerWithDefaultPrettyPrinter();
  private static final String ERROR = "error";
  private static Optional<SSLFactory> sslFactory = Optional.empty();

  public static void main(String[] args) throws Exception {
    CommandLine cmd = getCommandLine(args);
    try {
      Command foundCommand = ensureOnlyOneCommand(cmd);

      if (cmd.hasOption(Arg.DISABLE_LOG.toString())) {
        LogConfigurator.disableLog();
      }

      /**
       * Initialize SSL config if provided.
       */
      buildSslFactory(cmd);

      switch (foundCommand) {
        case GET:
          get(cmd);
        default:
          StringJoiner availableCommands = new StringJoiner(", ");
          for (Command c: Command.values()) {
            availableCommands.add("--" + c.toString());
          }
          throw new VeniceException("Must supply one of the following commands: " + availableCommands.toString());
      }
    } catch (Exception e) {
      printErrAndThrow(e, e.getMessage(), null);
    }
  }

  private static void get(CommandLine cmd) {
    String storeName = getRequiredArgument(cmd, Arg.STORE_NAME, Command.GET);
    String clusterDiscoveryD2ServiceName = getRequiredArgument(cmd, Arg.CLUSTER_DISCOVERY_D2_SERVICE_NAME, Command.GET);
    String zkHostUrl = getRequiredArgument(cmd, Arg.ZK_HOST_URL, Command.GET);

    String duckdbOutputDirectory = getOptionalArgument(cmd, Arg.DUCKDB_OUTPUT_DIRECTORY, "./");

    DaVinciConfig clientConfig = new DaVinciConfig();
    MetricsRepository metricsRepository = new MetricsRepository();

    Set<String> columnsToProject = Collections.emptySet();

    DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig(
        (storeVersion, keySchema, inputValueSchema, outputValueSchema) -> new DuckDBDaVinciRecordTransformer(
            storeVersion,
            keySchema,
            inputValueSchema,
            outputValueSchema,
            false,
            duckdbOutputDirectory,
            storeName,
            columnsToProject));
    clientConfig.setRecordTransformerConfig(recordTransformerConfig);

    D2ClientBuilder d2ClientBuilder = new D2ClientBuilder().setZkHosts(zkHostUrl)
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS);

    PropertyBuilder backendConfigBuilder =
        new PropertyBuilder().put(DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED, true);

    if (sslFactory.isPresent()) {
      d2ClientBuilder.setIsSSLEnabled(true)
          .setSSLContext(sslFactory.get().getSSLContext())
          .setSSLParameters(sslFactory.get().getSSLParameters());

      SSLConfig sslConfig = sslFactory.get().getSSLConfig();
      backendConfigBuilder.put(KAFKA_SECURITY_PROTOCOL, "SSL")
          .put(SSL_ENABLED, true)
          .put(SSL_KEYSTORE_LOCATION, sslConfig.getKeyStoreFilePath())
          .put(SSL_KEYSTORE_PASSWORD, sslConfig.getKeyStorePassword())
          .put(SSL_KEYSTORE_TYPE, sslConfig.getKeyStoreType())
          .put(SSL_KEY_PASSWORD, sslConfig.getKeyStorePassword())
          .put(SSL_TRUSTSTORE_LOCATION, sslConfig.getTrustStoreFilePath())
          .put(SSL_TRUSTSTORE_PASSWORD, sslConfig.getTrustStoreFilePassword())
          .put(SSL_TRUSTSTORE_TYPE, sslConfig.getTrustStoreType())
          .put(SSL_KEYMANAGER_ALGORITHM, "SunX509")
          .put(SSL_TRUSTMANAGER_ALGORITHM, "SunX509")
          .put(SSL_SECURE_RANDOM_IMPLEMENTATION, "SHA1PRNG");
    }

    D2Client d2Client = d2ClientBuilder.build();
    D2ClientUtils.startClient(d2Client);

    VeniceProperties backendConfig = backendConfigBuilder.build();

    try (CachingDaVinciClientFactory factory =
        new CachingDaVinciClientFactory(d2Client, clusterDiscoveryD2ServiceName, metricsRepository, backendConfig)) {
      DaVinciClient<Integer, Object> clientWithRecordTransformer =
          factory.getAndStartGenericAvroClient(storeName, clientConfig);

      LOGGER.info("Starting ingestion");
      // Data will get written to DucksDB
      clientWithRecordTransformer.subscribeAll().get();
      // ToDo: Make database file name configurable
      LOGGER.info("Finished ingestion. Data written to " + duckdbOutputDirectory + "my_database.duckdb");
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /* Things that are not commands */

  public static CommandLine getCommandLine(String[] args) throws ParseException {
    /**
     * Command Options are split up for help text formatting, see printUsageAndExit()
     *
     * Gather all the commands we have in "commandGroup"
     **/
    OptionGroup commandGroup = new OptionGroup();
    for (Command c: Command.values()) {
      createCommandOpt(c, commandGroup);
    }

    /**
     * Gather all the options we have in "options"
     */
    Options options = new Options();
    for (Arg arg: Arg.values()) {
      createOpt(arg, arg.isParameterized(), arg.getHelpText(), options);
    }

    Options parameterOptionsForHelp = new Options();
    for (Object obj: options.getOptions()) {
      Option o = (Option) obj;
      parameterOptionsForHelp.addOption(o);
    }

    options.addOptionGroup(commandGroup);

    CommandLineParser parser = new DefaultParser(false);
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption(Arg.HELP.first())) {
      printUsageAndExit(commandGroup, parameterOptionsForHelp);
    }

    // SSl config path is mandatory
    if (!cmd.hasOption(Arg.SSL_CONFIG_PATH.first())) {
      /**
       * Don't throw exception yet until all controllers are deployed with SSL support and the script
       * that automatically generates SSL config file is provided.
       */
      System.out.println("[WARN] Running duckvinci tool without SSL.");
    }
    return cmd;
  }

  private static String getRequiredArgument(CommandLine cmd, Arg arg, Command command) {
    return getRequiredArgument(cmd, arg, "when using --" + command.toString());
  }

  private static String getRequiredArgument(CommandLine cmd, Arg arg, String errorClause) {
    if (!cmd.hasOption(arg.first())) {
      printErrAndExit(arg.toString() + " is a required argument " + errorClause);
    }
    return cmd.getOptionValue(arg.first());
  }

  private static void printErrAndExit(String err) {
    Map<String, String> errMap = new HashMap<>();
    printErrAndExit(err, errMap);
  }

  private static void printErrAndExit(String errorMessage, Map<String, String> customMessages) {
    printErr(errorMessage, customMessages);
    Utils.exit("duck-vinci-tool encountered and error, exiting now.");
  }

  private static void printUsageAndExit(OptionGroup commandGroup, Options options) {

    /* Commands */
    String command = "java -jar "
        + new java.io.File(DuckVinciTool.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getName();

    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(140);
    helpFormatter
        .printHelp(command + " --<command> [parameters]\n\nCommands:", new Options().addOptionGroup(commandGroup));

    /* Parameters */
    helpFormatter.printHelp("Parameters: ", options);

    /* Examples */
    System.out.println("\nExamples:");
    Command[] commands = Command.values();
    Arrays.sort(commands, Command.commandComparator);
    for (Command c: commands) {
      StringJoiner exampleArgs = new StringJoiner(" ");
      for (Arg a: c.getRequiredArgs()) {
        exampleArgs.add("--" + a.toString());
        if (a.isParameterized()) {
          exampleArgs.add("<" + a + ">");
        }
      }
      for (Arg a: c.getOptionalArgs()) {
        exampleArgs.add("[--" + a.toString());
        String param = "";
        if (a.isParameterized()) {
          param += "<" + a + ">";
        }
        exampleArgs.add(param + "]");
      }

      System.out.println(command + " --" + c + " " + exampleArgs);
    }
    Utils.exit("printUsageAndExit");
  }

  private static void createOpt(Arg name, boolean hasArg, String help, Options options) {
    options.addOption(new Option(name.first(), name.toString(), hasArg, help));
  }

  private static void createCommandOpt(Command command, OptionGroup group) {
    group.addOption(OptionBuilder.withLongOpt(command.toString()).withDescription(command.getDesc()).create());
  }

  private static Command ensureOnlyOneCommand(CommandLine cmd) {
    String foundCommand = null;
    for (Command c: Command.values()) {
      if (cmd.hasOption(c.toString())) {
        if (foundCommand == null) {
          foundCommand = c.toString();
        } else {
          throw new VeniceException("Can only specify one of --" + foundCommand + " and --" + c);
        }
      }
    }
    return Command.getCommand(foundCommand, cmd);
  }

  private static String getOptionalArgument(CommandLine cmd, Arg arg) {
    return getOptionalArgument(cmd, arg, null);
  }

  private static String getOptionalArgument(CommandLine cmd, Arg arg, String defaultArgValue) {
    if (!cmd.hasOption(arg.first())) {
      return defaultArgValue;
    } else {
      return cmd.getOptionValue(arg.first());
    }
  }

  private static void buildSslFactory(CommandLine cmd) throws IOException {
    if (cmd.hasOption(Arg.SSL_CONFIG_PATH.first())) {
      String sslConfigPath = getOptionalArgument(cmd, Arg.SSL_CONFIG_PATH);
      Properties sslProperties = SslUtils.loadSSLConfig(sslConfigPath);
      String sslFactoryClassName = sslProperties.getProperty(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
      sslFactory = Optional.of(SslUtils.getSSLFactory(sslProperties, sslFactoryClassName));
    }
  }

  private static void printErrAndThrow(Exception e, String errorMessage, Map<String, String> customMessages)
      throws Exception {
    printErr(errorMessage, customMessages);
    throw e;
  }

  private static void printErr(String errorMessage, Map<String, String> customMessages) {
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
      System.out.println(jsonWriter.writeValueAsString(errMap));
    } catch (IOException e) {
      System.out.println("{\"" + ERROR + "\":\"" + e.getMessage() + "\"}");
    }
  }
}
