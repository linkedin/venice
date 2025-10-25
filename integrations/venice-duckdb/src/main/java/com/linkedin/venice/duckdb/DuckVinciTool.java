package com.linkedin.venice.duckdb;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
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
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUBSUB_SECURITY_PROTOCOL;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerFunctionalInterface;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.LogConfigurator;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLConfig;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.CliUtils;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
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
      sslFactory = CliUtils.buildSslFactory(cmd, Arg.SSL_CONFIG_PATH.toString());

      // Handle flat JSON output
      if (cmd.hasOption(Arg.FLAT_JSON.toString())) {
        jsonWriter = ObjectMapperFactory.getInstance().writer();
      }

      switch (foundCommand) {
        case INGEST:
          ingest(cmd);
        default:
          StringJoiner availableCommands = new StringJoiner(", ");
          for (Command c: Command.values()) {
            availableCommands.add("--" + c.toString());
          }
          throw new VeniceException("Must supply one of the following commands: " + availableCommands.toString());
      }
    } catch (Exception e) {
      CliUtils.printErrAndThrow(e, e.getMessage(), null);
    }
  }

  private static void ingest(CommandLine cmd) {
    String storeName = CliUtils.getRequiredArgument(cmd, Arg.STORE_NAME.toString(), "when using --" + Command.INGEST);
    String zkHostUrl = CliUtils.getRequiredArgument(cmd, Arg.ZK_HOST_URL.toString(), "when using --" + Command.INGEST);

    String duckdbOutputDirectory = CliUtils.getOptionalArgument(cmd, Arg.DUCKDB_OUTPUT_DIRECTORY.toString(), "./");
    String columnsToProjectStr = CliUtils.getOptionalArgument(cmd, Arg.COLUMNS_TO_PROJECT.toString());

    DaVinciConfig clientConfig = new DaVinciConfig();
    MetricsRepository metricsRepository = new MetricsRepository();

    final Set<String> columnsToProject;
    if (columnsToProjectStr != null && !columnsToProjectStr.trim().isEmpty()) {
      Set<String> tempColumnsToProject = new HashSet<>(Arrays.asList(columnsToProjectStr.split(",")));
      // Trim whitespace from column names
      columnsToProject = tempColumnsToProject.stream().map(String::trim).collect(java.util.stream.Collectors.toSet());
    } else {
      columnsToProject = Collections.emptySet();
    }

    DaVinciRecordTransformerFunctionalInterface recordTransformerFunction = (
        storeNameParam,
        storeVersion,
        keySchema,
        inputValueSchema,
        outputValueSchema,
        config) -> new DuckDBDaVinciRecordTransformer(
            storeNameParam,
            storeVersion,
            keySchema,
            inputValueSchema,
            outputValueSchema,
            config,
            duckdbOutputDirectory,
            columnsToProject);

    DaVinciRecordTransformerConfig recordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(recordTransformerFunction)
            .setStoreRecordsInDaVinci(false)
            .build();
    clientConfig.setRecordTransformerConfig(recordTransformerConfig);

    D2ClientBuilder d2ClientBuilder = new D2ClientBuilder().setZkHosts(zkHostUrl)
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS);

    PropertyBuilder backendConfigBuilder =
        new PropertyBuilder().put(DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED, true)
            .put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
            .put(DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY, false)
            .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 0)
            .put(PERSISTENCE_TYPE, ROCKS_DB);

    if (sslFactory.isPresent()) {
      d2ClientBuilder.setIsSSLEnabled(true)
          .setSSLContext(sslFactory.get().getSSLContext())
          .setSSLParameters(sslFactory.get().getSSLParameters());

      SSLConfig sslConfig = sslFactory.get().getSSLConfig();
      backendConfigBuilder.put(PUBSUB_SECURITY_PROTOCOL, "SSL")
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

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2Client,
        ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig)) {
      DaVinciClient<Integer, Object> clientWithRecordTransformer =
          factory.getAndStartGenericAvroClient(storeName, clientConfig);

      LOGGER.info("Starting ingestion");
      // Data will get written to DuckDB
      clientWithRecordTransformer.subscribeAll().get();
      LOGGER.info("Finished ingestion. Data written to directory:" + duckdbOutputDirectory);
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
      CliUtils.createCommandOpt(c.toString(), c.getDesc(), commandGroup);
    }

    /**
     * Gather all the options we have in "options"
     */
    Options options = new Options();
    for (Arg arg: Arg.values()) {
      CliUtils.createOpt(arg.first(), arg.toString(), arg.isParameterized(), arg.getHelpText(), options);
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
      CliUtils.printUsageAndExit("duckvinci-tool", commandGroup, parameterOptionsForHelp);
    }

    // SSL config path validation with improved warning message
    CliUtils.validateSslConfiguration(cmd, Arg.SSL_CONFIG_PATH.toString(), "duckvinci tool");
    return cmd;
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
}
