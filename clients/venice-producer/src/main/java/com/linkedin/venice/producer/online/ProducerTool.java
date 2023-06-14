package com.linkedin.venice.producer.online;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.LogConfigurator;
import com.linkedin.venice.client.schema.RouterBasedStoreSchemaFetcher;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.StatTrackingStoreClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;


public class ProducerTool {
  private static final Option STORE_OPTION =
      Option.builder().option("s").longOpt("store").hasArg().required().desc("Store name").build();
  private static final Option KEY_OPTION = Option.builder()
      .option("k")
      .longOpt("key")
      .hasArg()
      .required()
      .desc("Key of the record. Complex types must be specified as JSON")
      .build();
  private static final Option VALUE_OPTION = Option.builder()
      .option("v")
      .longOpt("value")
      .hasArg()
      .required()
      .desc("Value of the record. Complex types must be specified as JSON")
      .build();
  private static final Option VENICE_URL_OPTION = Option.builder()
      .option("vu")
      .longOpt("veniceUrl")
      .hasArg()
      .required()
      .desc("Router URL with http or https scheme or ZK address if using D2")
      .build();
  private static final Option D2_SERVICE_NAME_OPTION = Option.builder()
      .option("dsn")
      .longOpt("d2ServiceName")
      .hasArg()
      .desc("D2 service name for cluster discovery. default: " + ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME)
      .build();
  private static final Option CONFIG_PATH_OPTION =
      Option.builder().option("cp").longOpt("configPath").hasArg().desc("Path to config file").build();
  private static final Option HELP_OPTION =
      Option.builder().option("h").longOpt("help").desc("Print this help message").build();

  private static final Options CLI_OPTIONS = new Options().addOption(STORE_OPTION)
      .addOption(KEY_OPTION)
      .addOption(VALUE_OPTION)
      .addOption(VENICE_URL_OPTION)
      .addOption(D2_SERVICE_NAME_OPTION)
      .addOption(CONFIG_PATH_OPTION)
      .addOption(HELP_OPTION);

  public static void main(String[] args) throws Exception {
    LogConfigurator.disableLog();

    if (checkForHelp(args)) {
      printHelp();
      return;
    }

    CommandLine cmd = null;
    try {
      cmd = new DefaultParser().parse(CLI_OPTIONS, args);
    } catch (ParseException e) {
      System.out.println("[ERROR] " + e.getMessage());
      printHelp();
      System.exit(1);
    }

    ProducerContext producerContext = validateOptionsAndExtractContext(cmd);
    writeToStore(producerContext);
  }

  // Check if the CLI args specify either of -h or --help options
  private static boolean checkForHelp(String[] args) {
    for (String arg: args) {
      if (arg.equals("-" + HELP_OPTION.getOpt())) {
        return true;
      }

      if (arg.equals("--" + HELP_OPTION.getLongOpt())) {
        return true;
      }
    }
    return false;
  }

  private static void printHelp() {
    String header = "Write a single key-value pair to Venice\n\n";
    String footer = "\nPlease report issues to Venice team or at https://github.com/linkedin/venice/issues";

    HelpFormatter fmt = new HelpFormatter();
    fmt.printHelp("venice-producer", header, CLI_OPTIONS, footer, true);
  }

  private static ProducerContext validateOptionsAndExtractContext(CommandLine cmd) throws IOException {
    ProducerContext producerContext = new ProducerContext();
    producerContext.store = validateRequiredOption(cmd, STORE_OPTION);
    producerContext.key = validateRequiredOption(cmd, KEY_OPTION);
    producerContext.value = validateRequiredOption(cmd, VALUE_OPTION);
    String veniceUrl = validateRequiredOption(cmd, VENICE_URL_OPTION);

    String configFile = cmd.getOptionValue(CONFIG_PATH_OPTION);
    if (!StringUtils.isEmpty(configFile)) {
      producerContext.configProperties = SslUtils.loadSSLConfig(configFile);
      String sslFactoryClassName =
          producerContext.configProperties.getProperty(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
      producerContext.sslFactory = SslUtils.getSSLFactory(producerContext.configProperties, sslFactoryClassName);
    } else {
      producerContext.configProperties = new Properties();
    }

    // Verify the ssl engine is set up correctly.
    producerContext.veniceUrl = veniceUrl.toLowerCase().trim();
    if (producerContext.veniceUrl.startsWith("https")
        && (producerContext.sslFactory == null || producerContext.sslFactory.getSSLContext() == null)) {
      throw new VeniceException(
          "ERROR: The SSL configuration is not valid to send a request to " + producerContext.veniceUrl);
    }

    if (!producerContext.veniceUrl.startsWith("http")) {
      producerContext.d2ServiceName =
          cmd.getOptionValue(D2_SERVICE_NAME_OPTION, ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME);
    }
    return producerContext;
  }

  private static String validateRequiredOption(CommandLine cmd, Option option) {
    String optionValue = cmd.getOptionValue(option.getOpt());
    if (StringUtils.isEmpty(optionValue)) {
      System.err.println("Option is mandatory: " + option);
      printHelp();
      System.exit(1);
    }

    return optionValue;
  }

  private static void writeToStore(ProducerContext producerContext) throws Exception {
    ClientConfig clientConfig = new ClientConfig(producerContext.store).setVeniceURL(producerContext.veniceUrl);

    if (!StringUtils.isEmpty(producerContext.d2ServiceName)) {
      clientConfig.setD2ServiceName(producerContext.d2ServiceName);
    }

    if (producerContext.sslFactory != null) {
      clientConfig.setSslFactory(producerContext.sslFactory);
    }

    try (
        OnlineVeniceProducer producer = OnlineProducerFactory
            .createProducer(clientConfig, new VeniceProperties(producerContext.configProperties), null);
        AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(clientConfig)) {
      AbstractAvroStoreClient<Object, Object> castClient =
          (AbstractAvroStoreClient<Object, Object>) ((StatTrackingStoreClient<Object, Object>) client)
              .getInnerStoreClient();
      RouterBasedStoreSchemaFetcher schemaFetcher = new RouterBasedStoreSchemaFetcher(castClient);
      Schema keySchema = schemaFetcher.getKeySchema();
      Object key = adaptDataToSchema(producerContext.key, keySchema);
      Object value = getValueObject(producerContext.value, schemaFetcher);

      producer.asyncPut(key, value).get();
      System.out.println("Data written to Venice!");
    }
  }

  private static Object getValueObject(String valueString, RouterBasedStoreSchemaFetcher schemaFetcher) {
    Object value = null;
    for (Schema valueSchema: schemaFetcher.getAllValueSchemas()) {
      try {
        value = adaptDataToSchema(valueString, valueSchema);
        break;
      } catch (Exception e) {
        // Nothing to do. Try the next schema
      }
    }
    if (value == null) {
      throw new VeniceException(
          "Value schema not found. Is a schema that is compatible with the specified data already registered?");
    }
    return value;
  }

  private static Object adaptDataToSchema(String dataString, Schema dataSchema) {
    while (dataSchema.getType().equals(Schema.Type.UNION)) {
      dataSchema = VsonAvroSchemaAdapter.stripFromUnion(dataSchema);
    }

    Object data;
    switch (dataSchema.getType()) {
      case INT:
        data = Integer.parseInt(dataString);
        break;
      case DOUBLE:
        data = Double.parseDouble(dataString);
        break;
      case LONG:
        data = Long.parseLong(dataString);
        break;
      case STRING:
        data = dataString;
        break;
      case RECORD:
        try {
          data = new GenericDatumReader<>(dataSchema, dataSchema).read(
              null,
              AvroCompatibilityHelper.newJsonDecoder(dataSchema, new ByteArrayInputStream(dataString.getBytes())));
        } catch (IOException e) {
          throw new VeniceException("Invalid input:" + dataString, e);
        }
        break;
      default:
        throw new VeniceException("Cannot handle type, found schema: " + dataSchema);
    }
    return data;
  }

  private static class ProducerContext {
    private String store;
    private String key;
    private String value;
    private Properties configProperties;
    private SSLFactory sslFactory;
    private String veniceUrl;
    private String d2ServiceName;
  }
}
