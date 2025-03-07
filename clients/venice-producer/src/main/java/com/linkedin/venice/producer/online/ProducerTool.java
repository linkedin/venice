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
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
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
      cmd = new DefaultParser(false).parse(CLI_OPTIONS, args);
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
      if (producerContext.value.equals("null")) { // Only allow `null`. Not "null", or 'null', or whatever.
        producer.asyncDelete(key).get();
        System.out.println("Record deleted from Venice!");
      } else {
        Object value = getValueObject(producerContext.value, schemaFetcher);
        producer.asyncPut(key, value).get();
        System.out.println("Data written to Venice!");
      }
    } catch (Exception e) {
      System.err.println(ExceptionUtils.stackTraceToString(e));
      System.exit(1);
    }
  }

  private static Object getValueObject(String valueString, RouterBasedStoreSchemaFetcher schemaFetcher) {
    Object value = null;
    Map<Integer, Exception> exceptionMap = new HashMap<>();
    List<SchemaEntry> reversedSchemaEntryList = schemaFetcher.getAllValueSchemasWithId()
        .entrySet()
        .stream()
        .map(entry -> new SchemaEntry(entry.getKey(), entry.getValue()))
        .sorted(Comparator.comparingInt(SchemaEntry::getId).reversed())
        .collect(Collectors.toList());
    for (SchemaEntry valueSchemaEntry: reversedSchemaEntryList) {
      try {
        value = adaptDataToSchema(valueString, valueSchemaEntry.getSchema());
        break;
      } catch (Exception e) {
        exceptionMap.put(valueSchemaEntry.getId(), e);
        // Try the next schema
      }
    }
    if (value == null) {
      String exceptionDelimiter = "\n\t";
      String exceptionDetails = exceptionMap.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(entry -> {
        Exception e = entry.getValue();
        return "Schema Id: " + entry.getKey() + ". Exception: " + e.getClass().getName() + ": " + e.getMessage();
      }).collect(Collectors.joining(exceptionDelimiter));
      System.out.println(
          "[ERROR] Value schema not found. Is a schema that is compatible with the specified data already registered?\nException messages for each schema:"
              + exceptionDelimiter + exceptionDetails);
      System.exit(1);
    }
    return value;
  }

  private static Object adaptDataToSchema(String dataString, Schema dataSchema) {
    Object data;
    switch (dataSchema.getType()) {
      case INT:
        data = Integer.parseInt(dataString);
        break;
      case LONG:
        data = Long.parseLong(dataString);
        break;
      case FLOAT:
        data = Float.parseFloat(dataString);
        break;
      case DOUBLE:
        data = Double.parseDouble(dataString);
        break;
      case BOOLEAN:
        data = Boolean.parseBoolean(dataString);
        break;
      case STRING:
        data = dataString;
        break;
      default:
        try {
          data = new GenericDatumReader<>(dataSchema, dataSchema).read(
              null,
              AvroCompatibilityHelper.newJsonDecoder(dataSchema, new ByteArrayInputStream(dataString.getBytes())));
        } catch (IOException e) {
          throw new VeniceException("Invalid input:" + dataString, e);
        }
        break;
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
