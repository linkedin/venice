package com.linkedin.venice.producer.online;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.linkedin.venice.writer.update.UpdateBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
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
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Option STORE_OPTION =
      Option.builder().option("s").longOpt("store").hasArg().required().desc("Store name").build();
  private static final Option KEY_OPTION = Option.builder()
      .option("k")
      .longOpt("key")
      .hasArg()
      .desc("Key of the record. Complex types must be specified as JSON. Required unless --file is used")
      .build();
  private static final Option VALUE_OPTION = Option.builder()
      .option("v")
      .longOpt("value")
      .hasArg()
      .desc("Value of the record. Complex types must be specified as JSON. Required unless --file is used")
      .build();
  private static final Option FILE_OPTION = Option.builder()
      .option("f")
      .longOpt("file")
      .hasArg()
      .desc(
          "Path to a JSON Lines file where each line is a JSON object with 'key', 'value', "
              + "and optional 'operation' (put/update/delete, default: put). "
              + "Mutually exclusive with -k, -v, --delete, and --update")
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
  private static final Option UPDATE_OPTION = Option.builder()
      .option("u")
      .longOpt("update")
      .desc(
          "Perform a partial update instead of a full put. "
              + "The value must be a JSON object where keys are field names to update")
      .build();
  private static final Option DELETE_OPTION = Option.builder()
      .option("d")
      .longOpt("delete")
      .desc("Delete the record for the given key. Only requires -k, no -v needed")
      .build();
  private static final Option HELP_OPTION =
      Option.builder().option("h").longOpt("help").desc("Print this help message").build();

  private static final Options CLI_OPTIONS = new Options().addOption(STORE_OPTION)
      .addOption(KEY_OPTION)
      .addOption(VALUE_OPTION)
      .addOption(FILE_OPTION)
      .addOption(VENICE_URL_OPTION)
      .addOption(D2_SERVICE_NAME_OPTION)
      .addOption(CONFIG_PATH_OPTION)
      .addOption(UPDATE_OPTION)
      .addOption(DELETE_OPTION)
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
    String header = "Write key-value pairs to Venice. Supports single record (with -k and -v), partial update (-u), "
        + "or batch mode from a JSON Lines file (-f)\n\n";
    String footer = "\nPlease report issues to Venice team or at https://github.com/linkedin/venice/issues";

    HelpFormatter fmt = new HelpFormatter();
    fmt.printHelp("venice-producer", header, CLI_OPTIONS, footer, true);
  }

  private static ProducerContext validateOptionsAndExtractContext(CommandLine cmd) throws IOException {
    ProducerContext producerContext = new ProducerContext();
    producerContext.store = validateRequiredOption(cmd, STORE_OPTION);

    String filePath = cmd.getOptionValue(FILE_OPTION.getOpt());
    boolean hasFile = !StringUtils.isEmpty(filePath);
    boolean hasKey = !StringUtils.isEmpty(cmd.getOptionValue(KEY_OPTION.getOpt()));
    boolean hasValue = !StringUtils.isEmpty(cmd.getOptionValue(VALUE_OPTION.getOpt()));
    boolean isDelete = cmd.hasOption(DELETE_OPTION.getOpt());

    boolean isUpdate = cmd.hasOption(UPDATE_OPTION.getOpt());

    if (hasFile && (hasKey || hasValue || isDelete || isUpdate)) {
      System.err.println("[ERROR] --file cannot be used together with --key, --value, --delete, or --update");
      printHelp();
      System.exit(1);
    }
    if (isDelete && hasValue) {
      System.err.println("[ERROR] --delete cannot be used together with --value");
      printHelp();
      System.exit(1);
    }
    if (isDelete && isUpdate) {
      System.err.println("[ERROR] --delete cannot be used together with --update");
      printHelp();
      System.exit(1);
    }
    if (!hasFile && !hasKey) {
      System.err.println("[ERROR] --key is required unless --file is used");
      printHelp();
      System.exit(1);
    }
    if (!hasFile && !isDelete && !hasValue) {
      System.err.println("[ERROR] --value is required unless --delete or --file is used");
      printHelp();
      System.exit(1);
    }

    if (hasFile) {
      producerContext.filePath = filePath;
    } else {
      producerContext.key = cmd.getOptionValue(KEY_OPTION.getOpt());
      producerContext.isDelete = isDelete;
      if (!isDelete) {
        producerContext.value = cmd.getOptionValue(VALUE_OPTION.getOpt());
        producerContext.isUpdate = isUpdate;
      }
    }

    String veniceUrl = validateRequiredOption(cmd, VENICE_URL_OPTION);

    String configFile = cmd.getOptionValue(CONFIG_PATH_OPTION.getOpt());
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
          cmd.getOptionValue(D2_SERVICE_NAME_OPTION.getOpt(), ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME);
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

      if (producerContext.filePath != null) {
        writeFromFile(producerContext.filePath, producer, schemaFetcher);
      } else {
        Schema keySchema = schemaFetcher.getKeySchema();
        Object key = adaptDataToSchema(producerContext.key, keySchema);
        if (producerContext.isDelete) {
          producer.asyncDelete(key).get();
          System.out.println("Record deleted from Venice!");
        } else if (producerContext.value.equals("null")) { // Only allow `null`. Not "null", or 'null', or whatever.
          producer.asyncDelete(key).get();
          System.out.println("Record deleted from Venice!");
        } else if (producerContext.isUpdate) {
          java.util.function.Consumer<UpdateBuilder> updateFunction =
              buildUpdateFunction(producerContext.value, schemaFetcher);
          producer.asyncUpdate(key, updateFunction).get();
          System.out.println("Partial update written to Venice!");
        } else {
          Object value = getValueObject(producerContext.value, schemaFetcher);
          producer.asyncPut(key, value).get();
          System.out.println("Data written to Venice!");
        }
      }
    } catch (Exception e) {
      System.err.println(ExceptionUtils.stackTraceToString(e));
      System.exit(1);
    }
  }

  private static void writeFromFile(
      String filePath,
      OnlineVeniceProducer producer,
      RouterBasedStoreSchemaFetcher schemaFetcher) throws Exception {
    // Cache schemas up front to avoid hitting the router on every record
    Schema keySchema = schemaFetcher.getKeySchema();
    List<SchemaEntry> cachedValueSchemas = schemaFetcher.getAllValueSchemasWithId()
        .entrySet()
        .stream()
        .map(entry -> new SchemaEntry(entry.getKey(), entry.getValue()))
        .sorted(Comparator.comparingInt(SchemaEntry::getId).reversed())
        .collect(Collectors.toList());
    Schema latestValueSchema = cachedValueSchemas.stream()
        .max(Comparator.comparingInt(SchemaEntry::getId))
        .map(SchemaEntry::getSchema)
        .orElseThrow(() -> new VeniceException("No value schema found for the store"));

    int successCount = 0;

    try (java.io.BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8)) {
      String line;
      int lineNumber = 0;
      while ((line = reader.readLine()) != null) {
        lineNumber++;
        line = line.trim();
        if (line.isEmpty()) {
          continue;
        }

        JsonNode record;
        try {
          record = OBJECT_MAPPER.readTree(line);
        } catch (IOException e) {
          throw new VeniceException("Line " + lineNumber + ": Failed to parse JSON: " + e.getMessage(), e);
        }

        if (!record.has("key")) {
          throw new VeniceException("Line " + lineNumber + ": Missing required field 'key'");
        }
        if (!record.has("value")) {
          throw new VeniceException("Line " + lineNumber + ": Missing required field 'value'");
        }

        String keyStr = record.get("key").isTextual() ? record.get("key").asText() : record.get("key").toString();
        Object key = adaptDataToSchema(keyStr, keySchema);

        String operation = "put";
        if (record.has("operation")) {
          operation = record.get("operation").asText().toLowerCase(Locale.ROOT);
        }

        JsonNode valueNode = record.get("value");
        String valueStr = valueNode.isTextual() ? valueNode.asText() : valueNode.toString();

        switch (operation) {
          case "delete":
            producer.asyncDelete(key).get();
            break;
          case "update":
            java.util.function.Consumer<UpdateBuilder> updateFunction =
                buildUpdateFunctionWithSchema(valueStr, latestValueSchema);
            producer.asyncUpdate(key, updateFunction).get();
            break;
          case "put":
            Object value = getValueObjectFromCachedSchemas(valueStr, cachedValueSchemas);
            producer.asyncPut(key, value).get();
            break;
          default:
            throw new VeniceException(
                "Line " + lineNumber + ": Unknown operation '" + operation + "'. Must be one of: put, update, delete");
        }
        successCount++;
      }
    } catch (Exception e) {
      if (successCount > 0) {
        System.out.println(successCount + " record(s) written successfully before failure.");
      }
      throw e;
    }
    System.out.println("All " + successCount + " record(s) written to Venice successfully!");
  }

  private static final String OP_ADD_TO_LIST = "$addToList";
  private static final String OP_REMOVE_FROM_LIST = "$removeFromList";
  private static final String OP_ADD_TO_MAP = "$addToMap";
  private static final String OP_REMOVE_FROM_MAP = "$removeFromMap";

  /**
   * Parses the value string as a JSON object and builds an update function that applies the
   * specified operations to the UpdateBuilder. Each field in the JSON can be either:
   * <ul>
   *   <li>A plain value — replaces the field via {@code setNewFieldValue}</li>
   *   <li>An object with operator keys:
   *     <ul>
   *       <li>{@code $addToList: [...]} — appends elements to a list field</li>
   *       <li>{@code $removeFromList: [...]} — removes elements from a list field</li>
   *       <li>{@code $addToMap: {...}} — adds entries to a map field</li>
   *       <li>{@code $removeFromMap: [...]} — removes entries by key from a map field</li>
   *     </ul>
   *   </li>
   * </ul>
   */
  private static java.util.function.Consumer<UpdateBuilder> buildUpdateFunction(
      String valueString,
      RouterBasedStoreSchemaFetcher schemaFetcher) {
    Schema latestValueSchema = schemaFetcher.getAllValueSchemasWithId()
        .entrySet()
        .stream()
        .max(Comparator.comparingInt(Map.Entry::getKey))
        .map(entry -> new SchemaEntry(entry.getKey(), entry.getValue()).getSchema())
        .orElseThrow(() -> new VeniceException("No value schema found for the store"));
    return buildUpdateFunctionWithSchema(valueString, latestValueSchema);
  }

  private static java.util.function.Consumer<UpdateBuilder> buildUpdateFunctionWithSchema(
      String valueString,
      Schema latestValueSchema) {
    JsonNode rootNode;
    try {
      rootNode = OBJECT_MAPPER.readTree(valueString);
    } catch (IOException e) {
      throw new VeniceException("Failed to parse update value as JSON: " + valueString, e);
    }
    if (!rootNode.isObject()) {
      throw new VeniceException("Update value must be a JSON object, got: " + rootNode.getNodeType());
    }
    if (latestValueSchema.getType() != Schema.Type.RECORD) {
      throw new VeniceException(
          "Partial update requires a RECORD value schema, but the store's value schema is: "
              + latestValueSchema.getType());
    }

    // Collect all operations to apply
    List<java.util.function.Consumer<UpdateBuilder>> operations = new java.util.ArrayList<>();

    Iterator<Map.Entry<String, JsonNode>> fields = rootNode.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      String fieldName = field.getKey();
      JsonNode fieldValue = field.getValue();

      Schema.Field schemaField = latestValueSchema.getField(fieldName);
      if (schemaField == null) {
        throw new VeniceException(
            "Field '" + fieldName + "' does not exist in the value schema. " + "Available fields: "
                + latestValueSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.joining(", ")));
      }
      Schema fieldSchema = unwrapUnion(schemaField.schema());

      if (fieldValue.isObject() && hasOperatorKeys(fieldValue)) {
        // Collection operations
        parseCollectionOperations(fieldName, fieldValue, fieldSchema, operations);
      } else {
        // Plain value replacement
        String fieldValueStr = fieldValue.isTextual() ? fieldValue.asText() : fieldValue.toString();
        Object adaptedValue = adaptDataToSchema(fieldValueStr, fieldSchema);
        operations.add(builder -> builder.setNewFieldValue(fieldName, adaptedValue));
      }
    }

    if (operations.isEmpty()) {
      throw new VeniceException("No update operations specified in the value JSON");
    }

    return builder -> operations.forEach(op -> op.accept(builder));
  }

  private static Schema unwrapUnion(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      for (Schema unionBranch: schema.getTypes()) {
        if (unionBranch.getType() != Schema.Type.NULL) {
          return unionBranch;
        }
      }
    }
    return schema;
  }

  private static boolean hasOperatorKeys(JsonNode node) {
    Iterator<String> fieldNames = node.fieldNames();
    while (fieldNames.hasNext()) {
      String name = fieldNames.next();
      if (name.equals(OP_ADD_TO_LIST) || name.equals(OP_REMOVE_FROM_LIST) || name.equals(OP_ADD_TO_MAP)
          || name.equals(OP_REMOVE_FROM_MAP)) {
        return true;
      }
    }
    return false;
  }

  private static void parseCollectionOperations(
      String fieldName,
      JsonNode operatorNode,
      Schema fieldSchema,
      List<java.util.function.Consumer<UpdateBuilder>> operations) {
    Iterator<Map.Entry<String, JsonNode>> ops = operatorNode.fields();
    while (ops.hasNext()) {
      Map.Entry<String, JsonNode> op = ops.next();
      String operator = op.getKey();
      JsonNode operand = op.getValue();
      switch (operator) {
        case OP_ADD_TO_LIST: {
          if (fieldSchema.getType() != Schema.Type.ARRAY) {
            throw new VeniceException(
                OP_ADD_TO_LIST + " requires an ARRAY field, but field '" + fieldName + "' is " + fieldSchema.getType());
          }
          if (!operand.isArray()) {
            throw new VeniceException(OP_ADD_TO_LIST + " for field '" + fieldName + "' must be an array");
          }
          Schema elementSchema = unwrapUnion(fieldSchema.getElementType());
          List<Object> elements = parseJsonArray(operand, elementSchema);
          operations.add(builder -> builder.setElementsToAddToListField(fieldName, elements));
          break;
        }
        case OP_REMOVE_FROM_LIST: {
          if (fieldSchema.getType() != Schema.Type.ARRAY) {
            throw new VeniceException(
                OP_REMOVE_FROM_LIST + " requires an ARRAY field, but field '" + fieldName + "' is "
                    + fieldSchema.getType());
          }
          if (!operand.isArray()) {
            throw new VeniceException(OP_REMOVE_FROM_LIST + " for field '" + fieldName + "' must be an array");
          }
          Schema elementSchema = unwrapUnion(fieldSchema.getElementType());
          List<Object> elements = parseJsonArray(operand, elementSchema);
          operations.add(builder -> builder.setElementsToRemoveFromListField(fieldName, elements));
          break;
        }
        case OP_ADD_TO_MAP: {
          if (fieldSchema.getType() != Schema.Type.MAP) {
            throw new VeniceException(
                OP_ADD_TO_MAP + " requires a MAP field, but field '" + fieldName + "' is " + fieldSchema.getType());
          }
          if (!operand.isObject()) {
            throw new VeniceException(OP_ADD_TO_MAP + " for field '" + fieldName + "' must be an object");
          }
          Schema valueSchema = unwrapUnion(fieldSchema.getValueType());
          Map<String, Object> entries = parseJsonMap(operand, valueSchema);
          operations.add(builder -> builder.setEntriesToAddToMapField(fieldName, entries));
          break;
        }
        case OP_REMOVE_FROM_MAP: {
          if (fieldSchema.getType() != Schema.Type.MAP) {
            throw new VeniceException(
                OP_REMOVE_FROM_MAP + " requires a MAP field, but field '" + fieldName + "' is "
                    + fieldSchema.getType());
          }
          if (!operand.isArray()) {
            throw new VeniceException(OP_REMOVE_FROM_MAP + " for field '" + fieldName + "' must be an array of keys");
          }
          List<String> keys = new java.util.ArrayList<>();
          for (JsonNode keyNode: operand) {
            keys.add(keyNode.asText());
          }
          operations.add(builder -> builder.setKeysToRemoveFromMapField(fieldName, keys));
          break;
        }
        default:
          throw new VeniceException("Unknown operator '" + operator + "' for field '" + fieldName + "'");
      }
    }
  }

  private static List<Object> parseJsonArray(JsonNode arrayNode, Schema elementSchema) {
    List<Object> result = new java.util.ArrayList<>();
    for (JsonNode element: arrayNode) {
      String elementStr = element.isTextual() ? element.asText() : element.toString();
      result.add(adaptDataToSchema(elementStr, elementSchema));
    }
    return result;
  }

  private static Map<String, Object> parseJsonMap(JsonNode objectNode, Schema valueSchema) {
    Map<String, Object> result = new HashMap<>();
    Iterator<Map.Entry<String, JsonNode>> entries = objectNode.fields();
    while (entries.hasNext()) {
      Map.Entry<String, JsonNode> entry = entries.next();
      String valueStr = entry.getValue().isTextual() ? entry.getValue().asText() : entry.getValue().toString();
      result.put(entry.getKey(), adaptDataToSchema(valueStr, valueSchema));
    }
    return result;
  }

  private static Object getValueObject(String valueString, RouterBasedStoreSchemaFetcher schemaFetcher) {
    List<SchemaEntry> reversedSchemaEntryList = schemaFetcher.getAllValueSchemasWithId()
        .entrySet()
        .stream()
        .map(entry -> new SchemaEntry(entry.getKey(), entry.getValue()))
        .sorted(Comparator.comparingInt(SchemaEntry::getId).reversed())
        .collect(Collectors.toList());
    return getValueObjectFromCachedSchemas(valueString, reversedSchemaEntryList);
  }

  private static Object getValueObjectFromCachedSchemas(String valueString, List<SchemaEntry> reversedSchemaEntryList) {
    Object value = null;
    Map<Integer, Exception> exceptionMap = new HashMap<>();
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
    private boolean isUpdate;
    private boolean isDelete;
    private String filePath;
    private Properties configProperties;
    private SSLFactory sslFactory;
    private String veniceUrl;
    private String d2ServiceName;
  }
}
