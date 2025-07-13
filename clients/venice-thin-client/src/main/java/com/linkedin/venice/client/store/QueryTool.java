package com.linkedin.venice.client.store;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.commons.lang.StringUtils;


/**
 * A tool use thin client to query the value from a store by the specified key.
 * Now supports facet counting with countByValue and countByBucket functionality.
 */
public class QueryTool {
  private static final int STORE = 0;
  private static final int KEY_STRING = 1;
  private static final int URL = 2;
  private static final int IS_VSON_STORE = 3;
  private static final int SSL_CONFIG_FILE_PATH = 4;
  private static final int REQUIRED_ARGS_COUNT = 5;

  // New arguments for facet counting
  private static final int FACET_COUNTING_MODE = 5;
  private static final int COUNT_BY_VALUE_FIELDS = 6;
  private static final int TOP_K = 7;
  private static final int COUNT_BY_BUCKET_FIELDS = 6;
  private static final int BUCKET_DEFINITIONS = 7;

  public static void main(String[] args) throws Exception {
    System.out.println("=== PURE CLIENT AGG LOGIC ===");
    if (args.length < REQUIRED_ARGS_COUNT) {
      System.out.println(
          "Usage: java -jar venice-thin-client-0.1.jar <store> <key_string> <url> <is_vson_store> <ssl_config_file_path> [facet_counting_mode] [count_by_value_fields] [count_by_bucket_fields] [top_k] [bucket_definitions]");
      System.out.println();
      System.out.println("Facet counting mode options:");
      System.out.println("  - 'single': Query single key (default)");
      System.out.println("  - 'countByValue': Count distinct values for specified fields");
      System.out.println("  - 'countByBucket': Count records matching bucket predicates");
      System.out.println();
      System.out.println("Examples:");
      System.out.println("  # Single key query (original behavior):");
      System.out
          .println("  java -jar venice-thin-client-0.1.jar store_name 'key1' https://router:port false ssl.config");
      System.out.println();
      System.out.println("  # Count by value (top 5 most common firstName values):");
      System.out.println(
          "  java -jar venice-thin-client-0.1.jar store_name 'key1,key2,key3' https://router:port false ssl.config countByValue 'firstName,lastName' 5");
      System.out.println();
      System.out.println("  # Count by bucket (age ranges):");
      System.out.println(
          "  java -jar venice-thin-client-0.1.jar store_name 'key1,key2,key3' https://router:port false ssl.config countByBucket 'age' 10 'young:lt:30,senior:gte:30'");
      System.exit(1);
    }

    String store = removeQuotes(args[STORE]);
    String keyString = removeQuotes(args[KEY_STRING]);
    String url = removeQuotes(args[URL]);
    boolean isVsonStore = Boolean.parseBoolean(removeQuotes(args[IS_VSON_STORE]));
    String sslConfigFilePath = removeQuotes(args[SSL_CONFIG_FILE_PATH]);
    Optional<String> sslConfigFilePathArgs =
        StringUtils.isEmpty(sslConfigFilePath) ? Optional.empty() : Optional.of(sslConfigFilePath);

    // Parse facet counting parameters
    String facetCountingMode = args.length > FACET_COUNTING_MODE ? removeQuotes(args[FACET_COUNTING_MODE]) : "single";
    String countByValueFields = args.length > COUNT_BY_VALUE_FIELDS ? removeQuotes(args[COUNT_BY_VALUE_FIELDS]) : null;
    String countByBucketFields =
        args.length > COUNT_BY_BUCKET_FIELDS ? removeQuotes(args[COUNT_BY_BUCKET_FIELDS]) : null;
    int topK = 10; // Default value
    String bucketDefinitions = args.length > BUCKET_DEFINITIONS ? removeQuotes(args[BUCKET_DEFINITIONS]) : null;

    // Parse topK only for countByValue mode
    if ("countByValue".equals(facetCountingMode) && args.length > TOP_K) {
      topK = Integer.parseInt(removeQuotes(args[TOP_K]));
    }

    System.out.println();

    if ("single".equals(facetCountingMode)) {
      // Original single key query behavior
      Map<String, String> outputMap = queryStoreForKey(store, keyString, url, isVsonStore, sslConfigFilePathArgs);
      outputMap.entrySet().stream().forEach(System.out::println);
    } else if ("countByValue".equals(facetCountingMode)) {
      // Count by value functionality
      Map<String, String> outputMap = queryStoreWithCountByValue(
          store,
          keyString,
          url,
          isVsonStore,
          sslConfigFilePathArgs,
          countByValueFields,
          topK);
      outputMap.entrySet().stream().forEach(System.out::println);
    } else if ("countByBucket".equals(facetCountingMode)) {
      // Count by bucket functionality
      Map<String, String> outputMap = queryStoreWithCountByBucket(
          store,
          keyString,
          url,
          isVsonStore,
          sslConfigFilePathArgs,
          countByBucketFields,
          bucketDefinitions);
      outputMap.entrySet().stream().forEach(System.out::println);
    } else {
      throw new VeniceException("Unknown facet counting mode: " + facetCountingMode);
    }
    System.exit(0);
  }

  public static Map<String, String> queryStoreForKey(
      String store,
      String keyString,
      String url,
      boolean isVsonStore,
      Optional<String> sslConfigFile) throws Exception {

    SSLFactory factory = null;
    if (sslConfigFile.isPresent()) {
      Properties sslProperties = SslUtils.loadSSLConfig(sslConfigFile.get());
      String sslFactoryClassName = sslProperties.getProperty(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
      factory = SslUtils.getSSLFactory(sslProperties, sslFactoryClassName);
    }

    // Verify the ssl engine is set up correctly.
    if (url.toLowerCase().trim().startsWith("https") && (factory == null || factory.getSSLContext() == null)) {
      throw new VeniceException("ERROR: The SSL configuration is not valid to send a request to " + url);
    }

    Map<String, String> outputMap = new LinkedHashMap<>();
    try (AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(store)
            .setVeniceURL(url)
            .setVsonClient(isVsonStore)
            .setSslFactory(factory))) {
      AbstractAvroStoreClient<Object, Object> castClient =
          (AbstractAvroStoreClient<Object, Object>) ((StatTrackingStoreClient<Object, Object>) client)
              .getInnerStoreClient();
      Schema keySchema = castClient.getKeySchema();

      // Transfer vson schema to avro schema.
      while (keySchema.getType().equals(Schema.Type.UNION)) {
        keySchema = VsonAvroSchemaAdapter.stripFromUnion(keySchema);
      }

      // Check if keyString contains multiple keys
      if (keyString.contains(",")) {
        // Multiple keys - query each one separately
        String[] keyStrings = keyString.split(",");
        for (int i = 0; i < keyStrings.length; i++) {
          String singleKeyString = keyStrings[i].trim();
          Object key = convertKey(singleKeyString, keySchema);
          Object value = client.get(key).get(15, TimeUnit.SECONDS);

          outputMap.put("key" + (i + 1) + "-class", key.getClass().getCanonicalName());
          outputMap.put("key" + (i + 1) + "-value-class", value == null ? "null" : value.getClass().getCanonicalName());
          outputMap.put("key" + (i + 1) + "-request-path", castClient.getRequestPathByKey(key));
          outputMap.put("key" + (i + 1), singleKeyString);
          outputMap.put("value" + (i + 1), value == null ? "null" : value.toString());
        }
        outputMap.put("total-keys", String.valueOf(keyStrings.length));
      } else {
        // Single key - original behavior
        Object key = convertKey(keyString, keySchema);
        System.out.println("Key string parsed successfully. About to make the query.");

        Object value = client.get(key).get(15, TimeUnit.SECONDS);

        outputMap.put("key-class", key.getClass().getCanonicalName());
        outputMap.put("value-class", value == null ? "null" : value.getClass().getCanonicalName());
        outputMap.put("request-path", castClient.getRequestPathByKey(key));
        outputMap.put("key", keyString);
        outputMap.put("value", value == null ? "null" : value.toString());
      }

      return outputMap;
    }
  }

  public static Map<String, String> queryStoreWithCountByValue(
      String store,
      String keyString,
      String url,
      boolean isVsonStore,
      Optional<String> sslConfigFile,
      String countByValueFields,
      int topK) throws Exception {

    SSLFactory factory = null;
    if (sslConfigFile.isPresent()) {
      Properties sslProperties = SslUtils.loadSSLConfig(sslConfigFile.get());
      String sslFactoryClassName = sslProperties.getProperty(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
      factory = SslUtils.getSSLFactory(sslProperties, sslFactoryClassName);
    }

    if (url.toLowerCase().trim().startsWith("https") && (factory == null || factory.getSSLContext() == null)) {
      throw new VeniceException("ERROR: The SSL configuration is not valid to send a request to " + url);
    }

    Map<String, String> outputMap = new LinkedHashMap<>();
    try (AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(store)
            .setVeniceURL(url)
            .setVsonClient(isVsonStore)
            .setSslFactory(factory))) {

      // Parse keys
      Set<Object> keys = parseKeys(keyString, client);
      System.out.println("Keys parsed successfully. About to make the countByValue query.");

      // Parse fields
      String[] fields = countByValueFields.split(",");

      // Create a pure client-side aggregation builder
      ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(store)
          .setVeniceURL(url)
          .setVsonClient(isVsonStore)
          .setSslFactory(factory);
      AvroGenericReadComputeStoreClient<Object, Object> computeStoreClient =
          (AvroGenericReadComputeStoreClient<Object, Object>) client;
      AvroComputeAggregationRequestBuilder<Object> builder =
          new AvroComputeAggregationRequestBuilder<>(computeStoreClient, ClientFactory.getSchemaReader(clientConfig));
      builder.countGroupByValue(topK, fields);

      ComputeAggregationResponse response = builder.execute(keys).get(60, TimeUnit.SECONDS);

      outputMap.put("query-type", "countByValue");
      outputMap.put("keys", keyString);
      outputMap.put("fields", countByValueFields);
      outputMap.put("topK", String.valueOf(topK));

      // Add results for each field
      for (String field: fields) {
        Map<Object, Integer> valueCounts = response.getValueToCount(field);
        outputMap.put(field + "-counts", valueCounts.toString());
      }

      return outputMap;
    }
  }

  public static Map<String, String> queryStoreWithCountByBucket(
      String store,
      String keyString,
      String url,
      boolean isVsonStore,
      Optional<String> sslConfigFile,
      String countByBucketFields,
      String bucketDefinitions) throws Exception {

    SSLFactory factory = null;
    if (sslConfigFile.isPresent()) {
      Properties sslProperties = SslUtils.loadSSLConfig(sslConfigFile.get());
      String sslFactoryClassName = sslProperties.getProperty(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
      factory = SslUtils.getSSLFactory(sslProperties, sslFactoryClassName);
    }

    if (url.toLowerCase().trim().startsWith("https") && (factory == null || factory.getSSLContext() == null)) {
      throw new VeniceException("ERROR: The SSL configuration is not valid to send a request to " + url);
    }

    Map<String, String> outputMap = new LinkedHashMap<>();
    try (AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(store)
            .setVeniceURL(url)
            .setVsonClient(isVsonStore)
            .setSslFactory(factory))) {

      // Parse keys
      Set<Object> keys = parseKeys(keyString, client);
      System.out.println("Keys parsed successfully. About to make the countByBucket query.");

      // Parse fields
      String[] fields = countByBucketFields.split(",");

      // Parse bucket definitions
      Map<String, Predicate<Integer>> bucketPredicates = parseBucketDefinitions(bucketDefinitions);

      // Print bucket definitions in a nice way
      System.out.println("Bucket Definitions:");
      for (Map.Entry<String, Predicate<Integer>> entry: bucketPredicates.entrySet()) {
        System.out.println("  - " + entry.getKey() + ": " + entry.getValue().toString());
      }
      System.out.println();

      // Create a pure client-side aggregation builder
      ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(store)
          .setVeniceURL(url)
          .setVsonClient(isVsonStore)
          .setSslFactory(factory);
      AvroGenericReadComputeStoreClient<Object, Object> computeStoreClient =
          (AvroGenericReadComputeStoreClient<Object, Object>) client;
      AvroComputeAggregationRequestBuilder<Object> builder =
          new AvroComputeAggregationRequestBuilder<>(computeStoreClient, ClientFactory.getSchemaReader(clientConfig));
      builder.countGroupByBucket(bucketPredicates, fields);

      ComputeAggregationResponse response = builder.execute(keys).get(60, TimeUnit.SECONDS);

      outputMap.put("query-type", "countByBucket");
      outputMap.put("keys", keyString);
      outputMap.put("fields", countByBucketFields);
      outputMap.put("bucket-definitions", bucketDefinitions);

      // Add results for each field
      for (String field: fields) {
        Map<String, Integer> bucketCounts = response.getBucketNameToCount(field);
        outputMap.put(field + "-bucket-counts", bucketCounts.toString());
      }

      return outputMap;
    }
  }

  public static Set<Object> parseKeys(String keyString, AvroGenericStoreClient<Object, Object> client)
      throws Exception {
    Set<Object> keys = new HashSet<>();
    String[] keyStrings = keyString.split(",");

    AbstractAvroStoreClient<Object, Object> castClient =
        (AbstractAvroStoreClient<Object, Object>) ((StatTrackingStoreClient<Object, Object>) client)
            .getInnerStoreClient();
    Schema keySchema = castClient.getKeySchema();

    // Transfer vson schema to avro schema.
    while (keySchema.getType().equals(Schema.Type.UNION)) {
      keySchema = VsonAvroSchemaAdapter.stripFromUnion(keySchema);
    }

    for (String keyStr: keyStrings) {
      Object key = convertKey(keyStr.trim(), keySchema);
      keys.add(key);
    }

    return keys;
  }

  public static Map<String, Predicate<Integer>> parseBucketDefinitions(String bucketDefinitions) {
    Map<String, Predicate<Integer>> bucketPredicates = new HashMap<>();

    if (bucketDefinitions == null || bucketDefinitions.isEmpty()) {
      return bucketPredicates;
    }

    String[] bucketDefs = bucketDefinitions.split(",");
    for (String bucketDef: bucketDefs) {
      bucketDef = bucketDef.trim();

      // Check if it's a range format (e.g., "20-25")
      if (bucketDef.contains("-")) {
        String[] range = bucketDef.split("-");
        if (range.length != 2) {
          throw new VeniceException("Invalid range format: " + bucketDef + ". Expected format: min-max");
        }

        try {
          int min = Integer.parseInt(range[0].trim());
          int max = Integer.parseInt(range[1].trim());

          // Create a predicate for the range [min, max]
          Predicate<Integer> predicate =
              Predicate.and(IntPredicate.greaterOrEquals(min), IntPredicate.lowerOrEquals(max));
          bucketPredicates.put(bucketDef, predicate);
        } catch (NumberFormatException e) {
          throw new VeniceException("Invalid number format in range: " + bucketDef, e);
        }
      } else {
        // Legacy operator format (e.g., "bucketName:operator:value")
        String[] parts = bucketDef.split(":");
        if (parts.length != 3) {
          throw new VeniceException(
              "Invalid bucket definition format: " + bucketDef
                  + ". Expected format: bucketName:operator:value or min-max");
        }

        String bucketName = parts[0];
        String operator = parts[1];

        try {
          int value = Integer.parseInt(parts[2]);

          Predicate<Integer> predicate = null;
          switch (operator.toLowerCase()) {
            case "lt":
              predicate = IntPredicate.lowerThan(value);
              break;
            case "lte":
              predicate = IntPredicate.lowerOrEquals(value);
              break;
            case "gt":
              predicate = IntPredicate.greaterThan(value);
              break;
            case "gte":
              predicate = IntPredicate.greaterOrEquals(value);
              break;
            case "eq":
              predicate = IntPredicate.equalTo(value);
              break;
            default:
              throw new VeniceException("Unknown operator: " + operator);
          }

          bucketPredicates.put(bucketName, predicate);
        } catch (NumberFormatException e) {
          throw new VeniceException("Invalid number format in bucket definition: " + bucketDef, e);
        }
      }
    }

    return bucketPredicates;
  }

  public static Object convertKey(String keyString, Schema keySchema) {
    Object key;
    try {
      switch (keySchema.getType()) {
        case INT:
          key = Integer.parseInt(keyString);
          break;
        case LONG:
          key = Long.parseLong(keyString);
          break;
        case FLOAT:
          key = Float.parseFloat(keyString);
          break;
        case DOUBLE:
          key = Double.parseDouble(keyString);
          break;
        case BOOLEAN:
          key = Boolean.parseBoolean(keyString);
          break;
        case STRING:
          key = keyString;
          break;
        default:
          try {
            key = new GenericDatumReader<>(keySchema, keySchema).read(
                null,
                AvroCompatibilityHelper.newJsonDecoder(keySchema, new ByteArrayInputStream(keyString.getBytes())));
          } catch (IOException e) {
            throw new VeniceException("Invalid input key:" + keyString, e);
          }
          break;
      }
    } catch (NumberFormatException e) {
      throw new VeniceException("Invalid number format for key: " + keyString, e);
    }
    return key;
  }

  public static String removeQuotes(String str) {
    String result = str;
    if (result.startsWith("\"")) {
      result = result.substring(1);
    }
    if (str.endsWith("\"")) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }
}
