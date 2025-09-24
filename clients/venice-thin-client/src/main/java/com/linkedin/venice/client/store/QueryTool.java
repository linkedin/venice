package com.linkedin.venice.client.store;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.store.predicate.LongPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
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
 * A command-line tool that uses Venice thin client to query values from a store.
 * Supports single key queries and aggregation queries (countByValue and countByBucket).
 */
public class QueryTool {
  private static final int STORE = 0;
  private static final int KEY_STRING = 1;
  private static final int URL = 2;
  private static final int IS_VSON_STORE = 3;
  private static final int SSL_CONFIG_FILE_PATH = 4;
  private static final int REQUIRED_ARGS_COUNT = 5;

  public static void main(String[] args) throws Exception {
    if (args.length > 0) {
      if ("--countByValue".equals(args[0])) {
        handleCountByValue(Arrays.copyOfRange(args, 1, args.length));
        return;
      } else if ("--countByBucket".equals(args[0])) {
        handleCountByBucket(Arrays.copyOfRange(args, 1, args.length));
        return;
      }
    }

    if (args.length < REQUIRED_ARGS_COUNT) {
      System.out.println(
          "Usage: java -jar venice-thin-client-0.1.jar <store> <key_string> <url> <is_vson_store> <ssl_config_file_path>");
      System.exit(1);
    }
    String store = removeQuotes(args[STORE]);
    String keyString = removeQuotes(args[KEY_STRING]);
    String url = removeQuotes(args[URL]);
    boolean isVsonStore = Boolean.parseBoolean(removeQuotes(args[IS_VSON_STORE]));
    String sslConfigFilePath = removeQuotes(args[SSL_CONFIG_FILE_PATH]);
    Optional<String> sslConfigFilePathArgs =
        StringUtils.isEmpty(sslConfigFilePath) ? Optional.empty() : Optional.of(sslConfigFilePath);
    System.out.println();

    Map<String, String> outputMap = queryStoreForKey(store, keyString, url, isVsonStore, sslConfigFilePathArgs);
    outputMap.entrySet().stream().forEach(System.out::println);
  }

  private static void handleCountByValue(String[] args) throws Exception {
    if (args.length < 6) {
      System.err.println(
          "Usage: --countByValue <store> <key_string> <url> <is_vson_store> <ssl_config_file_path> <fields> [topK]");
      System.exit(1);
    }

    String store = removeQuotes(args[0]);
    String keyString = removeQuotes(args[1]);
    String url = removeQuotes(args[2]);
    boolean isVsonStore = Boolean.parseBoolean(removeQuotes(args[3]));
    String sslConfigFilePath = removeQuotes(args[4]);
    String countByValueFields = removeQuotes(args[5]);
    int topK = args.length > 6 ? Integer.parseInt(removeQuotes(args[6])) : 10;

    Optional<String> sslConfigFilePathArgs =
        StringUtils.isEmpty(sslConfigFilePath) ? Optional.empty() : Optional.of(sslConfigFilePath);

    Map<String, String> outputMap =
        queryStoreWithCountByValue(store, keyString, url, isVsonStore, sslConfigFilePathArgs, countByValueFields, topK);
    outputMap.entrySet().stream().forEach(System.out::println);
  }

  private static void handleCountByBucket(String[] args) throws Exception {
    if (args.length < 7) {
      System.err.println(
          "Usage: --countByBucket <store> <key_string> <url> <is_vson_store> <ssl_config_file_path> <fields> <bucket_definitions>");
      System.exit(1);
    }

    String store = removeQuotes(args[0]);
    String keyString = removeQuotes(args[1]);
    String url = removeQuotes(args[2]);
    boolean isVsonStore = Boolean.parseBoolean(removeQuotes(args[3]));
    String sslConfigFilePath = removeQuotes(args[4]);
    String countByBucketFields = removeQuotes(args[5]);
    String bucketDefinitions = removeQuotes(args[6]);

    Optional<String> sslConfigFilePathArgs =
        StringUtils.isEmpty(sslConfigFilePath) ? Optional.empty() : Optional.of(sslConfigFilePath);

    Map<String, String> outputMap = queryStoreWithCountByBucket(
        store,
        keyString,
        url,
        isVsonStore,
        sslConfigFilePathArgs,
        countByBucketFields,
        bucketDefinitions);
    outputMap.entrySet().stream().forEach(System.out::println);
  }

  /**
   * Query store for a single key
   */
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

      Object key = null;
      // Transfer vson schema to avro schema.
      while (keySchema.getType().equals(Schema.Type.UNION)) {
        keySchema = VsonAvroSchemaAdapter.stripFromUnion(keySchema);
      }
      key = convertKey(keyString, keySchema);
      System.out.println("Key string parsed successfully. About to make the query.");

      Object value = client.get(key).get(15, TimeUnit.SECONDS);

      outputMap.put("key-class", key.getClass().getCanonicalName());
      outputMap.put("value-class", value == null ? "null" : value.getClass().getCanonicalName());
      outputMap.put("request-path", castClient.getRequestPathByKey(key));
      outputMap.put("key", keyString);
      outputMap.put("value", value == null ? "null" : value.toString());
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

    SSLFactory factory = createSSLFactory(sslConfigFile);
    validateSSLConfiguration(url, factory);

    Map<String, String> outputMap = new LinkedHashMap<>();
    try (AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(store)
            .setVeniceURL(url)
            .setVsonClient(isVsonStore)
            .setSslFactory(factory))) {

      // Parse keys
      Set<Object> keys = parseKeys(keyString, client);

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

    SSLFactory factory = createSSLFactory(sslConfigFile);
    validateSSLConfiguration(url, factory);

    Map<String, String> outputMap = new LinkedHashMap<>();
    try (AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(store)
            .setVeniceURL(url)
            .setVsonClient(isVsonStore)
            .setSslFactory(factory))) {

      // Parse keys
      Set<Object> keys = parseKeys(keyString, client);

      // Parse fields
      String[] fields = countByBucketFields.split(",");

      // Parse bucket definitions
      Map<String, Predicate<Long>> bucketPredicates = parseBucketDefinitions(bucketDefinitions);

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

  /**
   * Parses bucket definitions string into a map of bucket names to predicates.
   *
   * <p>Currently only supports LongPredicate for integer-based bucket definitions.
   * Future versions may support additional predicate types.</p>
   *
   * <p>Supported formats:</p>
   * <ul>
   *   <li>Range format: "min-max" (e.g., "20-25")</li>
   *   <li>Operator format: "bucketName:operator:value" (e.g., "age:gte:18")</li>
   * </ul>
   *
   * <p>Supported operators: lt, lte, gt, gte, eq</p>
   *
   * @param bucketDefinitions comma-separated bucket definitions
   * @return map of bucket names to LongPredicate instances
   * @throws VeniceException if bucket definition format is invalid
   */
  public static Map<String, Predicate<Long>> parseBucketDefinitions(String bucketDefinitions) {
    Map<String, Predicate<Long>> bucketPredicates = new HashMap<>();

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
          long min = Long.parseLong(range[0].trim());
          long max = Long.parseLong(range[1].trim());

          // Create a predicate for the range [min, max]
          Predicate<Long> predicate =
              Predicate.and(LongPredicate.greaterOrEquals(min), LongPredicate.lowerOrEquals(max));
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
          long value = Long.parseLong(parts[2]);

          Predicate<Long> predicate = null;
          switch (operator.toLowerCase()) {
            case "lt":
              predicate = LongPredicate.lowerThan(value);
              break;
            case "lte":
              predicate = LongPredicate.lowerOrEquals(value);
              break;
            case "gt":
              predicate = LongPredicate.greaterThan(value);
              break;
            case "gte":
              predicate = LongPredicate.greaterOrEquals(value);
              break;
            case "eq":
              predicate = LongPredicate.equalTo(value);
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

  /**
   * Creates SSL factory from configuration file
   */
  private static SSLFactory createSSLFactory(Optional<String> sslConfigFile) throws Exception {
    if (!sslConfigFile.isPresent()) {
      return null;
    }
    Properties sslProperties = SslUtils.loadSSLConfig(sslConfigFile.get());
    String sslFactoryClassName = sslProperties.getProperty(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
    return SslUtils.getSSLFactory(sslProperties, sslFactoryClassName);
  }

  /**
   * Validates SSL configuration for HTTPS URLs
   */
  private static void validateSSLConfiguration(String url, SSLFactory factory) {
    if (url.toLowerCase().trim().startsWith("https") && (factory == null || factory.getSSLContext() == null)) {
      throw new VeniceException("ERROR: The SSL configuration is not valid to send a request to " + url);
    }
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
