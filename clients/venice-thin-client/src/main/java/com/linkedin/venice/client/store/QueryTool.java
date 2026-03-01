package com.linkedin.venice.client.store;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.store.predicate.DoublePredicate;
import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.LongPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
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
 */
public class QueryTool {
  private static final int STORE = 0;
  private static final int KEY_STRING = 1;
  private static final int URL = 2;
  private static final int IS_VSON_STORE = 3;
  private static final int SSL_CONFIG_FILE_PATH = 4;
  private static final int REQUIRED_ARGS_COUNT = 5;

  public static void main(String[] args) throws Exception {
    if (args.length > 0 && args[0].equals("--batch")) {
      // Batch mode: --batch <store> <url> <is_vson_store> <ssl_config_file_path> <key1> [<key2> ...]
      if (args.length < 6) {
        System.out.println(
            "Usage: java -jar venice-thin-client-all.jar --batch <store> <url> <is_vson_store> <ssl_config_file_path> <key1> [<key2> ...]");
        System.exit(1);
      }
      String store = removeQuotes(args[1]);
      String url = removeQuotes(args[2]);
      boolean isVsonStore = Boolean.parseBoolean(removeQuotes(args[3]));
      String sslConfigFilePath = removeQuotes(args[4]);
      Optional<String> sslConfigFilePathArgs =
          StringUtils.isEmpty(sslConfigFilePath) ? Optional.empty() : Optional.of(sslConfigFilePath);

      Set<String> keys = new LinkedHashSet<>();
      for (int i = 5; i < args.length; i++) {
        keys.add(removeQuotes(args[i]));
      }

      Map<String, Map<String, String>> results =
          batchQueryStoreForKeys(store, keys, url, isVsonStore, sslConfigFilePathArgs);
      for (Map.Entry<String, Map<String, String>> entry: results.entrySet()) {
        entry.getValue().entrySet().forEach(System.out::println);
      }
      return;
    }

    if (args.length < REQUIRED_ARGS_COUNT) {
      System.out.println(
          "Usage: java -jar venice-thin-client-all.jar <store> <key_string> <url> <is_vson_store> <ssl_config_file_path>"
              + " [--countByValue --fields=f1,f2 [--topK=N]]"
              + " [--countByBucket --fields=f1 --buckets=high:gt:100,low:lte:100]");
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

    Map<String, String> flags = parseFlags(args, REQUIRED_ARGS_COUNT);

    if (flags.containsKey("countByValue")) {
      String fieldsArg = flags.get("fields");
      if (fieldsArg == null || fieldsArg.trim().isEmpty()) {
        System.out.println("ERROR: --countByValue requires --fields=f1,f2,...");
        System.exit(1);
      }
      String topKString = flags.get("topK");
      int topK;
      if (topKString == null || topKString.isEmpty()) {
        topK = 10;
      } else {
        try {
          topK = Integer.parseInt(topKString);
        } catch (NumberFormatException e) {
          System.out.println("ERROR: --topK must be a positive integer (got '" + topKString + "')");
          System.exit(1);
          return;
        }
      }
      if (topK <= 0) {
        System.out.println("ERROR: --topK must be a positive integer (got '" + topK + "')");
        System.exit(1);
        return;
      }
      String[] fieldNames = fieldsArg.split("\\s*,\\s*");
      for (String fieldName: fieldNames) {
        if (fieldName.isEmpty()) {
          System.out.println("ERROR: --fields contains an empty field name");
          System.exit(1);
        }
      }

      Map<String, Map<Object, Integer>> result =
          queryStoreForCountByValue(store, keyString, url, isVsonStore, sslConfigFilePathArgs, topK, fieldNames);
      result.entrySet().stream().forEach(System.out::println);
    } else if (flags.containsKey("countByBucket")) {
      String fieldsArg = flags.get("fields");
      if (fieldsArg == null || fieldsArg.trim().isEmpty()) {
        System.out.println("ERROR: --countByBucket requires --fields=f1,f2,...");
        System.exit(1);
      }
      String bucketsArg = flags.get("buckets");
      if (bucketsArg == null || bucketsArg.trim().isEmpty()) {
        System.out.println("ERROR: --countByBucket requires --buckets=name:op:value,...");
        System.exit(1);
      }
      String[] fieldNames = fieldsArg.split("\\s*,\\s*");
      for (String fieldName: fieldNames) {
        if (fieldName.isEmpty()) {
          System.out.println("ERROR: --fields contains an empty field name");
          System.exit(1);
        }
      }

      Map<String, Predicate> bucketPredicates = parseBucketPredicates(bucketsArg);
      Map<String, Map<String, Integer>> result = queryStoreForCountByBucket(
          store,
          keyString,
          url,
          isVsonStore,
          sslConfigFilePathArgs,
          bucketPredicates,
          fieldNames);
      result.entrySet().stream().forEach(System.out::println);
    } else {
      Map<String, String> outputMap = queryStoreForKey(store, keyString, url, isVsonStore, sslConfigFilePathArgs);
      outputMap.entrySet().stream().forEach(System.out::println);
    }
  }

  public static Map<String, String> queryStoreForKey(
      String store,
      String keyString,
      String url,
      boolean isVsonStore,
      Optional<String> sslConfigFile) throws Exception {

    SSLFactory factory = createAndValidateSslFactory(url, sslConfigFile);

    Map<String, String> outputMap = new LinkedHashMap<>();
    try (AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(store)
            .setVeniceURL(url)
            .setVsonClient(isVsonStore)
            .setSslFactory(factory))) {
      AbstractAvroStoreClient<Object, Object> castClient = getInnerClient(client);
      Schema keySchema = resolveKeySchema(castClient);

      Object key = convertKey(keyString, keySchema);
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

  public static Map<String, Map<String, String>> batchQueryStoreForKeys(
      String store,
      Set<String> keyStrings,
      String url,
      boolean isVsonStore,
      Optional<String> sslConfigFile) throws Exception {

    SSLFactory factory = createAndValidateSslFactory(url, sslConfigFile);

    Map<String, Map<String, String>> allResults = new LinkedHashMap<>();
    try (AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(store)
            .setVeniceURL(url)
            .setVsonClient(isVsonStore)
            .setSslFactory(factory))) {
      Schema keySchema = resolveKeySchema(getInnerClient(client));

      // Convert all keys and maintain insertion order
      Set<Object> keys = new LinkedHashSet<>();
      Map<Object, String> keyToString = new LinkedHashMap<>();
      for (String keyString: keyStrings) {
        Object key = convertKey(keyString, keySchema);
        keys.add(key);
        keyToString.put(key, keyString);
      }

      // Single batch request for all keys
      Map<Object, Object> values = client.batchGet(keys).get(30, TimeUnit.SECONDS);

      for (Map.Entry<Object, String> entry: keyToString.entrySet()) {
        Object key = entry.getKey();
        String keyString = entry.getValue();
        Object value = values.get(key);

        Map<String, String> outputMap = new LinkedHashMap<>();
        outputMap.put("key", keyString);
        outputMap.put("value", value == null ? "null" : value.toString());
        allResults.put(keyString, outputMap);
      }
      return allResults;
    }
  }

  public static Map<String, Map<Object, Integer>> queryStoreForCountByValue(
      String store,
      String keysString,
      String url,
      boolean isVsonStore,
      Optional<String> sslConfigFile,
      int topK,
      String... fieldNames) throws Exception {

    SSLFactory factory = createAndValidateSslFactory(url, sslConfigFile);

    try (AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(store)
            .setVeniceURL(url)
            .setVsonClient(isVsonStore)
            .setSslFactory(factory))) {
      Schema keySchema = resolveKeySchema(getInnerClient(client));

      List<String> keyStrings = parseKeys(keysString);
      Set<Object> keys = new HashSet<>();
      for (String ks: keyStrings) {
        keys.add(convertKey(ks.trim(), keySchema));
      }
      System.out.println("Parsed " + keys.size() + " keys. About to run countByValue aggregation.");

      AvroGenericReadComputeStoreClient<Object, Object> computeClient =
          (AvroGenericReadComputeStoreClient<Object, Object>) client;
      ComputeAggregationResponse response = computeClient.computeAggregation()
          .countGroupByValue(topK, fieldNames)
          .execute(keys)
          .get(30, TimeUnit.SECONDS);

      Map<String, Map<Object, Integer>> result = new LinkedHashMap<>();
      for (String fieldName: fieldNames) {
        result.put(fieldName, response.getValueToCount(fieldName));
      }
      return result;
    }
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Map<String, Integer>> queryStoreForCountByBucket(
      String store,
      String keysString,
      String url,
      boolean isVsonStore,
      Optional<String> sslConfigFile,
      Map<String, Predicate> bucketPredicates,
      String... fieldNames) throws Exception {

    SSLFactory factory = createAndValidateSslFactory(url, sslConfigFile);

    try (AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(store)
            .setVeniceURL(url)
            .setVsonClient(isVsonStore)
            .setSslFactory(factory))) {
      Schema keySchema = resolveKeySchema(getInnerClient(client));

      List<String> keyStrings = parseKeys(keysString);
      Set<Object> keys = new HashSet<>();
      for (String ks: keyStrings) {
        keys.add(convertKey(ks.trim(), keySchema));
      }
      System.out.println("Parsed " + keys.size() + " keys. About to run countByBucket aggregation.");

      AvroGenericReadComputeStoreClient<Object, Object> computeClient =
          (AvroGenericReadComputeStoreClient<Object, Object>) client;
      ComputeAggregationResponse response = (ComputeAggregationResponse) computeClient.computeAggregation()
          .countGroupByBucket((Map) bucketPredicates, fieldNames)
          .execute(keys)
          .get(30, TimeUnit.SECONDS);

      Map<String, Map<String, Integer>> result = new LinkedHashMap<>();
      for (String fieldName: fieldNames) {
        result.put(fieldName, response.getBucketNameToCount(fieldName));
      }
      return result;
    }
  }

  private static SSLFactory createAndValidateSslFactory(String url, Optional<String> sslConfigFile) throws Exception {
    SSLFactory factory = null;
    if (sslConfigFile.isPresent()) {
      Properties sslProperties = SslUtils.loadSSLConfig(sslConfigFile.get());
      String sslFactoryClassName = sslProperties.getProperty(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
      factory = SslUtils.getSSLFactory(sslProperties, sslFactoryClassName);
    }
    if (url.toLowerCase().trim().startsWith("https") && (factory == null || factory.getSSLContext() == null)) {
      throw new VeniceException("ERROR: The SSL configuration is not valid to send a request to " + url);
    }
    return factory;
  }

  @SuppressWarnings("unchecked")
  private static AbstractAvroStoreClient<Object, Object> getInnerClient(AvroGenericStoreClient<Object, Object> client) {
    return (AbstractAvroStoreClient<Object, Object>) ((StatTrackingStoreClient<Object, Object>) client)
        .getInnerStoreClient();
  }

  private static Schema resolveKeySchema(AbstractAvroStoreClient<Object, Object> client) {
    Schema keySchema = client.getKeySchema();
    while (keySchema.getType().equals(Schema.Type.UNION)) {
      keySchema = VsonAvroSchemaAdapter.stripFromUnion(keySchema);
    }
    return keySchema;
  }

  @SuppressWarnings("unchecked")
  static Map<String, Predicate> parseBucketPredicates(String bucketsArg) {
    Map<String, Predicate> predicates = new LinkedHashMap<>();
    String[] expressions = bucketsArg.split(",");
    for (String expr: expressions) {
      String[] parts = expr.trim().split(":");
      if (parts.length != 3) {
        throw new VeniceException(
            "Invalid bucket expression '" + expr.trim() + "': expected format 'name:operator:value'");
      }
      String bucketName = parts[0].trim();
      String operator = parts[1].trim();
      String valueStr = parts[2].trim();

      Predicate predicate = createTypedPredicate(operator, valueStr);
      predicates.put(bucketName, predicate);
    }
    return predicates;
  }

  static Predicate createTypedPredicate(String operator, String valueStr) {
    // Check for explicit type suffixes: 100i (int), 100L (long), 1.5d (double)
    if (!valueStr.isEmpty()) {
      char lastChar = valueStr.charAt(valueStr.length() - 1);
      String numericPart = valueStr.substring(0, valueStr.length() - 1);
      switch (lastChar) {
        case 'i':
        case 'I':
          try {
            return createIntPredicate(operator, Integer.parseInt(numericPart));
          } catch (NumberFormatException e) {
            throw new VeniceException("Invalid int literal in bucket value: '" + valueStr + "'", e);
          }
        case 'l':
        case 'L':
          try {
            return createLongPredicate(operator, Long.parseLong(numericPart));
          } catch (NumberFormatException e) {
            throw new VeniceException("Invalid long literal in bucket value: '" + valueStr + "'", e);
          }
        case 'd':
        case 'D':
          try {
            return createDoublePredicate(operator, Double.parseDouble(numericPart));
          } catch (NumberFormatException e) {
            throw new VeniceException("Invalid double literal in bucket value: '" + valueStr + "'", e);
          }
        default:
          break;
      }
    }

    // Default: whole numbers → LongPredicate (Avro LONG is far more common than INT)
    try {
      long longVal = Long.parseLong(valueStr);
      return createLongPredicate(operator, longVal);
    } catch (NumberFormatException ignored) {
    }

    // Try double
    try {
      double doubleVal = Double.parseDouble(valueStr);
      return createDoublePredicate(operator, doubleVal);
    } catch (NumberFormatException ignored) {
    }

    // Fall back to string — only eq supported
    if (!"eq".equals(operator)) {
      throw new VeniceException("Operator '" + operator + "' is not supported for string values; only 'eq' is allowed");
    }
    return Predicate.equalTo(valueStr);
  }

  private static Predicate createIntPredicate(String operator, int value) {
    switch (operator) {
      case "eq":
        return IntPredicate.equalTo(value);
      case "gt":
        return IntPredicate.greaterThan(value);
      case "gte":
        return IntPredicate.greaterOrEquals(value);
      case "lt":
        return IntPredicate.lowerThan(value);
      case "lte":
        return IntPredicate.lowerOrEquals(value);
      default:
        throw new VeniceException("Unknown operator '" + operator + "'. Supported: eq, gt, gte, lt, lte");
    }
  }

  private static Predicate createLongPredicate(String operator, long value) {
    switch (operator) {
      case "eq":
        return LongPredicate.equalTo(value);
      case "gt":
        return LongPredicate.greaterThan(value);
      case "gte":
        return LongPredicate.greaterOrEquals(value);
      case "lt":
        return LongPredicate.lowerThan(value);
      case "lte":
        return LongPredicate.lowerOrEquals(value);
      default:
        throw new VeniceException("Unknown operator '" + operator + "'. Supported: eq, gt, gte, lt, lte");
    }
  }

  private static Predicate createDoublePredicate(String operator, double value) {
    switch (operator) {
      case "eq":
        return DoublePredicate.equalTo(value, 0.0);
      case "gt":
        return DoublePredicate.greaterThan(value);
      case "gte":
        return DoublePredicate.greaterOrEquals(value);
      case "lt":
        return DoublePredicate.lowerThan(value);
      case "lte":
        return DoublePredicate.lowerOrEquals(value);
      default:
        throw new VeniceException("Unknown operator '" + operator + "'. Supported: eq, gt, gte, lt, lte");
    }
  }


  public static Object convertKey(String keyString, Schema keySchema) {
    Object key;
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
              AvroCompatibilityHelper
                  .newJsonDecoder(keySchema, new ByteArrayInputStream(keyString.getBytes(StandardCharsets.UTF_8))));
        } catch (IOException e) {
          throw new VeniceException("Invalid input key:" + keyString, e);
        }
        break;
    }
    return key;
  }

  public static String removeQuotes(String str) {
    String result = str;
    if (result.startsWith("\"")) {
      result = result.substring(1);
    }
    if (result.endsWith("\"")) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }

  /**
   * Splits a multi-key string on commas, but only at nesting depth zero.
   * Tracks both {@code {}} and {@code []} nesting so that JSON keys containing commas
   * (e.g. records, arrays, maps) are not incorrectly split. For simple keys like
   * {@code 1,2,3} it behaves identically to {@code String.split(",")}.
   */
  static List<String> parseKeys(String multiKeyString) {
    if (multiKeyString == null || multiKeyString.trim().isEmpty()) {
      return Collections.emptyList();
    }
    List<String> keys = new ArrayList<>();
    int depth = 0;
    int start = 0;
    for (int i = 0; i < multiKeyString.length(); i++) {
      char c = multiKeyString.charAt(i);
      if (c == '{' || c == '[') {
        depth++;
      } else if (c == '}' || c == ']') {
        depth--;
      } else if (c == ',' && depth == 0) {
        String segment = multiKeyString.substring(start, i).trim();
        if (!segment.isEmpty()) {
          keys.add(segment);
        }
        start = i + 1;
      }
    }
    String last = multiKeyString.substring(start).trim();
    if (!last.isEmpty()) {
      keys.add(last);
    }
    return keys;
  }

  /**
   * Parses {@code --key=value} and {@code --flag} style arguments from {@code args[startIndex..]} into a map.
   * Boolean flags (no {@code =}) get the value {@code "true"}.
   */
  static Map<String, String> parseFlags(String[] args, int startIndex) {
    Map<String, String> flags = new HashMap<>();
    for (int i = startIndex; i < args.length; i++) {
      String arg = args[i];
      if (!arg.startsWith("--")) {
        continue;
      }
      arg = arg.substring(2); // strip leading --
      int eq = arg.indexOf('=');
      if (eq >= 0) {
        flags.put(arg.substring(0, eq), arg.substring(eq + 1));
      } else {
        flags.put(arg, "true");
      }
    }
    return flags;
  }
}
