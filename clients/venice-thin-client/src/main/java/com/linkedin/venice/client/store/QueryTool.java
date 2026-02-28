package com.linkedin.venice.client.store;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
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
    if (args.length < REQUIRED_ARGS_COUNT) {
      System.out.println(
          "Usage: java -jar venice-thin-client-0.1.jar <store> <key_string> <url> <is_vson_store> <ssl_config_file_path>"
              + " [--countByValue --fields=f1,f2 [--topK=N]]");
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
    } else {
      Map<String, String> outputMap = queryStoreForKey(store, keyString, url, isVsonStore, sslConfigFilePathArgs);
      outputMap.entrySet().stream().forEach(System.out::println);
    }
  }

  static SSLFactory createSslFactory(Optional<String> sslConfigFile) {
    if (!sslConfigFile.isPresent()) {
      return null;
    }
    try {
      Properties sslProperties = SslUtils.loadSSLConfig(sslConfigFile.get());
      String sslFactoryClassName = sslProperties.getProperty(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
      return SslUtils.getSSLFactory(sslProperties, sslFactoryClassName);
    } catch (IOException e) {
      throw new VeniceException("Failed to load SSL config from " + sslConfigFile.get(), e);
    }
  }

  static void validateSslFactory(String url, SSLFactory factory) {
    if (url.toLowerCase().trim().startsWith("https") && (factory == null || factory.getSSLContext() == null)) {
      throw new VeniceException("ERROR: The SSL configuration is not valid to send a request to " + url);
    }
  }

  public static Map<String, String> queryStoreForKey(
      String store,
      String keyString,
      String url,
      boolean isVsonStore,
      Optional<String> sslConfigFile) throws Exception {

    SSLFactory factory = createSslFactory(sslConfigFile);
    validateSslFactory(url, factory);

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

  public static Map<String, Map<Object, Integer>> queryStoreForCountByValue(
      String store,
      String keysString,
      String url,
      boolean isVsonStore,
      Optional<String> sslConfigFile,
      int topK,
      String... fieldNames) throws Exception {

    SSLFactory factory = createSslFactory(sslConfigFile);
    validateSslFactory(url, factory);

    try (AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(store)
            .setVeniceURL(url)
            .setVsonClient(isVsonStore)
            .setSslFactory(factory))) {
      AbstractAvroStoreClient<Object, Object> castClient =
          (AbstractAvroStoreClient<Object, Object>) ((StatTrackingStoreClient<Object, Object>) client)
              .getInnerStoreClient();
      Schema keySchema = castClient.getKeySchema();

      while (keySchema.getType().equals(Schema.Type.UNION)) {
        keySchema = VsonAvroSchemaAdapter.stripFromUnion(keySchema);
      }

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
