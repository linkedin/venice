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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
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

  public static Object convertKey(String keyString, Schema keySchema) {
    Object key;
    switch (keySchema.getType()) {
      case INT:
        key = Integer.parseInt(keyString);
        break;
      case DOUBLE:
        key = Double.parseDouble(keyString);
        break;
      case LONG:
        key = Long.parseLong(keyString);
        break;
      case STRING:
        key = keyString;
        break;
      case RECORD:
        try {
          key = new GenericDatumReader<>(keySchema, keySchema).read(
              null,
              AvroCompatibilityHelper.newJsonDecoder(keySchema, new ByteArrayInputStream(keyString.getBytes())));
        } catch (IOException e) {
          throw new VeniceException("Invalid input key:" + keyString, e);
        }
        break;
      default:
        throw new VeniceException("Cannot handle key type, found key schema: " + keySchema);
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
