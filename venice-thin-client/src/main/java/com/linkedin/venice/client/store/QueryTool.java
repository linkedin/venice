package com.linkedin.venice.client.store;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactoryImpl;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.JsonDecoder;

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

  private static final String SSL_ENABLED = "ssl.enabled";
  private static final String KEYSTORE_TYPE = "keystore.type";
  private static final String TRUSTSTORE_TYPE="truststore.type";
  private static final String KEYSTORE_PASSWORD = "keystore.password";
  private static final String TRUSTSTORE_PASSWORD = "truststore.password";
  private static final String KEYSTORE_PATH="keystore.path";
  private static final String TRUESTSTORE_PATH="truststore.path";


  public static void main(String[] args)
      throws Exception {
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
    System.out.println();

    Properties sslProperties = loadSSLConfig(sslConfigFilePath);
    SSLEngineComponentFactory factory = getSSLEngineComponentFactory(sslProperties);

    // Verify the ssl engine is set up correctly.
    if(url.toLowerCase().trim().startsWith("https") && factory.getSSLContext() == null){
      System.err.println("ERROR: The SSL configuration is not valid to send a request to "+url);
      System.exit(1);
    }

    Map<String, String> outputMap = queryStoreForKey(store, keyString, url, isVsonStore, factory);
    outputMap.entrySet().stream().forEach(System.out::println);
  }

  private static Properties loadSSLConfig(String configFilePath)
      throws IOException {
    Properties props = new Properties();
    try (FileInputStream inputStream = new FileInputStream(configFilePath)) {
      props.load(inputStream);
    } catch (IOException e) {
      System.err.println("ERROR: Could not load ssl config file from path: " + configFilePath);
      throw e;
    }
    return props;
  }

  private static SSLEngineComponentFactory getSSLEngineComponentFactory(Properties sslProperties)
      throws Exception {
    SSLEngineComponentFactoryImpl.Config config = new SSLEngineComponentFactoryImpl.Config();
    config.setSslEnabled(Boolean.valueOf(sslProperties.getProperty(SSL_ENABLED)));
    config.setKeyStoreType(sslProperties.getProperty(KEYSTORE_TYPE));
    config.setTrustStoreFilePassword(sslProperties.getProperty(TRUSTSTORE_TYPE));
    config.setKeyStoreFilePath(sslProperties.getProperty(KEYSTORE_PATH));
    config.setTrustStoreFilePath(sslProperties.getProperty(TRUESTSTORE_PATH));
    config.setKeyStorePassword(sslProperties.getProperty(KEYSTORE_PASSWORD));
    config.setTrustStoreFilePassword(sslProperties.getProperty(TRUSTSTORE_PASSWORD));

    try {
      return new SSLEngineComponentFactoryImpl(config);
    } catch (Exception e) {
      System.err.println("ERROR: Could not build ssl engine component factory by config.");
      throw e;
    }
  }

  public static Map<String, String> queryStoreForKey(String store, String keyString, String url, boolean isVsonStore, SSLEngineComponentFactory sslEngineComponentFactory)
      throws VeniceClientException, ExecutionException, InterruptedException {
    Map<String, String> outputMap = new LinkedHashMap<>();
    try (AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(store).setVeniceURL(url).setVsonClient(isVsonStore).setSslEngineComponentFactory(sslEngineComponentFactory))) {
      AbstractAvroStoreClient<Object, Object> castClient =
          (AbstractAvroStoreClient<Object, Object>) ((StatTrackingStoreClient<Object, Object>) client).getInnerStoreClient();
      SchemaReader schemaReader = castClient.getSchemaReader();
      Schema keySchema = schemaReader.getKeySchema();

      Object key = null;
      // Transfer vson schema to avro schema.
      while(keySchema.getType().equals(Schema.Type.UNION)) {
        keySchema = VsonAvroSchemaAdapter.stripFromUnion(keySchema);
      }
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
            key = new GenericDatumReader<>(keySchema).read(null,
                new JsonDecoder(keySchema, new ByteArrayInputStream(keyString.getBytes())));
          } catch (IOException e) {
            throw new VeniceException("Invalid input key:" + key, e);
          }
          break;
        default:
          throw new VeniceException("Cannot handle key type, found key schema: " + keySchema.toString());
      }

      Object value = client.get(key).get();

      outputMap.put("key-class", key.getClass().getCanonicalName());
      outputMap.put("value-class", value == null ? "null" : value.getClass().getCanonicalName());
      outputMap.put("request-path", castClient.getRequestPathByKey(key));
      outputMap.put("key", keyString);
      outputMap.put("value", value == null ? "null" : value.toString());
      return outputMap;
    }
  }

  private static String removeQuotes(String str) {
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
