package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
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
  private static final int REQUIRED_ARGS_COUNT = 4;

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    if (args.length < REQUIRED_ARGS_COUNT) {
      System.out.println("Usage: java -jar venice-thin-client-0.1.jar <store> <key_string> <url> <is_vson_store>");
      System.exit(1);
    }
    String store = removeQuotes(args[STORE]);
    String keyString = removeQuotes(args[KEY_STRING]);
    String url = removeQuotes(args[URL]);
    boolean isVsonStore = Boolean.parseBoolean(removeQuotes(args[IS_VSON_STORE]));

    System.out.println();

    Map<String, String> outputMap = queryStoreForKey(store, keyString, url, isVsonStore);
    outputMap.entrySet().stream().forEach(System.out::println);
  }

  public static Map<String, String> queryStoreForKey(String store, String keyString, String url, boolean isVsonStore)
      throws VeniceClientException, ExecutionException, InterruptedException {
    Map<String, String> outputMap = new LinkedHashMap<>();
    try (AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(store).setVeniceURL(url).setVsonClient(isVsonStore))) {
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
