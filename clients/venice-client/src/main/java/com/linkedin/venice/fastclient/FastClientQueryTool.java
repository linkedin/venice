package com.linkedin.venice.fastclient;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.TransportClientFactory;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.fastclient.factory.ClientFactory;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;


/**
 * A CLI tool to query values from a Venice store using the fast client with D2 service discovery.
 *
 * Usage: java -jar venice-client-all.jar {@literal <store_name>} {@literal <key>} {@literal <zk_address>} [--insecure]
 *
 * The optional --insecure flag enables a trust-all SSL context for environments where HTTPS
 * is used with self-signed or untrusted certificates (e.g., Docker quickstart).
 * Without this flag, the tool uses plain HTTP transport only.
 */
public class FastClientQueryTool {
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("Usage: java -jar venice-client-all.jar <store_name> <key> <zk_address> [--insecure]");
      System.exit(1);
    }

    String storeName = args[0];
    String keyString = args[1];
    String zkAddress = args[2];
    boolean insecure = args.length > 3 && "--insecure".equals(args[3]);

    TransportClientFactory httpTransport = new HttpClientFactory.Builder().setUsePipelineV2(true).build();
    D2ClientBuilder d2ClientBuilder = new D2ClientBuilder().setZkHosts(zkAddress)
        .setZkSessionTimeout(5000, TimeUnit.MILLISECONDS)
        .setZkStartupTimeout(5000, TimeUnit.MILLISECONDS)
        .setLbWaitTimeout(5000, TimeUnit.MILLISECONDS)
        .setBasePath("/d2");

    Map<String, Object> r2Properties;

    if (insecure) {
      // Trust-all SSLContext for environments with self-signed/untrusted certs (non-production use)
      TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
        public X509Certificate[] getAcceptedIssuers() {
          return new X509Certificate[0];
        }

        public void checkClientTrusted(X509Certificate[] certs, String authType) {
        }

        public void checkServerTrusted(X509Certificate[] certs, String authType) {
        }
      } };
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, trustAllCerts, new SecureRandom());
      SSLParameters sslParameters = sslContext.getDefaultSSLParameters();

      Map<String, TransportClientFactory> transportClients = new HashMap<>();
      transportClients.put("http", httpTransport);
      transportClients.put("https", httpTransport);

      d2ClientBuilder.setSSLContext(sslContext)
          .setSSLParameters(sslParameters)
          .setIsSSLEnabled(true)
          .setClientFactories(transportClients);

      r2Properties = new HashMap<>();
      r2Properties.put(HttpClientFactory.HTTP_SSL_CONTEXT, sslContext);
      r2Properties.put(HttpClientFactory.HTTP_SSL_PARAMS, sslParameters);
    } else {
      r2Properties = Collections.emptyMap();
    }

    D2Client d2Client = d2ClientBuilder.build();
    D2ClientUtils.startClient(d2Client);

    Client r2Client = new TransportClientAdapter(httpTransport.getClient(r2Properties));

    // Build fast client config
    ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
        .setR2Client(r2Client)
        .setD2Client(d2Client)
        .setClusterDiscoveryD2Service("venice-discovery")
        .setStoreMetadataFetchMode(StoreMetadataFetchMode.SERVER_BASED_METADATA)
        .setMetadataRefreshIntervalInSeconds(1)
        .build();

    try (AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericStoreClient(clientConfig)) {
      // Poll until metadata (key schema) is available
      Schema keySchema = null;
      Exception lastException = null;
      long deadline = System.currentTimeMillis() + 30_000;
      while (keySchema == null) {
        if (System.currentTimeMillis() > deadline) {
          String message = "Timed out waiting for metadata to be fetched for store: " + storeName;
          if (lastException != null) {
            throw new VeniceException(message + ". Last error: " + lastException.getMessage(), lastException);
          }
          throw new VeniceException(message);
        }
        try {
          keySchema = client.getKeySchema();
        } catch (Exception e) {
          // Metadata not yet available, record and retry until timeout
          lastException = e;
          keySchema = null;
        }
        if (keySchema == null) {
          Thread.sleep(200);
        }
      }

      Object key = convertKey(keyString, keySchema);

      Object value = client.get(key).get(15, TimeUnit.SECONDS);

      System.out.println("key-class=" + key.getClass().getCanonicalName());
      System.out.println("value-class=" + (value == null ? "null" : value.getClass().getCanonicalName()));
      System.out.println("key=" + keyString);
      System.out.println("value=" + (value == null ? "null" : value.toString()));
    } finally {
      D2ClientUtils.shutdownClient(d2Client);
    }
  }

  static Object convertKey(String keyString, Schema keySchema) {
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
          throw new VeniceException("Invalid input key: " + keyString, e);
        }
        break;
    }
    return key;
  }
}
