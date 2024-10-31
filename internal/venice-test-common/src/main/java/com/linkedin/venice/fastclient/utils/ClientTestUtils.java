package com.linkedin.venice.fastclient.utils;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.TransportClientFactory;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.r2.transport.http.common.HttpProtocolVersion;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.transport.HttpClient5BasedR2Client;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class ClientTestUtils {
  public enum FastClientHTTPVariant {
    HTTP_1_1_BASED_R2_CLIENT, HTTP_2_BASED_R2_CLIENT, HTTP_2_BASED_HTTPCLIENT5
  }

  public static final Object[] FASTCLIENT_HTTP_VARIANTS = { FastClientHTTPVariant.HTTP_1_1_BASED_R2_CLIENT,
      FastClientHTTPVariant.HTTP_2_BASED_R2_CLIENT, FastClientHTTPVariant.HTTP_2_BASED_HTTPCLIENT5 };

  // Small => No explicit "_STREAMING" RequestTypes
  public static final Object[] REQUEST_TYPES_SMALL =
      { RequestType.SINGLE_GET, RequestType.MULTI_GET, RequestType.COMPUTE };

  public static final Object[] STORE_METADATA_FETCH_MODES = { StoreMetadataFetchMode.SERVER_BASED_METADATA };

  private static Client setupTransportClientFactory(FastClientHTTPVariant fastClientHTTPVariant) {
    /**
     * 'setUsePipelineV2' is required to force http2 for all types of request.
     */
    TransportClientFactory transportClientFactory = new HttpClientFactory.Builder().setUsePipelineV2(true).build();
    SSLFactory sslFactory = SslUtils.getVeniceLocalSslFactory();
    final Map<String, Object> properties = new HashMap();
    properties.put(HttpClientFactory.HTTP_SSL_CONTEXT, sslFactory.getSSLContext());
    properties.put(HttpClientFactory.HTTP_SSL_PARAMS, sslFactory.getSSLParameters());
    if (fastClientHTTPVariant == FastClientHTTPVariant.HTTP_2_BASED_R2_CLIENT) {
      properties.put(HttpClientFactory.HTTP_PROTOCOL_VERSION, HttpProtocolVersion.HTTP_2.toString());
    }

    return new TransportClientAdapter(transportClientFactory.getClient(properties));
  }

  public static Client getR2Client() throws Exception {
    return getR2Client(FastClientHTTPVariant.HTTP_1_1_BASED_R2_CLIENT);
  }

  public static Client getR2Client(FastClientHTTPVariant fastClientHTTPVariant) throws Exception {
    switch (fastClientHTTPVariant) {
      case HTTP_1_1_BASED_R2_CLIENT:
      case HTTP_2_BASED_R2_CLIENT:
        return setupTransportClientFactory(fastClientHTTPVariant);

      case HTTP_2_BASED_HTTPCLIENT5:
        return HttpClient5BasedR2Client.getR2Client(SslUtils.getVeniceLocalSslFactory().getSSLContext(), 8, 5000);

      default:
        throw new VeniceException("Unsupported Http type: " + fastClientHTTPVariant);
    }
  }

  public static String getMetricPrefix(String storeName, RequestType requestType) {
    String metricPrefix = "." + storeName;
    switch (requestType) {
      case MULTI_GET:
        metricPrefix += "--" + RequestType.MULTI_GET_STREAMING.getMetricPrefix();
        break;
      case COMPUTE:
        metricPrefix += "--" + RequestType.COMPUTE_STREAMING.getMetricPrefix();
        break;
      case MULTI_GET_STREAMING:
      case COMPUTE_STREAMING:
      case SINGLE_GET:
        metricPrefix += "--" + requestType.getMetricPrefix();
        break;
      default:
        throw new VeniceUnsupportedOperationException("Request type: " + requestType);
    }
    return metricPrefix;
  }

  public static Set<String> getAllMetricPrefixes(String storeName) {
    Set<String> allMetricPrefixes = new HashSet<>();
    for (RequestType requestType: RequestType.values()) {
      for (int i = 0; i < 2; i++) {
        allMetricPrefixes.add(getMetricPrefix(storeName, requestType));
      }
    }
    return allMetricPrefixes;
  }
}
