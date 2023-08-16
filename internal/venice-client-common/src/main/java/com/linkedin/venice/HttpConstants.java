package com.linkedin.venice;

public class HttpConstants {
  public static final String TEXT_PLAIN = "text/plain";
  public static final String TEXT_HTML = "text/html";
  public static final String JSON = "application/json";
  public static final String AVRO_BINARY = "avro/binary";
  public static final String BINARY = "application/octet-stream";

  private static final String COLON_SLASH_SLASH = "://";
  public static final String HTTP = "http";
  public static final String HTTPS = "https";
  public static final String HTTP_PREFIX = HTTP + COLON_SLASH_SLASH;
  public static final String HTTPS_PREFIX = HTTPS + COLON_SLASH_SLASH;
  public static final String LOCALHOST = "localhost";
  public static final String HTTP_GET = "GET";
  public static final String HTTPS_POST = "POST";

  public static final String VENICE_STORE_VERSION = "X-VENICE-STORE-VERSION";
  public static final String VENICE_API_VERSION = "X-VENICE-API-VERSION";
  public static final String VENICE_SCHEMA_ID = "X-VENICE-SCHEMA-ID";
  public static final String VENICE_REQUEST_RCU = "X-VENICE-RCU";
  public static final String VENICE_RETRY = "X-VENICE-RETRY";

  public static final String VENICE_COMPRESSION_STRATEGY = "X-VENICE-COMPRESSION-STRATEGY";
  public static final String VENICE_SUPPORTED_COMPRESSION_STRATEGY = "X-VENICE-SUPPORTED-COMPRESSION-STRATEGY";

  public static final String VENICE_STREAMING = "X-VENICE-STREAMING";
  public static final String VENICE_STREAMING_RESPONSE = "X-VENICE-STREAMING-RESPONSE";

  public static final String VENICE_KEY_COUNT = "X-VENICE-KEY-COUNT";

  public static final String VENICE_COMPUTE_VALUE_SCHEMA_ID = "X-VENICE-COMPUTE-VALUE-SCHEMA-ID";

  public static final String VENICE_ALLOW_REDIRECT = "X-VENICE-ALLOW-REDIRECT";

  public static final String VENICE_CLIENT_COMPUTE = "X-VENICE-CLIENT-COMPUTE";

  public static final int SC_MISDIRECTED_REQUEST = 421;

  private HttpConstants() {
  }
}
