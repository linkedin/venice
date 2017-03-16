package com.linkedin.venice;

/**
 * Created by mwise on 3/22/16.
 */
public class HttpConstants {
  public static final String TEXT_PLAIN = "text/plain";
  public static final String TEXT_HTML = "text/html";
  public static final String APPLICATION_OCTET = "application/octet-stream";
  public static final String JSON = "application/json";


  private static final String COLON_SLASH_SLASH = "://";
  public static final String HTTP = "http";
  public static final String HTTPS = "https";
  public static final String HTTP_PREFIX = HTTP + COLON_SLASH_SLASH;
  public static final String HTTPS_PREFIX = HTTPS + COLON_SLASH_SLASH;
  public static final String LOCALHOST = "localhost";

  public static final String VENICE_OFFSET = "X-VENICE-OFFSET";
  public static final String VENICE_STORE_VERSION = "X-VENICE-STORE-VERSION";
  public static final String VENICE_PARTITION = "X-VENICE-PARTITION";
  public static final String VENICE_API_VERSION = "X-VENICE-API-VERSION";
  public static final String VENICE_SCHEMA_ID = "X-VENICE-SCHEMA-ID";

  public static final int SC_MISDIRECTED_REQUEST = 421;

  private HttpConstants(){}
}
