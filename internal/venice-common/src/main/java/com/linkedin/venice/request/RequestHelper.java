package com.linkedin.venice.request;

import java.net.URI;


public class RequestHelper {
  /**
   * Parses the '/' separated parts of a request uri.
   * @param uri
   * @return String array of the request parts
   */
  public static String[] getRequestParts(String uri) {
    /**
     * Sometimes req.uri() gives a full uri (e.g. https://host:port/path) and sometimes it only gives a path.
     * Generating a URI lets us always take just the path, but we need to add on the query string.
     */
    URI fullUri = URI.create(uri);
    String path = fullUri.getRawPath();
    if (fullUri.getRawQuery() != null) {
      path += "?" + fullUri.getRawQuery();
    }
    return path.split("/");
  }
}
