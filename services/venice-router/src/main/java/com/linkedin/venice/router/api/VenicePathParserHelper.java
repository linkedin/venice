package com.linkedin.venice.router.api;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.venice.router.utils.VeniceRouterUtils;
import io.netty.handler.codec.http.HttpRequest;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by mwise on 4/25/16.
 */
public class VenicePathParserHelper {
  private static final Logger LOGGER = LogManager.getLogger(VenicePathParserHelper.class);

  private RouterResourceType resourceType = null;
  private String resourceName = null;
  private String key = null;

  public static VenicePathParserHelper parseRequest(HttpRequest request) {
    if (request instanceof BasicFullHttpRequest) {
      BasicFullHttpRequest basicFullHttpRequest = (BasicFullHttpRequest) request;
      if (basicFullHttpRequest.hasAttr(VeniceRouterUtils.PATHPARSER_ATTRIBUTE_KEY)) {
        return basicFullHttpRequest.attr(VeniceRouterUtils.PATHPARSER_ATTRIBUTE_KEY).get();
      }
    }

    VenicePathParserHelper helper = new VenicePathParserHelper(request.uri());

    if (request instanceof BasicFullHttpRequest) {
      BasicFullHttpRequest basicFullHttpRequest = (BasicFullHttpRequest) request;
      basicFullHttpRequest.attr(VeniceRouterUtils.PATHPARSER_ATTRIBUTE_KEY).set(helper);
    }
    return helper;
  }

  /**
   * We provide this method as a utility function as opposed to storing the query parameters in this class.  We do this
   * because we only really need these parameters for some very specific circumstances, so we avoid keeping around extra
   * maps on heap in the data path.
   *
   * @param request request to have it's query parameters extracted
   * @return a map keyed by the parameter name and accompanied by it's associated value.
   */
  public Map<String, String> extractQueryParameters(HttpRequest request) {
    Map<String, String> queryPairs = new LinkedHashMap<>();
    try {
      String query = Optional.ofNullable(new URI(request.uri()).getQuery()).orElse("");
      if (query.isEmpty()) {
        return queryPairs;
      }
      String[] pairs = query.split("&");
      for (String pair: pairs) {
        int idx = pair.indexOf("=");
        queryPairs.put(
            URLDecoder.decode(pair.substring(0, idx), "UTF-8"),
            URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
      }
    } catch (UnsupportedEncodingException | URISyntaxException ex) {
      LOGGER.warn("Failed to parse uri query string: {}", request.uri(), ex);
    }
    return queryPairs;
  }

  private VenicePathParserHelper(String uri) {
    try {
      URI uriObject = new URI(uri);
      String typeName = null;
      String[] path = uriObject.getPath().split("/"); // getPath does not include the querystring '?f=b64'
      if (path.length > 0) {
        int offset = 0;
        if (path[0].equals("")) {
          offset = 1; // leading slash in uri splits to an empty path section
        }
        if (path.length - offset >= 1) {
          typeName = path[0 + offset];
          if (path.length - offset >= 2) {
            resourceName = path[1 + offset];
            if (path.length - offset >= 3) {
              key = path[2 + offset];
            }
          }
        }
      }
      this.resourceType = RouterResourceType.getTypeResourceType(typeName);
    } catch (URISyntaxException e) {
      LOGGER.warn("Failed to parse uri: {}", uri);
      this.resourceType = RouterResourceType.TYPE_INVALID;
    }
  }

  public RouterResourceType getResourceType() {
    return resourceType;
  }

  public String getResourceName() {
    return resourceName;
  }

  public String getKey() {
    return key;
  }
}
