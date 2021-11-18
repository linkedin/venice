package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.router.utils.VeniceRouterUtils;
import io.netty.handler.codec.http.HttpRequest;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.log4j.Logger;

import static com.linkedin.venice.router.api.VenicePathParser.*;


/**
 * Created by mwise on 4/25/16.
 */
public class VenicePathParserHelper {
  private static final Logger logger = Logger.getLogger(VenicePathParserHelper.class);

  private String resourceType = null;
  private String resourceName = null;
  private String key = null;
  private Map<String,String> queryParameters = null;

  public static VenicePathParserHelper parseRequest(HttpRequest request) {
    if (request instanceof BasicFullHttpRequest) {
      BasicFullHttpRequest basicFullHttpRequest = (BasicFullHttpRequest) request;
      if (basicFullHttpRequest.hasAttr(VeniceRouterUtils.PATHPARSER_ATTRIBUTE_KEY)) {
         return basicFullHttpRequest.attr(VeniceRouterUtils.PATHPARSER_ATTRIBUTE_KEY).get();
      }
    }

    VenicePathParserHelper helper = new VenicePathParserHelper(request.uri());

    if (request instanceof  BasicFullHttpRequest) {
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
  public Map<String,String> extractQueryParamters(HttpRequest request) {
    List<NameValuePair> params = null;
    Map<String, String> queryParams = new HashMap<>();
    try {
      params = URLEncodedUtils.parse(new URI(request.uri()), Charset.forName("UTF-8"));
      queryParams = params.stream().collect(Collectors.toMap(NameValuePair::getName, NameValuePair::getValue));
    } catch (URISyntaxException e) {
      logger.warn("Failed to parse uri: " + request.uri());
    }
    return queryParams;
  }

  private VenicePathParserHelper(String uri){
    try {
      URI uriObject = new URI(uri);
      String[] path = uriObject.getPath().split("/");  //getPath does not include the querystring '?f=b64'
      if (path.length > 0) {
        int offset = 0;
        if (path[0].equals("")) {
          offset = 1;  //leading slash in uri splits to an empty path section
        }
        if (path.length - offset >= 1) {
          resourceType = path[0 + offset];
          if (path.length - offset >= 2){
            resourceName = path[1+offset];
            if (path.length - offset >= 3){
              key = path[2+offset];
            }
          }
        }
      }
    } catch (URISyntaxException e) {
      logger.warn("Failed to parse uri: " + uri);
    }
  }

  public String getResourceType() {
    return resourceType;
  }

  public String getResourceName() {
    return resourceName;
  }

  public String getKey() {
    return key;
  }

  public boolean isInvalidStorageRequest(){
    return Utils.isNullOrEmpty(resourceType)
        || (!resourceType.equals(TYPE_STORAGE))
        || Utils.isNullOrEmpty(resourceName);
  }
}
