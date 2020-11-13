package com.linkedin.venice.router.api;

import com.linkedin.venice.utils.Utils;
import java.net.URI;
import java.net.URISyntaxException;
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

  public VenicePathParserHelper(String uri){
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
