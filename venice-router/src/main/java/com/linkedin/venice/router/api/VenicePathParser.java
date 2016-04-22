package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.base.misc.QueryStringDecoder;
import com.linkedin.ddsstorage.router.api.ResourcePathParser;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.RequestConstants;
import com.linkedin.venice.exceptions.VeniceException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;


/***
 *   Inbound request to the router will look like:
 *   /read/storename/key?f=fmt
 *
 *   'read' is a literal, meaning we will request the value for a single key
 *   storename will be the name of the requested store
 *   key is the key being looked up
 *   fmt is an optional format parameter, one of 'string' or 'b64'.  If ommitted, assumed to be 'string'
 *
 *   The VenicePathParser is responsible for looking up the active version of the store, and constructing the store-version
 */
public class VenicePathParser implements ResourcePathParser<VeniceStoragePath, RouterKey> {

  private static final Logger logger = Logger.getLogger(VenicePathParser.class.getName());

  public static final String STORE_VERSION_SEP = "_v";
  public static final Pattern STORE_PATTERN = Pattern.compile("\\A[a-zA-Z][a-zA-Z0-9_-]*\\z"); // \A and \z are start and end of string
  public static final int STORE_MAX_LENGTH = 128;
  public static final String SEP = "/";

  public static final String TYPE_STORAGE = "storage";
  public static final String TYPE_CONTROLLER = "controller";

  private VeniceVersionFinder versionFinder;
  private VenicePartitionFinder partitionFinder;

  public VenicePathParser(VeniceVersionFinder versionFinder, VenicePartitionFinder partitionFinder){
    this.versionFinder = versionFinder;
    this.partitionFinder = partitionFinder;
  };

  @Override
  public VeniceStoragePath parseResourceUri(String uri) throws RouterException {
    VenicePathParserHelper pathHelper = new VenicePathParserHelper(uri);
    if (pathHelper.isInvalidStorageRequest()){
      throw new RouterException(HttpResponseStatus.BAD_REQUEST,
          "Request URI must have a resource type, storename, and key", true);
    }
    String resourceType = pathHelper.getResourceType();
    String storename = pathHelper.getResourceName();
    String key = pathHelper.getKey();

    if (resourceType.equals(TYPE_STORAGE)) {
      String resourceName = getResourceFromStoreName(storename);
      RouterKey routerKey;
      if (isFormatB64(uri)){
        routerKey = RouterKey.fromBase64(key);
      } else {
        routerKey = RouterKey.fromString(key);
      }
      String partition = Integer.toString(partitionFinder.findPartitionNumber(resourceName, routerKey));
      return new VeniceStoragePath(resourceName, Collections.singleton(routerKey), partition);

    } else {
      throw new RouterException(HttpResponseStatus.BAD_REQUEST,
          "Requested resource type: " + resourceType + " is not a valid type", true);
    }
  }

  @Override
  public VeniceStoragePath substitutePartitionKey(VeniceStoragePath path, RouterKey key) {
    String partition = Integer.toString(partitionFinder.findPartitionNumber(path.getResourceName(), key));
    return new VeniceStoragePath(path.getResourceName(), Collections.singleton(key), partition);
  }

  @Override
  public VeniceStoragePath substitutePartitionKey(VeniceStoragePath path, Collection<RouterKey> keys) {
    // Assumes all keys on same partition
    String partition = Integer.toString(
        partitionFinder.findPartitionNumber(path.getResourceName(), keys.iterator().next()));
    return new VeniceStoragePath(path.getResourceName(), keys, partition);
  }

  /***
   * Queries the helix metadata repository for the
   *
   * @param storeName
   * @return store-version, matches the helix resource
   */
  private String getResourceFromStoreName(String storeName) throws RouterException {
    int version = versionFinder.getVersion(storeName);
    return storeName + STORE_VERSION_SEP + version;
  }

  public static boolean isStoreNameValid(String storeName){
    if (storeName.length() > STORE_MAX_LENGTH){
      return false;
    }
    Matcher m = STORE_PATTERN.matcher(storeName);
    return m.matches();
  }

  protected static boolean isFormatB64(String key) {
    String format = RequestConstants.DEFAULT_FORMAT; //"string"
    QueryStringDecoder queryStringParser = new QueryStringDecoder(key, StandardCharsets.UTF_8);
    if (queryStringParser.getParameters().keySet().contains(RequestConstants.FORMAT_KEY)) {
      format = queryStringParser.getParameters().get(RequestConstants.FORMAT_KEY).get(0);
    }
    return format.equals(RequestConstants.B64_FORMAT);
  }


}
