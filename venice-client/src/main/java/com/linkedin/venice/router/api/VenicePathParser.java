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
public class VenicePathParser implements ResourcePathParser<Path, RouterKey> {

  private static final Logger logger = Logger.getLogger(VenicePathParser.class.getName());

  public static final String STORE_VERSION_SEP = "_v";
  public static final Pattern STORE_PATTERN = Pattern.compile("\\A[a-zA-Z][a-zA-Z0-9_-]*\\z"); // \A and \z are start and end of string
  public static final int STORE_MAX_LENGTH = 128;
  public static final String SEP = "/";
  public static final String ACTION_STORAGE = "storage";

  private VeniceVersionFinder versionFinder;
  private VenicePartitionFinder partitionFinder;

  public VenicePathParser(VeniceVersionFinder versionFinder, VenicePartitionFinder partitionFinder){
    this.versionFinder = versionFinder;
    this.partitionFinder = partitionFinder;
  };

  @Override
  public Path parseResourceUri(String uri) throws RouterException {
    URI uriObject;
    try {
      uriObject = new URI(uri);
    } catch (URISyntaxException e) {
      logger.error(e);
      throw new RouterException(HttpResponseStatus.INTERNAL_SERVER_ERROR, e, true);
    }
    String[] path = uriObject.getPath().split("/");  //getPath strips off the querystring '?f=b64'
    int offset = 0;
    if (path[0].equals("")){
      offset = 1;  //leading slash in uri splits to an empty path section
    }
    if (path.length - offset < 3){
      throw new RouterException(HttpResponseStatus.BAD_REQUEST,
          new VeniceException("Request URI must have an action, storename, and key"), true);
    }
    String action = path[0+offset];
    String storename = path[1+offset];
    String key = path[2+offset];

    if (action.equals(ACTION_STORAGE)) {
      String resourceName = getResourceFromStoreName(storename);
      RouterKey routerKey;
      if (isFormatB64(uri)){
        routerKey = RouterKey.fromBase64(key);
      } else {
        routerKey = RouterKey.fromString(key);
      }
      String partition = Integer.toString(partitionFinder.findPartitionNumber(resourceName, routerKey));
      return new Path(resourceName, Collections.singleton(routerKey), partition);

    } else {
      throw new RouterException(HttpResponseStatus.BAD_REQUEST,
          new VeniceException("Requested Action: " + action + " is not a valid action"), true);
    }
  }

  @Override
  public Path substitutePartitionKey(Path path, RouterKey key) {
    String partition = Integer.toString(partitionFinder.findPartitionNumber(path.getResourceName(), key));
    return new Path(path.getResourceName(), Collections.singleton(key), partition);
  }

  @Override
  public Path substitutePartitionKey(Path path, Collection<RouterKey> keys) {
    // Assumes all keys on same partition
    String partition = Integer.toString(partitionFinder.findPartitionNumber(path.getResourceName(), keys.iterator().next()));
    return new Path(path.getResourceName(), keys, partition);
  }

  /***
   * Queries the helix metadata repository for the
   *
   * @param storeName
   * @return store-version, matches the helix resource
   */
  private String getResourceFromStoreName(String storeName){
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
