package com.linkedin.venice.router;

import com.linkedin.ddsstorage.router.api.ResourcePath;
import com.linkedin.ddsstorage.router.api.ResourcePathParser;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


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

  public static final String STORE_VERSION_SEP = "_v";
  public static final Pattern STORE_PATTERN = Pattern.compile("\\A[a-zA-Z][a-zA-Z0-9_-]*\\z");
  public static final int STORE_MAX_LENGTH = 128;
  public static final String SEP = "/";
  public static final String ACTION_READ = "read";

  public static final String B64FORMAT = "f=b64";

  private VersionFinder versionFinder;

  public VenicePathParser(VersionFinder versionFinder){
    this.versionFinder = versionFinder;
  };

  @Override
  public Path parseResourceUri(String uri) {//throws RouterException {
    URI uriObject;
    try {
      uriObject = new URI(uri);
    } catch (URISyntaxException e) {
      e.printStackTrace();
      throw new VeniceRouterException("Failed to parse uri: " + uri, e);
    }

    String[] path = uriObject.getPath().split("/");
    int offset = 0;
    if (path[0].equals("")){
      offset = 1;  //leading slash in uri splits to an empty path section
    }
    if (path.length - offset < 3){
      throw new VeniceRouterException("Request URI must have an action, storename, and key");
    }
    String action = path[0+offset];
    String storename = path[1+offset];
    String key = path[2+offset];

    if (action.equals(ACTION_READ)) {
      String resourceName = getResourceFromStoreName(storename);
      RouterKey routerKey;
      if (isFormatB64(uriObject.getQuery())){
        routerKey = RouterKey.fromBase64(key);
      } else {
        routerKey = RouterKey.fromString(key);
      }
      return new Path(resourceName, Collections.singleton(routerKey));

    } else {
      throw new VeniceRouterException("Requested Action: " + action + " is not a valid actions");
    }
  }

  @Override
  public Path substitutePartitionKey(Path path, RouterKey key) {
    return new Path(path.getResourceName(), Collections.singleton(key));
  }

  @Override
  public Path substitutePartitionKey(Path path, Collection<RouterKey> keys) {
    return new Path(path.getResourceName(), keys);
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

  private boolean isFormatB64(String query) {
    if (null != query && query.equals(B64FORMAT)) {
      return true;
    }
    return false;
  }



}
