package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.ResourcePath;
import com.linkedin.venice.RequestConstants;
import java.util.ArrayList;
import java.util.Collection;


/**
 * Created by mwise on 3/3/16.
 */
public class Path implements ResourcePath<RouterKey> {

  private String resourceName;
  private Collection<RouterKey> keys;

  public Path(String resourceName, Collection<RouterKey> keys){
    this.resourceName = resourceName;
    this.keys = keys;
  }

  @Override
  public String getLocation() {
    String sep = VenicePathParser.SEP;
    return VenicePathParser.ACTION_READ + sep +
        resourceName + sep +
        getPartitionKey().base64Encoded() + "?" + RequestConstants.FORMAT_KEY +"="+ RequestConstants.B64_FORMAT;
  }

  @Override
  public Collection<RouterKey> getPartitionKeys() {
    return keys;
  }

  @Override
  public String getResourceName() {
    return resourceName;
  }

  @Override
  public Path clone() {
    return new Path(new String(resourceName), new ArrayList<>(keys));
  }
}
