package com.linkedin.venice.listener.request;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import java.net.URI;


public abstract class MultiKeyRouterRequestWrapper<K> extends RouterRequest {
  private final Iterable<K> keys;
  protected int keyCount = 0;

  protected MultiKeyRouterRequestWrapper(String resourceName, Iterable<K> keys, HttpRequest request) {
    super(resourceName, request);

    this.keys = keys;
    // TODO: looping through all keys at the beginning would prevent us from using lazy deserializer; refactor this
    this.keys.forEach( key -> ++keyCount);
  }

  public Iterable<K> getKeys() {
    return this.keys;
  }

  @Override
  public int getKeyCount() {
    return this.keyCount;
  }

}
