package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.RoleFinder;
import javax.annotation.Nonnull;
import org.jboss.netty.handler.codec.http.HttpHeaders;


/**
 * Created by mwise on 3/9/16.
 */
public class VeniceRoleFinder implements RoleFinder<VeniceRole> {
  @Nonnull
  @Override
  public VeniceRole parseRole(String method, HttpHeaders httpHeaders) {
    return VeniceRole.REPLICA;
  }
}
