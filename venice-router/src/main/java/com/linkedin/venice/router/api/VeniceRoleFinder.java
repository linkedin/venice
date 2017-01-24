package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.base.misc.Headers;
import com.linkedin.ddsstorage.router.api.RoleFinder;
import javax.annotation.Nonnull;

public class VeniceRoleFinder implements RoleFinder<VeniceRole> {
  @Nonnull
  @Override
  public VeniceRole parseRole(String method, Headers httpHeaders) {
    return VeniceRole.REPLICA;
  }
}
