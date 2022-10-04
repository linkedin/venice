package com.linkedin.alpini.router.api;

import com.linkedin.alpini.base.misc.Headers;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface RoleFinder<Role> {
  @Nonnull
  Role parseRole(@Nonnull String method, @Nonnull Headers httpHeaders);
}
