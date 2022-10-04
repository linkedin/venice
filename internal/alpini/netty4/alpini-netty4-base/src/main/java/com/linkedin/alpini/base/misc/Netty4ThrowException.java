package com.linkedin.alpini.base.misc;

import io.netty.util.internal.PlatformDependent;


/**
 * Simple wrapper using Netty4 {@linkplain PlatformDependent} implementation
 */
public final class Netty4ThrowException implements ExceptionUtil.ExceptionThrower {
  @Override
  public void throwException(Throwable throwable) {
    PlatformDependent.throwException(throwable);
  }
}
